#include "zkjobs.h"
#include "libzeke_indirect.h"

#include <apr_ring.h>

#ifndef ZKQUEUE_DEFAULT_TIMEOUT
#define ZKQUEUE_DEFAULT_TIMEOUT 30
#endif

#define ZKQUEUE_SESSION_PREFIX "zkqueue:"
#define ZKQUEUE_SESSION_KEY(ctx) ((ctx)->session_key ? (ctx)->session_key : \
  ((ctx)->session_key = apr_pstrcat((ctx)->pool,ZKQUEUE_SESSION_PREFIX,(ctx)->path,NULL)))
#define ZKQUEUE_NTIMERS 1

typedef long zkindex_t;

enum zkqueuestate {
  zkqueue_state_init = 0,
#define ZKQUEUE_STATE_INIT zkqueue_state_init
  zkqueue_state_cleared = (1 << 0),
#define ZKQUEUE_STATE_CLEARED zkqueue_state_cleared
  zkqueue_state_refresh_in_progress = (1 << 1),
#define ZKQUEUE_STATE_REFRESH_IN_PROGRESS zkqueue_state_refresh_in_progress
  zkqueue_state_loading = (1 << 2),
#define ZKQUEUE_STATE_LOADING zkqueue_state_loading
  zkqueue_state_defer_reload = (1 << 3),
#define ZKQUEUE_STATE_DEFER_RELOAD zkqueue_state_defer_reload
  zkqueue_state_running = (1 << 4),
#define ZKQUEUE_STATE_RUNNING zkqueue_state_running
};

typedef struct {
  apr_pool_t *pool;
  zkqueue_t *q;
  const char *session_key;
} zkqueue_context_t;

struct zkqueue {
  apr_pool_t *pool,*spool;
  struct zkws *ws;
  const char *path;
  apr_size_t njobs;
  zkqueue_context_t *ctx;
  const zeke_connection_t *conn;
  apr_uint32_t state;
  apr_interval_time_t *timeout;
  zkqueue_callback_fn update_fn;

  zeke_context_t *user_context;

  struct {
    apr_array_header_t *a;
    apr_hash_t *q;
  } index;

  APR_RING_HEAD(,zkjob_priv) pending;
};

struct loadarg {
  zkqueue_t *q;
  zkjob_t *j;
  zkqueue_context_t *ctx;
  void *misc;
};

#define ZKJOB_PENDING_IS_ORPHAN(j) (APR_RING_NEXT((j)->priv,link) == (j)->priv && \
                                    APR_RING_PREV((j)->priv,link) == (j)->priv)

#define ZKJOBF_LOADED (1 << 0)
#define ZKJOBF_DELETED (1 << 1)
#define ZKJOBF_NEW (1 << 3)
#define ZKJOBF_ERROR (1 << 4)
#define ZKJOBF_PENDING (1 << 5)

struct zkjob_priv {
  zkqueue_t *q;
  zkindex_t index;
  zkjob_t *j;
  apr_uint32_t flags;
  apr_int16_t nrefs;
  zk_status_t st;
  const char *node;

  APR_RING_ENTRY(zkjob_priv) link;
};

struct zkjob_ref {
  zeke_magic_t magic;
#define ZKJOB_REF_MAGIC 0x009abe15
  apr_pool_t *pool;
  apr_int16_t *nrefs;
  zeke_indirect_t *ind;
};

static inline apr_int16_t zkjob_increment_refcnt(apr_int16_t *r)
{
  assert(*r > 0);
  return ++(*r);
}

static inline apr_int16_t zkjob_decrement_refcnt(apr_int16_t *r)
{
  assert(*r >= 1);
  return --(*r);
}

#ifdef DEBUGGING
static inline apr_int16_t ZKINCREF(zkjob_t *j)
{
  apr_int16_t r = zkjob_increment_refcnt(&j->priv->nrefs);
  LOG("++ JOB INCREF #%" APR_UINT64_T_FMT " refcnt=%d\n",
               j->seq,(int)r);
  return r;
}
static inline apr_int16_t ZKDECREF(zkjob_t *j)
{
  apr_int16_t r = zkjob_decrement_refcnt(&j->priv->nrefs);
  LOG("-- JOB DECREF #%" APR_UINT64_T_FMT " refcnt=%d\n",
               j->seq,(int)r);
  return r;
}
#else /* !DEBUGGING */
#define ZKINCREF(j) zkjob_increment_refcnt(&(j)->priv->nrefs)
#define ZKDECREF(j) zkjob_decrement_refcnt(&(j)->priv->nrefs)
#endif

DECLARE_CONTEXT(zkqueue_context_t)
#define ZKQUEUE_CONTEXT(cbd) cast_context_to_zkqueue_context_t((zeke_context_t*)(cbd)->ctx)

typedef apr_status_t (*apr_cleanup_fn)(void*);

/* fowrard decls */
static zk_status_t zkqueue_start_reload(zkqueue_t *q);
static zeke_cb_t null_callback(const zeke_callback_data_t*);
static void queue_watcher(const zeke_watcher_cb_data_t*);

static zeke_regex_t *get_job_regex(zkqueue_t *q)
{
  zeke_regex_t *re = NULL;
  AN(q);
  AN(q->pool);
  apr_pool_userdata_get((void**)&re,"zkqueue:job:regex",q->pool);
  if(re == NULL) {
    AZOK(zeke_regex_create_ex(&re,"[-_](\\d{9,})$",
          ZEKE_RE_DOLLAR_ENDONLY|ZEKE_RE_STUDY,q->pool));
    apr_pool_userdata_setn(re,"zkqueue:job:regex",
                           (apr_cleanup_fn)zeke_regex_destroy,
                           q->pool);
  }
  return re;
}

static inline void zkjob_assert(const zkjob_t *j)
{
  ZK_CHECK_OBJ_NOTNULL(j, ZKJOB_MAGIC);
  ZK_CHECK_OBJ_NOTNULL(j->ws, ZK_WS_MAGIC);
}

static zeke_cb_t null_callback(const zeke_callback_data_t *cbd)
{
  DESTROY;
}

static void zkjob_free(zkjob_t *j)
{
  zkjob_assert(j);
  ZK_CHECK_OBJ_NOTNULL(j->ws, ZK_WS_MAGIC);
  assert(j->priv->nrefs == 0);
  apr_pool_destroy(j->pool);
}

static apr_int64_t zkjob_seq(zkqueue_t *q, const char *node, apr_pool_t *p)
{
  apr_int64_t seq;
  apr_size_t ngroups = 0;
  const char *n;
  char *end = NULL;
  zeke_regex_match_t *m;
  zeke_regex_t *re;

  AN(q); AN(p);
  re = get_job_regex(q);
  AN(re);
  m = zeke_regex_exec(re,node,&ngroups);
  if(!m || ngroups < 1)
    return APR_INT64_C(-1);
  n = zeke_regex_match_group(m,1,p);
  AN(n);
  seq = apr_strtoi64(n,&end,10);
  if(!end || *end)
    return APR_INT64_C(-1);
  return seq;
}

static apr_status_t cleanup_job_queue(void *jv)
{
  zkjob_t *j = jv;
  zkqueue_t *q = NULL;

  if(j && j->priv)
    q = j->priv->q;
  if(!q || !q->index.q) return APR_SUCCESS;

  zkjob_assert(j);
  AN(j->priv->node);
  apr_hash_set(q->index.q,j->priv->node,APR_HASH_KEY_STRING,NULL);
  j->priv->node = NULL;
  j->priv->q = NULL;
  return APR_SUCCESS;
}

static zkjob_t *zkjob_new(zkqueue_t *q, const char *node, const char *ns)
{
  struct zkws *ws;
  zkjob_t *j;
  apr_int64_t seq;

  AN(q);
  AN(q->pool);
  ws = zk_workspace_new(q->pool);
  ZK_CHECK_OBJ_NOTNULL(ws, ZK_WS_MAGIC);
  j = zk_workspace_alloc(ws, sizeof(*j));
  AN(j);
  j->magic = ZKJOB_MAGIC;
  j->pool = ws->p;
  j->ws = ws;
  j->content = NULL;
  j->vxid = 0; /* invalid until a proc is created */
  j->len = 0;
  j->headers = apr_table_make(j->pool,2);
  j->priv = zk_workspace_alloc(ws,sizeof(*j->priv));
  j->priv->q = q;
  j->priv->j = j;
  j->priv->flags = 0;
  j->priv->st = ZEOK;
  j->priv->index = -1;
  j->priv->nrefs = 1;
  APR_RING_ELEM_INIT(j->priv,link);

  if(ns != NULL)
    j->priv->node = zk_workspace_nodecat(ws,q->path,ns,node,NULL);
  else {
    j->priv->node = zk_workspace_nodecat(ws,q->path,node,NULL);
  }
  AN(j->priv->node);
  seq = zkjob_seq(q,j->priv->node,j->pool);
  if(seq == APR_INT64_C(-1)) {
    j->seq = APR_UINT64_C(0);
    ERR("invalid sequence number for job at %s",j->priv->node);
  } else
    j->seq = (apr_uint64_t)seq;
  zeke_pool_cleanup_register(ws->p,j,cleanup_job_queue);
  zeke_pool_tag_ex(ws->p,"zkjobd workspace pool for job #%" APR_UINT64_T_FMT,
                   j->seq);
  return j;
}

static void pack_job_index(zkqueue_t *q)
{
  const char **cp;
  int i;
  apr_size_t u,cnt;

  ZK_CHECK_OBJ_NOTNULL(q->ws,ZK_WS_MAGIC);
  u = zk_workspace_reserve(q->ws,q->njobs * sizeof(const char*));
  assert(u >= q->njobs * sizeof(const char*));
  cp = (const char**)q->ws->f;
  cnt = 0;
  for(i = 0; i < q->index.a->nelts; i++) {
    const char *node = APR_ARRAY_IDX(q->index.a,i,const char*);
    if(node != NULL) {
      zkjob_t *j = apr_hash_get(q->index.q,node,APR_HASH_KEY_STRING);
      zkjob_assert(j);
      *(cp+cnt) = node;
      j->priv->index = (zkindex_t)cnt++;
    }
  }

  assert(cnt == q->njobs);
  apr_array_clear(q->index.a);
  for(i = 0, cp = (const char**)q->ws->f; i < cnt; i++)
    APR_ARRAY_PUSH(q->index.a,const char*) = *(cp+i);
  assert(q->njobs == q->index.a->nelts);
  zk_workspace_release(q->ws,0);
}

static zeke_cb_t reload_complete(const zeke_callback_data_t *cbd)
{
  apr_hash_t *newjobs;
  apr_array_header_t *jobs;
  zkqueue_context_t *ctx = ZKQUEUE_CONTEXT(cbd);
  zkqueue_t *q;
  zk_status_t st = zeke_callback_status_get(cbd);
  apr_size_t njobs,ndel,nadd;
  int had = 0,i;
  apr_hash_index_t *hi;

  if(!ctx || !ctx->q || !ctx->q->index.q) DESTROY;
  q = ctx->q;
  ZK_CHECK_OBJ_NOTNULL(q->ws,ZK_WS_MAGIC);
  LOG("QUEUE: reload complete, njobs=%d",cbd->vectors.strings->count);
  if(!(q->state & ZKQUEUE_STATE_LOADING) || (q->state & ZKQUEUE_STATE_CLEARED)) {
    WARN("WARNING: unexpected reload completion, ignoring");
    DESTROY;
  }
  q->state &= ~ZKQUEUE_STATE_LOADING;
  q->state |= ZKQUEUE_STATE_RUNNING;

  if(!ZEKE_STATUS_IS_OKAY(st)) {
    zeke_apr_eprintf(st,"queue at %s",q->path);
    zeke_session_context_release(q->conn,ctx->session_key);
    return ZEKE_CALLBACK_DESTROY;
  }
  njobs = cbd->vectors.strings->count;
  newjobs = apr_hash_make(cbd->pool);
  if(q->index.a->nelts > 0)
    had++;

  do {
    jobs = apr_array_make(cbd->pool, njobs, sizeof(const char*));
    AN(jobs);
    for(i = 0; i < njobs; i++) {
      const char *node = cbd->vectors.strings->data[i];
      AN(node);
      if(zkjob_seq(q,node,cbd->pool) > APR_INT64_C(-1)) {
        APR_ARRAY_PUSH(jobs,const char*) = node;
      }
    }

    njobs = jobs->nelts;
  } while(0);

  if(njobs > 1)
    AAOK(zeke_array_sort_seq(jobs,cbd->pool));

  for(i = 0; i < njobs; i++) {
    const char *node = APR_ARRAY_IDX(jobs,i,const char*);
    AN(node);
    apr_hash_set(newjobs,node,APR_HASH_KEY_STRING,node);
  }

  ndel = nadd = 0;
  /* delete everything from the old set if not in the new */
  for(i = 0; i < q->index.a->nelts; i++) {
    const char *node = APR_ARRAY_IDX(q->index.a,i,const char*);
    const char *new;
    if(node == NULL) {
      ndel++;
      continue;
    }
    new = apr_hash_get(newjobs,node,APR_HASH_KEY_STRING);
    if(!new) {
      zkjob_t *job;
      job = apr_hash_get(q->index.q,node,APR_HASH_KEY_STRING);
      if(job) {
        zkjob_assert(job);
        AN(job->priv);
        if(job->priv->flags & ZKJOBF_DELETED) {
          LOG("dumping deleted job %s@%d",job->priv->node,i);
          apr_hash_set(q->index.q,node,APR_HASH_KEY_STRING,NULL);
          ndel++;
          APR_ARRAY_IDX(q->index.a,i,const char*) = NULL;
          if(!ZKDECREF(job))
            zkjob_free(job);
        }
      }
    }
  }

  /* add new entries not in our current index */
  for(hi = apr_hash_first(cbd->pool,newjobs); hi; hi = apr_hash_next(hi)) {
    const char *node;
    zkjob_t *old;
    apr_hash_this(hi,NULL,NULL,(void**)&node);
    AN(node);
    old = apr_hash_get(q->index.q,node,APR_HASH_KEY_STRING);
    if(!old) {
      zkjob_t *j = zkjob_new(q,node,NULL);
      zkjob_assert(j);
      node = apr_pstrdup(q->spool,node);
      APR_ARRAY_PUSH(q->index.a,const char*) = node;
      j->priv->index = q->index.a->nelts-1;
      j->priv->flags |= ZKJOBF_NEW;
      apr_hash_set(q->index.q,node,APR_HASH_KEY_STRING,j);
      LOG("JOB: seq=%" APR_UINT64_T_FMT " f=%u i=%ld njobs=%lu %s\n",j->seq,j->priv->flags,
          j->priv->index,(unsigned long)njobs,j->priv->node);
      nadd++;
    } else {
      zkjob_assert(old);
    }
  }

  q->njobs = njobs;
  if(ndel > 10)
    pack_job_index(q);

  if (q->state & ZKQUEUE_STATE_DEFER_RELOAD) {
    q->state &= ~ZKQUEUE_STATE_DEFER_RELOAD;
    st = zkqueue_start_reload(q);
    if(ZEKE_STATUS_IS_OKAY(st))
      RETURN;
    zeke_apr_error(q->path,st);
    DESTROY;
  }

  if(nadd > 0 && q->update_fn) {
    apr_hash_index_t *hi;
    zeke_cb_t rv;
    zeke_callback_status_set(cbd,ZEOK);
    if(q->njobs > 0 && !had) {
      zeke_callback_status_set(cbd,ZEOK);
      rv = q->update_fn(q,zkq_ev_indexed,NULL,q->user_context);

      if(rv == ZEKE_CALLBACK_DESTROY)
        return rv;
    }

    for(hi = apr_hash_first(cbd->pool,q->index.q); hi; hi = apr_hash_next(hi)) {
      zkjob_t *j;
      apr_ssize_t sz;
      const char *node;
      apr_hash_this(hi,(const void**)&node,&sz,(void**)&j);
      zkjob_assert(j);
      AN(node);
      if(j->priv->flags & ZKJOBF_NEW) {
        rv = q->update_fn(q,zkq_ev_job_new,j,q->user_context);
        j->priv->flags &= ~ZKJOBF_NEW;
        if(rv == ZEKE_CALLBACK_DESTROY)
          return rv;
      }
    }
  } else {
    for(hi = apr_hash_first(cbd->pool,q->index.q); hi; hi = apr_hash_next(hi)) {
      zkjob_t *j;
      apr_ssize_t sz;
      const char *node;
      apr_hash_this(hi,(const void**)&node,&sz,(void**)&j);
      AN(node);
      if(j->priv->flags & ZKJOBF_NEW)
        j->priv->flags &= ~ZKJOBF_NEW;
    }
  }
  DESTROY;
}

static zk_status_t zkqueue_start_reload(zkqueue_t *q)
{
  zk_status_t st;
  zeke_callback_data_t *cbd;
  AN(q);
  AN(q->ctx);
  ZK_CHECK_OBJ_NOTNULL(q->ws, ZK_WS_MAGIC);

  if(q->state & ZKQUEUE_STATE_LOADING)
    return APR_EBUSY;

  q->state |= ZKQUEUE_STATE_LOADING;
  cbd = zeke_callback_data_create_ex(q->conn,queue_watcher,NULL,1);
  AN(cbd);
  zeke_callback_separate_watcher(cbd,q->ctx);
  st = zeke_aget_children(&cbd,q->conn,q->path,queue_watcher,reload_complete,q->ctx);
  if(!ZEKE_STATUS_IS_OKAY(st))
    q->state &= ~ZKQUEUE_STATE_LOADING;
  return st;
}

static void queue_watcher(const zeke_watcher_cb_data_t *cbwd)
{
  zkqueue_context_t *ctx = (zkqueue_context_t*)cbwd->ctx;
  zkqueue_t *q;
  zk_status_t st = ZEOK;

  AN(ctx);
  q = ctx->q;
  if(q == NULL)
    return;

  ZK_CHECK_OBJ_NOTNULL(q->ws,ZK_WS_MAGIC);
  if((q->state & ZKQUEUE_STATE_LOADING) != 0)
    q->state |= ZKQUEUE_STATE_DEFER_RELOAD;
  else {
    st = zkqueue_start_reload(q);
    if(!ZEKE_STATUS_IS_OKAY(st)) {
      zeke_apr_eprintf(st,"trying to reload %s",q->path);
      zeke_safe_shutdown(ZKE_TO_ERROR(st));
    }
  }
}

static void zkqueue_session_cleanup(zeke_context_t *ctx, void *sp)
{
  zkqueue_t *q;

  zkqueue_context_t *session = (zkqueue_context_t*)sp;
  q = session->q;
  if(session->pool)
    apr_pool_destroy(session->pool);
  if(q)
    q->state |= ZKQUEUE_STATE_CLEARED;
}

static apr_status_t zkqueue_index_cleanup(void *p)
{
  zkqueue_t *q = (zkqueue_t*)p;
  AN(q);
  ZK_CHECK_OBJ_NOTNULL(q->ws, ZK_WS_MAGIC);
  q->index.q = NULL;
  q->index.a = NULL;
  return APR_SUCCESS;
}

ZEKE_HIDDEN
zk_status_t zkqueue_clear(zkqueue_t *q)
{
  zkjob_t *j;
  struct zkjob_priv *jp,*state;

  AN(q);
  ZK_CHECK_OBJ_NOTNULL(q->ws, ZK_WS_MAGIC);
  if(q->state == ZKQUEUE_STATE_INIT || (q->state & ZKQUEUE_STATE_CLEARED))
    return APR_EINVAL;

  if(q->state & ZKQUEUE_STATE_LOADING)
    return APR_EBUSY;

  APR_RING_FOREACH_SAFE(jp,state,&q->pending,zkjob_priv,link) {
    j = jp->j;
    assert(j->priv == jp);
    assert(jp->nrefs > 0);
    APR_RING_REMOVE(jp,link);
    APR_RING_ELEM_INIT(jp,link);
    if(!ZKDECREF(j))
      zkjob_free(j);
  }

  assert(APR_RING_EMPTY(&q->pending,zkjob_priv,link));
  AN(q->spool);
  AN(q->ctx); AN(q->ctx->session_key);
  q->state = ZKQUEUE_STATE_CLEARED;
  zeke_session_context_release(q->conn,q->ctx->session_key);
  return APR_SUCCESS;
}

ZEKE_HIDDEN
zk_status_t zkqueue_start(zkqueue_t *q, zkqueue_callback_fn update_fn,
                                       apr_interval_time_t *timeout)
{
  AN(q);
  ZK_CHECK_OBJ_NOTNULL(q->ws, ZK_WS_MAGIC);

  if(q->state != ZKQUEUE_STATE_INIT && !(q->state & ZKQUEUE_STATE_CLEARED))
    return APR_EBUSY;
  if(q->spool == NULL) {
    const char *session_key = apr_pstrcat(q->pool,ZKQUEUE_SESSION_PREFIX,q->path,NULL);
    zkqueue_context_t *session = zeke_session_context_create(q->conn,session_key,
                                                             sizeof(zkqueue_context_t));
    AN(session);
    ZEKE_ASSERTV(session->pool == NULL, "session already exists for %s",session_key);
    AAOK(apr_pool_create(&session->pool,q->pool));
    zeke_session_cleanup_register(q->conn,session_key,session,zkqueue_session_cleanup);
    zeke_pool_cleanup_indirect(session->pool,session->pool);
    zeke_pool_tag(session->pool,"zkjobd zkqueue session pool");
    q->spool = session->pool;
    zeke_pool_cleanup_indirect(session->pool,q->spool);
    session->session_key = apr_pstrdup(session->pool,session_key);
    zeke_pool_cleanup_indirect(session->pool,session->session_key);
    session->q = q;
    zeke_pool_cleanup_indirect(session->pool,session->q);
    q->ctx = session;
  }
  AN(q->spool);
  q->njobs = 0;
  AZ(q->index.a);
  q->index.a = apr_array_make(q->spool,10,sizeof(const char *));
  AN(q->index.a);
  AZ(q->index.q);
  q->index.q = apr_hash_make(q->spool);
  AN(q->index.q);
  q->timeout = timeout;
  apr_pool_pre_cleanup_register(q->spool,q,zkqueue_index_cleanup);
  AN(q->ctx);
  if(update_fn)
    q->update_fn = update_fn;

  q->state &= ~ZKQUEUE_STATE_CLEARED;
  return zkqueue_start_reload(q);
}

ZEKE_HIDDEN
zk_status_t zkqueue_create(zkqueue_t **qp, const char *path,
                           zkqueue_callback_fn update_fn,
                           const zeke_connection_t *conn,
                           apr_pool_t *p)
{
  zkqueue_t *q;

  AN(qp); AN(path); AN(conn); AN(p);
  q = apr_pcalloc(p,sizeof(*q));
  q->pool = p;
  q->ws = zk_workspace_new_ex(p,"zkjobd zkqueue");
  ZK_CHECK_OBJ_NOTNULL(q->ws, ZK_WS_MAGIC);
  q->path = apr_pstrdup(p,path);
  q->conn = conn;
  q->state = ZKQUEUE_STATE_INIT;
  q->update_fn = update_fn;
  APR_RING_INIT(&q->pending,zkjob_priv,link);
  *qp = q;
  return ZEOK;
}

ZEKE_HIDDEN
void zkqueue_context_set(zkqueue_t *q, zeke_context_t *ctx)
{
  AN(q);
  ZK_CHECK_OBJ_NOTNULL(q->ws, ZK_WS_MAGIC);
  q->user_context = ctx;
}

ZEKE_HIDDEN
zeke_context_t *zkqueue_context_get(zkqueue_t *q)
{
  AN(q);
  ZK_CHECK_OBJ_NOTNULL(q->ws, ZK_WS_MAGIC);
  return q->user_context;
}

static
apr_status_t cleanup_job_ref(void *rp)
{
  zkjob_ref_t *ref = (zkjob_ref_t*)rp;
  zeke_indirect_t *ind;
  zkjob_t *j;

  ZK_CHECK_OBJ_NOTNULL(ref, ZKJOB_REF_MAGIC);
  ind = ref->ind;
  ref->ind = NULL;
  if(ind) {
    j = zeke_indirect_consume(ind);
    if(j && !ZKDECREF(j))
      zkjob_free(j);
  }

  return APR_SUCCESS;
}

ZEKE_HIDDEN
int zkqueue_job_ref_eq(const zkjob_ref_t *ref1,
                       const zkjob_ref_t *ref2)
{
  if(!ref1 || !ref2)
    return 0;

  ZK_CHECK_OBJ_NOTNULL(ref1, ZKJOB_REF_MAGIC);
  ZK_CHECK_OBJ_NOTNULL(ref2, ZKJOB_REF_MAGIC);

  if(ref1->ind && ref2->ind) {
    void *v1 = zeke_indirect_peek(ref1->ind);
    void *v2 = zeke_indirect_peek(ref2->ind);
    if(v1 && v2)
      return (v1 == v2);
  }
  return 0;
}

ZEKE_HIDDEN
zkjob_t *zkqueue_job_ref_get(const zkjob_ref_t *ref)
{
  ZK_CHECK_OBJ_NOTNULL(ref, ZKJOB_REF_MAGIC);
  assert(ref->ind != NULL);
  assert(*(ref->nrefs) > 0);
  return zeke_indirect_peek(ref->ind);
}

ZEKE_HIDDEN
zkjob_ref_t *zkqueue_job_ref_make(zkjob_t *j, apr_pool_t *p)
{
  zkjob_ref_t *ref;
  apr_pool_t *pool;

  AN(p);
  ZK_CHECK_OBJ_NOTNULL(j, ZKJOB_MAGIC);
  assert(p != j->pool);

  AAOK(apr_pool_create(&pool,p));
  zeke_pool_tag_ex(pool,"reference to job #%" APR_UINT64_T_FMT,j->seq);
  ref = apr_palloc(pool,sizeof(*ref));
  ref->magic = ZKJOB_REF_MAGIC;
  ref->nrefs = &j->priv->nrefs;
  ref->pool = pool;
  ZKINCREF(j);
  ref->ind = zeke_indirect_make(j,j->pool);
  AN(ref->ind);
  apr_pool_pre_cleanup_register(pool,ref,cleanup_job_ref);
  return ref;
}

ZEKE_HIDDEN
void zkqueue_job_ref_destroy(zkjob_ref_t *ref)
{
  if(ref) {
    ZK_CHECK_OBJ_NOTNULL(ref, ZKJOB_REF_MAGIC);
    if(ref->ind) {
      zkjob_t *j;
      apr_pool_t *p = ref->pool;
      apr_uint16_t r = 99;
      assert(p != NULL);
      j = zeke_indirect_consume(ref->ind);
      ref->ind = NULL;
      ref->pool = NULL;
      if(j && j->priv && j->priv->nrefs > 0)
        r = ZKDECREF(j);
      apr_pool_cleanup_kill(p,ref,cleanup_job_ref);
      apr_pool_destroy(p);
      if(r == 0)
        zkjob_free(j);
    }
  }
}

ZEKE_HIDDEN
zk_status_t zkqueue_job_delete(zkjob_t *j)
{
  zkqueue_t *q;

  zkjob_assert(j);
  AN(j->priv); AN(j->priv->node);
  q = j->priv->q;
  AN(q);
  assert(ZKJOB_PENDING_IS_ORPHAN(j));
  if(j->priv->flags & ZKJOBF_DELETED) {
    return APR_EBUSY;
    assert("attempt to delete job more than once?" == NULL);
    /* return APR_EBUSY; */
  }
  j->priv->flags |= ZKJOBF_DELETED;
  /* The watcher will pick this up and remove it from the index */
  return zeke_adelete(NULL,q->conn,j->priv->node,-1,null_callback,NULL);
}

ZEKE_HIDDEN
zk_status_t zkqueue_job_pending_pop(zkqueue_t *q, zkjob_t **jpp)
{
  zkjob_t *j;
  struct zkjob_priv *jp;

  AN(q);
  if(!q->spool || !q->index.q || !q->index.a)
    return APR_EINVAL;
  ZK_CHECK_OBJ_NOTNULL(q->ws, ZK_WS_MAGIC);
  AN(q->index.q); AN(q->index.a);

  if(APR_RING_EMPTY(&q->pending,zkjob_priv,link))
    return APR_EINCOMPLETE;

  jp = APR_RING_FIRST(&q->pending);
  assert(jp && jp->j);
  j = jp->j;
  assert(j->priv == jp);
  zkjob_assert(j);
  /* note, we normally DON'T decref here as the reference is simply passed to
   * the caller.
   */
  APR_RING_REMOVE(j->priv,link);
  APR_RING_ELEM_INIT(j->priv,link);
  LOG("-PENDING: job #%" APR_UINT64_T_FMT " popped, orphan=%d",
    j->seq,ZKJOB_PENDING_IS_ORPHAN(j));
  if(jpp)
    *jpp = j;
  else if(ZKDECREF(j) == 0)
    zkjob_free(j);
  return ZEOK;
}

ZEKE_HIDDEN
zk_status_t zkqueue_job_pending_push(zkqueue_t *q, zkjob_t *j)
{
  AN(q);
  if(!q->spool || !q->index.q || !q->index.a)
    return APR_EINVAL;
  ZK_CHECK_OBJ_NOTNULL(q->ws, ZK_WS_MAGIC);
  AN(q->index.q); AN(q->index.a);

  zkjob_assert(j);
  assert(ZKJOB_PENDING_IS_ORPHAN(j));

  /* increase the ref count so it stays around */
  ZKINCREF(j);
  j->priv->flags &= ~ZKJOBF_NEW;
  j->priv->flags |= ZKJOBF_PENDING;
  APR_RING_INSERT_TAIL(&q->pending,j->priv,zkjob_priv,link);
  LOG("+PENDING: job #%" APR_UINT64_T_FMT " pushed, orphan=%d",
    j->seq,ZKJOB_PENDING_IS_ORPHAN(j));
  return ZEOK;
}

ZEKE_HIDDEN
zk_status_t zkqueue_job_lock(zkjob_t *j)
{
  zkjob_assert(j);
  AN(j->priv); AN(j->priv->node);
  ZKINCREF(j);
  LOG("LOCKED (%d)",j->priv->nrefs);
  return APR_SUCCESS;
}

ZEKE_HIDDEN
zk_status_t zkqueue_job_unlock(zkjob_t *j)
{
  zkjob_assert(j);
  AN(j->priv); AN(j->priv->node);
  LOG("UNLOCKED (%d)",j->priv->nrefs-1);
  if(ZKDECREF(j) == 0)
    zkjob_free(j);
  return APR_SUCCESS;
}

ZEKE_HIDDEN
int zkqueue_is_job_locked(zkjob_t *j)
{
  zkjob_assert(j);
  AN(j->priv); AN(j->priv->node);
  return ((j->priv->nrefs > 1) ? 1 : 0);
}

ZEKE_HIDDEN
apr_pool_t *zkqueue_pool_get(const zkqueue_t *q)
{
  AN(q);
  ZK_CHECK_OBJ_NOTNULL(q->ws,ZK_WS_MAGIC);
  return q->pool;
}

static
void job_make_headers(zkjob_t *j)
{
  apr_size_t u,b;
  const char *l,*cp;
  char *r;
  zkjob_assert(j);

  AN(j->content);
  for(; apr_isspace(*j->content); j->content++, j->len--)
    ;
  if(!*j->content) return;
  u = zk_workspace_reserve(j->ws,0);
  b = 0;
  AN(u);
  for(l = NULL, cp = (const char*)j->content; cp && *cp; cp++) {
    char *key,*val,*tok,*last = NULL;
    for(; *cp && apr_isspace(*cp) && *cp != APR_ASCII_LF; cp++)
      ;
    l = cp;
    if(!*l || *l == APR_ASCII_LF)
      break;
    cp = strchr(l,APR_ASCII_LF);
    if(!cp)
      break;
    if(cp - l >= u-1) {
      zk_workspace_release(j->ws,b);
      u = zk_workspace_reserve(j->ws,((cp-l)+1)*2);
      assert(u > (cp-l)+1);
    }
    tok = j->ws->r;
    r = apr_cpystrn(tok,l,(cp-l)+1);
    l = cp + 1;
    AN(r); r++;
    j->ws->r = r;
    u -= (r - tok);
    b += (r - tok);
    r = tok;
    for(key = val = NULL, tok = apr_strtok(r,":",&last);
                                          tok != NULL;
                      tok = apr_strtok(NULL,":",&last)) {
      while(*tok && apr_isspace(*tok)) tok++;
      if(!*tok)
        break;
      if(key == NULL) {
        key = tok;
        apr_collapse_spaces(key,tok);
      } else {
        AZ(val);
        for(val = tok; apr_isspace(*val); val++)
          ;
        break;
      }
    }
    if(key && val && *key && *val) {
      for(r = key; *r; r++)
          *r = apr_tolower(*r);
      apr_table_mergen(j->headers,key,val);
    } else if(key)
      ERR("invalid or malformed header '%s' in job %s", key, j->priv->node);
  }
  if(l) {
    while(*l && (apr_isspace(*l)))
      l++;
    if(*l == '\n') l++;
    j->len -= (l - (const char*)j->content);
    j->content = (const unsigned char*)l;
  }

  zk_workspace_release(j->ws,b);
}

static
zeke_cb_t job_load_complete(const zeke_callback_data_t *cbd)
{
  zk_status_t st;
  struct loadarg *a = (struct loadarg*)cbd->ctx;
  zkjob_t *j = NULL;
  zkqueue_t *q = NULL;

  if(a) {
    q = a->q;
    j = a->j;
    if(q)
      apr_pool_cleanup_kill(q->pool,&a->q,zeke_indirect_wipe);
    if(j)
      apr_pool_cleanup_kill(j->pool,&a->j,zeke_indirect_wipe);
    if(a->ctx)
      apr_pool_cleanup_kill(a->ctx->pool,&a->ctx,zeke_indirect_wipe);
  }
  if(!a || !j || !q)
    goto job_load_complete_fini;

  zkjob_assert(j);
  AN(j->priv); AN(j->priv->node);
  assert(j->priv->q == q);
  if((q->state & ZKQUEUE_STATE_CLEARED) || !(q->state & ZKQUEUE_STATE_RUNNING)) {
    ZK_CHECK_OBJ_NOTNULL(q->ws, ZK_WS_MAGIC);
    LOG("queue cleared, tossing job #%" APR_UINT64_T_FMT,j->seq);
    assert(j->priv->nrefs >= 0);
    if(ZKDECREF(j) == 0)
      zkjob_free(j);
    goto job_load_complete_fini;
  }
  st = zeke_callback_status_get(cbd);
  if(!ZEKE_STATUS_IS_OKAY(st)) {
    j->priv->flags |= ZKJOBF_ERROR;
    j->priv->st = st;
    zkqueue_job_unlock(j);
    if(q->update_fn)
      q->update_fn(q,zkq_ev_job_error,j,q->user_context);
    if(ZEKE_STATUS_IS_OKAY(j->priv->st))
      zeke_aget(NULL,q->conn,j->priv->node,NULL,job_load_complete,a);
    goto job_load_complete_fini;
  }
  LOG("job #%" APR_UINT64_T_FMT " loaded, value_len=%d",j->seq,(int)cbd->value_len);
  if(cbd->value == NULL || cbd->value_len == -1) {
    j->content = NULL;
    j->len = 0;
    j->priv->flags |= ZKJOBF_LOADED;
  } else {
    apr_size_t u;
    ZK_CHECK_OBJ_NOTNULL(j->ws,ZK_WS_MAGIC);
    j->len = cbd->value_len;
    u = zk_workspace_reserve(j->ws,j->len+1);
    assert(u >= j->len+1);
    memcpy(j->ws->f,cbd->value,j->len);
    if(j->len > 0 && *(j->ws->f + j->len - 1) != '\0')
      *(j->ws->f + j->len++) = '\0';
    j->content = (const unsigned char*)zk_workspace_release(j->ws,j->len);
    job_make_headers(j);
    j->priv->flags |= ZKJOBF_LOADED;
    if(a) free(a);
    a = NULL;
  }

  if(q->update_fn) {
    if(q->update_fn(q,zkq_ev_job_loaded,j,q->user_context) == ZEKE_CALLBACK_DESTROY) {
      if(ZKJOB_PENDING_IS_ORPHAN(j)) {
        if((j->priv->flags & ZKJOBF_DELETED) == 0)
          zkqueue_job_delete(j);
      } else j->priv->flags &= ~ZKJOBF_LOADED;
      zkqueue_job_unlock(j);
    }
  }

job_load_complete_fini:
  if(a)
    free(a);
  DESTROY;
}

ZEKE_HIDDEN
int zkqueue_job_is_loaded(zkjob_t *j)
{
  zkjob_assert(j);
  AN(j->priv);

  return (j->priv->flags & ZKJOBF_LOADED) ? 1 : 0;
}

ZEKE_HIDDEN
zk_status_t zkqueue_job_load(zkjob_t *j)
{
  zk_status_t st;
  struct loadarg *a;
  zkqueue_t *q;
  zkjob_assert(j);
  AN(j->priv); AN(j->priv->node);
  q = j->priv->q;
  AN(q);

  if(!q->index.q || (j->priv->flags & ZKJOBF_LOADED))
    return APR_EINVAL;

  assert(ZKJOB_PENDING_IS_ORPHAN(j));
  zkqueue_job_lock(j);

  AZ(j->content);
  AZ(j->len);
  assert(apr_table_elts(j->headers)->nelts == 0);
  a = malloc(sizeof(*a));
  AN(a);
  a->q = q;
  zeke_pool_cleanup_indirect(q->pool,a->q);
  a->j = j;
  zeke_pool_cleanup_indirect(j->pool,a->j);
  a->ctx = (zkqueue_context_t*)q->ctx;
  zeke_pool_cleanup_indirect(a->ctx->pool,a->ctx);
  a->misc = NULL;
  st = zeke_aget(NULL,q->conn,j->priv->node,NULL,job_load_complete,a);
  if(!ZEKE_STATUS_IS_OKAY(st)) {
    free(a);
    j->priv->st = st;
    j->priv->flags |= ZKJOBF_ERROR;
    zkqueue_job_unlock(j);
  }
  return st;
}

ZEKE_HIDDEN
int zkqueue_job_is_pending(zkjob_t *j)
{
  zkjob_assert(j);
  return !ZKJOB_PENDING_IS_ORPHAN(j);
}

ZEKE_HIDDEN
zk_status_t zkqueue_job_status_get(zkjob_t *j, zk_status_t *stp)
{
  zkjob_assert(j);
  AN(j->priv); AN(j->priv->node);
  if(!stp)
    return APR_EINVAL;
  if(j->priv->flags & ZKJOBF_ERROR)
    *stp = j->priv->st;
  else
    *stp = ZEOK;
  return APR_SUCCESS;
}

ZEKE_HIDDEN
zk_status_t zkqueue_job_status_clear(zkjob_t *j)
{
  zkjob_assert(j);
  AN(j->priv); AN(j->priv->node);
  if((j->priv->flags & ZKJOBF_ERROR) == 0)
    return APR_EINVAL;
  j->priv->flags &= ~ZKJOBF_ERROR;
  return APR_SUCCESS;
}

ZEKE_HIDDEN
const char *zkqueue_job_node_get(const zkjob_t *j)
{
  zkjob_assert(j);
  AN(j->priv);
  return j->priv->node;
}
