#include "zkjobs.h"

#ifndef ZKELECTION_DEFAULT_TIMEOUT
#define ZKELECTION_DEFAULT_TIMEOUT 30
#endif /* ZKELECTION_DEFAULT_TIMEOUT */

#define ZKELECTION_SESSION_PREFIX "zkelection:"
#define ZKELECTION_SESSION_KEY(e) ((e)->ctx && (e)->ctx->session_key ? (e)->ctx->session_key : NULL)
#define ZKELECTION_NEW_SESSION_KEY(e,p) apr_pstrcat((p),ZKELECTION_SESSION_PREFIX,(e)->path,NULL)
#define ZKELECTION_NTIMERS 1
#define zkelection_reset_timers(elec,index,value) \
        zktool_reset_timers((elec)->timers,(elec)->waits,ZKELECTION_NTIMERS,(index),(value))
#define zkelection_update_timers(elec,shortest,updates,max_updates) \
        zktool_update_timers((elec)->timers,(elec)->waits,ZKELECTION_NTIMERS,(shortest),(updates),(max_updates))
#define zkelection_update_timeouts(elec,index,shortest) \
        zktool_update_timeouts((elec)->timers,(elec)->waits,ZKELECTION_NTIMERS,(index),(shortest))

enum zkelec_timer {
  zkelection_timer_default = 0
#define ZKELECTION_TIMER_DEFAULT zkelection_timer_default
};

typedef struct zkelection_ctx {
  apr_pool_t *pool;
  const char *session_key;
  zkelection_t *e;
} zkelection_ctx_t;

struct zkelection {
  apr_pool_t *pool;
  struct zkws *ws;
  const char *path;
  const char *prefix,*name;
  apr_size_t plen;
  apr_pool_t *member_pool;
  const char *winner;
  apr_array_header_t *members;
  zktool_timer_t timers[ZKELECTION_NTIMERS];
  zktool_wait_t waits[ZKELECTION_NTIMERS];
  const zeke_connection_t *conn;
  zkelection_ctx_t *ctx;
  struct Stat *stat,*prev_stat;
  struct zkws *watch;
  apr_uint32_t state;
  zkelection_callback_fn update_fn;
  const char *saved_session_key;

  zeke_context_t *user_context;
};

/* Forward decls */
static void update_members(zkelection_t*, apr_array_header_t *src);
static zeke_cb_t refresh_complete(const zeke_callback_data_t *cbd);

DECLARE_CONTEXT(zkelection_ctx_t)
#define ZKELECTION_CONTEXT(cbd) fetch_cbd_context(cbd)
#define ZKELECTION(cbd) ZKELECTION_CONTEXT(cbd)->e;

static
zkelection_ctx_t *fetch_cbd_context(const zeke_callback_data_t *cbd)
{
  AN(cbd->connection);
  return (zkelection_ctx_t*)zeke_session_context(cbd->connection,(const char*)cbd->ctx);
}

static
zkelection_ctx_t *fetch_cbwd_context(const zeke_watcher_cb_data_t *cbwd)
{
  AN(cbwd->connection);
  return (zkelection_ctx_t*)zeke_session_context(cbwd->connection,(const char*)cbwd->ctx);
}

static
zeke_cb_t null_callback(const zeke_callback_data_t *cbd)
{
  DESTROY;
}

static zk_status_t election_refresh(zkelection_t *e, zeke_callback_fn completed)
{
  zk_status_t st;
  AN(e);
  AN(completed);

  st = zeke_aget_children(NULL,e->conn,e->path,NULL,completed,ZKELECTION_SESSION_KEY(e));
  if(ZEKE_STATUS_IS_OKAY(st))
    e->state |= ZKELEC_STATE_REFRESH_IN_PROGRESS;
  return st;
}

static void election_update_watcher(const zeke_watcher_cb_data_t *cbwd)
{
  struct zkws *ws;
  zkelection_t *e;
  zkelection_ctx_t *ctx;
  zk_status_t st = ZEOK;

  AN(cbwd);
  ctx = fetch_cbwd_context(cbwd);
  if(!ctx || !ctx->e)
    return;
  e = ctx->e;
  if(e == NULL || e->watch == NULL) return;
  ws = e->watch;
  ZK_CHECK_OBJ_NOTNULL(ws,ZK_WS_MAGIC);
  e->state |= ZKELEC_STATE_DIRTY;
  if ((e->state & ZKELEC_STATE_WAIT_NODEDIR) == 0) {
    if((e->state & ZKELEC_STATE_REFRESH_IN_PROGRESS) == 0)
      st = election_refresh(e,refresh_complete);
    if(ZEKE_STATUS_IS_OKAY(st)) {
      e->state |= ZKELEC_STATE_WAIT_NODEDIR;
    } else
      zeke_fatal(e->path,st);
  }
}

static
void register_watcher_int(zkelection_t *e, const char *node, zeke_callback_data_t *cbd)
{
  struct zkws *ws;
  const char *path;

  AN(e);
  AN(node);

  ws = e->watch;
  if(ws == NULL)
    ws = zk_workspace_new(e->pool);
  else {
    zk_workspace_clear(&ws);
    e->watch = ws;
  }
  ZK_CHECK_OBJ_NOTNULL(ws,ZK_WS_MAGIC);
  if(!cbd)
    cbd = zeke_callback_data_create_ex(e->conn,election_update_watcher,
                                       NULL,1);
  zeke_callback_separate_watcher(cbd, e->ctx->session_key);
  if(node)
    path = zk_workspace_strcat(ws,e->path,"/",node,NULL);
  else
    path = e->path;
  if(path != NULL) {
    zk_status_t st = zeke_aexists(&cbd,e->conn,path,election_update_watcher,
                                  null_callback,ZKELECTION_SESSION_KEY(e));
    if(!ZEKE_STATUS_IS_OKAY(st)) {
      e->watch = NULL;
      zk_workspace_free(&ws);
    } else
      e->watch = ws;
  }
}

static
apr_array_header_t *array_clone(apr_pool_t *p, apr_array_header_t *src)
{
  int i;
  apr_array_header_t *dst = apr_array_make(p,src->nalloc,sizeof(const char*));

  assert(src->elt_size == sizeof(const char*));
  AN(dst);
  for(i = 0; i < src->nelts; i++) {
    APR_ARRAY_PUSH(dst,const char*) = apr_pstrdup(p,APR_ARRAY_IDX(src,i,const char*));
  }

  return dst;
}

static
void update_members(zkelection_t *e, apr_array_header_t *a)
{
  AN(e);
  if(a == e->members)
    return;
  if(a == NULL) {
    if(e->member_pool != NULL)
      apr_pool_destroy(e->member_pool);
    e->member_pool = NULL;
  } else {
    if(e->members != NULL) {
      if(e->member_pool != NULL) {
        apr_pool_destroy(e->member_pool);
        AZ(e->members);
        AAOK(apr_pool_create(&e->member_pool,e->pool));
        zeke_pool_cleanup_indirect(e->member_pool,e->members);
        zeke_pool_cleanup_indirect(e->member_pool,e->winner);
      } else {
        e->members = NULL;
        e->winner = NULL;
      }
    } else if(e->member_pool == NULL) {
      AAOK(apr_pool_create(&e->member_pool,e->pool));
      zeke_pool_cleanup_indirect(e->member_pool,e->members);
      zeke_pool_cleanup_indirect(e->member_pool,e->winner);
    }
    AZ(e->members);
    e->members = array_clone(e->member_pool,a);
  }
}

static
apr_status_t reset_prefix(void *ep)
{
  char *prefix = NULL;

  AN(ep);
  AN(((zkelection_t*)ep)->pool);
  apr_pool_userdata_get((void**)&prefix,ZKELECTION_SESSION_PREFIX "prefix",
                        ((zkelection_t*)ep)->pool);
  if(prefix != NULL) {
    ((zkelection_t*)ep)->prefix = prefix;
    ((zkelection_t*)ep)->plen = strlen(prefix);
  } else {
    ((zkelection_t*)ep)->prefix = "<invalid>";
    ((zkelection_t*)ep)->plen = 9;
  }
  return APR_SUCCESS;
}

static zeke_cb_t refresh_complete(const zeke_callback_data_t *cbd)
{
  zkelection_t *e;
  zk_status_t st = zeke_callback_status_get(cbd);
  const char *watch = NULL;
  const char *winner = NULL;
  int was_winner = 0;
  unsigned i,nchildren;
  apr_array_header_t *memb;

  e = ZKELECTION(cbd);
  if(e == NULL || (e->state & ZKELEC_STATE_RELINQUISH))
    DESTROY;

  assert(e->state & ZKELEC_STATE_REGISTERED);
  e->state &= ~ZKELEC_STATE_REFRESH_IN_PROGRESS;
  if (!ZEKE_STATUS_IS_OKAY(st)) {
    zeke_apr_eprintf(st,"node %s",e->path);
    zeke_safe_shutdown(ZKE_TO_ERROR(st));
    RETURN;
  }
  ZK_CHECK_OBJ_NOTNULL(e->ws,ZK_WS_MAGIC);
  if(e->state & ZKELEC_STATE_WINNER)
    was_winner = 1;
  e->state &= ~(ZKELEC_STATE_WAIT_NODEDIR|ZKELEC_STATE_WAITING);

  nchildren = cbd->vectors.strings->count;
  memb = apr_array_make(cbd->pool,nchildren+1,sizeof(const char*));
  AN(memb);
  for(i = 0; i < nchildren; i++) {
    const char *node = cbd->vectors.strings->data[i];
    if(node)
      APR_ARRAY_PUSH(memb,const char*) = node;
  }

  if(memb->nelts > 0)
    AAOK(zeke_array_sort_seq(memb,cbd->pool));
  update_members(e,memb);
  memb = e->members;
  if(e->prefix && memb && !apr_is_empty_array(memb)) {
    for(i = 0; i < memb->nelts; i++) {
      const char *node = APR_ARRAY_IDX(memb,i,const char*);
      if(strncmp(node,e->prefix,e->plen) == 0) {
        if(i == 0) {
          e->state |= ZKELEC_STATE_WINNER;
          assert(e->state & ZKELEC_STATE_REGISTERED);
          winner = node;
          break;
        } else {
          watch = APR_ARRAY_IDX(memb,i-1,const char*);
        }
      }
      if(!winner) winner = node;
    }
  }

  if(watch) {
    e->state &= ~ZKELEC_STATE_WINNER;
    register_watcher_int(e,watch,NULL);
  }
  if((e->state & ZKELEC_STATE_WINNER) && winner && e->prefix != winner) {
    AN(e->member_pool);
    apr_pool_cleanup_kill(e->member_pool,e,reset_prefix);
    e->prefix = winner;
    e->plen = strlen(winner);
    apr_pool_pre_cleanup_register(e->member_pool,e,reset_prefix);
  } else if((e->state & ZKELEC_STATE_WINNER) && nchildren == 1) {
    AZ(watch);
    AN(winner);
    register_watcher_int(e,NULL,NULL);
  }
  if(!e->winner && winner)
    e->winner = winner;

  e->state |= ZKELEC_STATE_HAVE_NODEDIR;
  if(e->update_fn && (e->state & ZKELEC_STATE_REGISTERED)) {
    if((e->state & ZKELEC_STATE_WINNER) && !was_winner)
      e->update_fn(e,ZKELEC_WIN,e->user_context);
    else if((e->state & ZKELEC_STATE_WINNER) == 0 && was_winner)
      e->update_fn(e,ZKELEC_CHANGE,e->user_context);
  }
  DESTROY;
}

static zeke_cb_t get_nodedir(const zeke_callback_data_t *cbd)
{
  zkelection_t *e;
  zk_status_t st = zeke_callback_status_get(cbd);
  if (ZEKE_STATUS_IS_OKAY(st)) {
    e = ZKELECTION(cbd);
    if(e == NULL) DESTROY;
    ZK_CHECK_OBJ_NOTNULL(e->ws,ZK_WS_MAGIC);
    zk_workspace_clear(&e->ws);
    e->state &= ~ZKELEC_STATE_STARTING;
    e->state |= ZKELEC_STATE_REGISTERED;
    if((e->state & ZKELEC_STATE_REFRESH_IN_PROGRESS) == 0)
      st = election_refresh(e,refresh_complete);
    if(!ZEKE_STATUS_IS_OKAY(st)) {
      zeke_apr_eprintf(st,"node %s",e->path);
      zeke_safe_shutdown(ZKE_TO_ERROR(st));
      return 0;
    }
    e->state |= ZKELEC_STATE_WAIT_NODEDIR;
  }
  DESTROY;
}

static zeke_cb_t join_election(const zeke_callback_data_t *cbd)
{
  zkelection_t *e;
  zk_status_t st = zeke_callback_status_get(cbd);
  if (ZEKE_STATUS_IS_OKAY(st)) {
    const char *fullpath;
    const struct Stat *s;
    zeke_callback_data_t *newcbd;
    e = ZKELECTION(cbd);
    if(e == NULL) DESTROY;
    ZK_CHECK_OBJ_NOTNULL(e->ws,ZK_WS_MAGIC);
    AN(cbd->stat);
    s = cbd->stat;
    if(e->stat == NULL) {
      e->stat = apr_palloc(e->pool,sizeof(struct Stat));
      e->state |= ZKELEC_STATE_WAITING;
    }
    if(e->prev_stat) {
      if(e->prev_stat->version != s->version ||e->prev_stat->cversion != s->cversion ||
         e->prev_stat->aversion != s->aversion)
        e->state |= ZKELEC_STATE_DIRTY;
      e->prev_stat->version = s->version;
      e->prev_stat->aversion = s->aversion;
      e->prev_stat->cversion = s->cversion;
    }
    e->stat->version = s->version;
    e->stat->cversion = s->cversion;
    e->stat->aversion = s->aversion;
    if(e->prev_stat == NULL)
      e->prev_stat = (struct Stat*)apr_pmemdup(e->pool,e->stat,sizeof(struct Stat));
    fullpath = zk_workspace_nodecat(e->ws,e->path,e->prefix,NULL);
    AN(fullpath);
    newcbd = zeke_callback_data_create_ex(cbd->connection,NULL,cbd->pool,0);
    st = zeke_acreate(&newcbd,e->conn,fullpath,NULL,0,NULL,ZOO_EPHEMERAL|ZOO_SEQUENCE,
                      get_nodedir,ZKELECTION_SESSION_KEY(e));
    if(!ZEKE_STATUS_IS_OKAY(st)) {
      zeke_apr_eprintf(st,"creating seq node %s",fullpath);
      zeke_safe_shutdown(ZKE_TO_ERROR(st));
    }
    RETURN;
  }
  DESTROY;
}

ZEKE_HIDDEN
zk_status_t zkelection_refresh(zkelection_t *e)
{
  zk_status_t st;
  AN(e);
  if((e->state & (ZKELEC_STATE_INIT|ZKELEC_STATE_STARTING)) != 0 ||
      (e->state & ZKELEC_STATE_REGISTERED) == 0)
    return APR_EINVAL;

  if((e->state & ZKELEC_STATE_REFRESH_IN_PROGRESS) == 0)
    st = election_refresh(e,refresh_complete);
  else
    st = APR_EBUSY;
  return st;
}

static
apr_status_t zkelection_cleanup(void *ep)
{
  zkelection_t *e = (zkelection_t*)ep;
  if(e && e->ctx && e->ctx->session_key)
    zeke_session_context_release(e->conn,e->ctx->session_key);
  return APR_SUCCESS;
}

static
void zkelection_session_context_cleanup(zeke_context_t *zctx,void *ep)
{
  zkelection_ctx_t *ctx = (zkelection_ctx_t*)ep;

  if(ctx) {
    ctx->session_key = NULL;
    if(ctx->e)
      ctx->e->ctx = NULL;
    ctx->e = NULL;
  }
}

ZEKE_HIDDEN
zk_status_t zkelection_start(zkelection_t *e, apr_interval_time_t *timeout)
{
  zk_status_t st = APR_SUCCESS;
  AN(e);
  if(timeout && *timeout > APR_TIME_C(-1))
    e->timers[ZKELECTION_TIMER_DEFAULT] = *timeout;

  zkelection_reset_timers(e,ZKELECTION_TIMER_DEFAULT,APR_TIME_C(-1));
  if(e->ctx == NULL) {
    const char *key = e->saved_session_key;
    if(key == NULL)
      key = ZKELECTION_NEW_SESSION_KEY(e,e->pool);
    AN(key);
    e->ctx = zeke_session_context_create(e->conn,key,sizeof(*e->ctx));
    AN(e->ctx);
    if(e->ctx->session_key == NULL) {
      e->ctx->session_key = apr_pstrdup(zeke_session_pool(e->conn),key);
      e->saved_session_key = e->ctx->session_key;
    }
    e->ctx->e = e;
    if(e->ctx->pool == NULL) {
      e->ctx->pool = zeke_session_pool(e->conn);
      zeke_session_cleanup_register(e->conn,e->ctx->session_key,e->ctx,
                                      zkelection_session_context_cleanup);
    }
  }
  if(e->state & ZKELEC_STATE_STARTING)
    return APR_EBUSY;
  e->state = ZKELEC_STATE_INIT|ZKELEC_STATE_STARTING;
  st = zeke_aexists(NULL,e->conn,e->path,NULL,join_election,ZKELECTION_SESSION_KEY(e));
  if(!ZEKE_STATUS_IS_OKAY(st))
    zeke_apr_eprintf(st,"zkelection %s",e->path);
  return st;
}

ZEKE_HIDDEN
void zkelection_context_set(zkelection_t *e,
                                   void *data)
{
  AN(e);
  e->user_context = data;
}

ZEKE_HIDDEN
void *zkelection_context_get(zkelection_t *e)
{
  AN(e);
  return e->user_context;
}

ZEKE_HIDDEN
zk_status_t zkelection_cancel(zkelection_t *e)
{
  AN(e);
  e->user_context = NULL;
  if(e->ctx && e->ctx->session_key) {
    zeke_session_context_release(e->conn,e->ctx->session_key);
    return APR_SUCCESS;
  }
  return APR_ENOENT;
}

ZEKE_HIDDEN
int zkelection_member_count(const zkelection_t *e)
{
  AN(e);
  if(!e->members || (e->state & ZKELEC_STATE_REGISTERED) == 0)
    return -1;

  return (int)e->members->nelts;
}

static
zeke_cb_t relinquish_complete(const zeke_callback_data_t *cbd)
{
  zkelection_t *e = ZKELECTION(cbd);
  if(e) {
    ZK_CHECK_OBJ_NOTNULL(e->ws, ZK_WS_MAGIC);
    zk_workspace_clear(&e->ws);
    if(e->member_pool) {
      apr_pool_destroy(e->member_pool);
      AZ(e->member_pool);
      AZ(e->members);
    }
    e->state &= ~(ZKELEC_STATE_WINNER|ZKELEC_STATE_REGISTERED|ZKELEC_STATE_STARTING);
    if(e->state & ZKELEC_STATE_RESTART)
      zkelection_start(e,NULL);
  }
  DESTROY;
}

ZEKE_HIDDEN
zk_status_t zkelection_restart(zkelection_t *e)
{
  AN(e);
  LOG("ELECTION RESTART");
  if((e->state & ZKELEC_STATE_REGISTERED) && (e->state & ZKELEC_STATE_RELINQUISH)) {
    e->state = ZKELEC_STATE_RESTART;
    LOG("  (deferred)");
    return ZEOK;
  }
  if(e->state & (ZKELEC_STATE_REGISTERED|ZKELEC_STATE_STARTING)) {
    LOG("  FAILED");
    return APR_EBUSY;
  }
  return zkelection_start(e,NULL);
}

ZEKE_HIDDEN
zk_status_t zkelection_relinquish_leader(zkelection_t *e)
{
  char *fullpath;

  if(!e || (e->state & ZKELEC_STATE_REGISTERED) == 0) {
    zeke_eprintf("STATE: %d\n",e->state);
    zeke_safe_shutdown(1);
    return APR_EINVAL;
  }
  if((e->state & ZKELEC_STATE_WINNER) == 0)
    return APR_ENOENT;

  ZK_CHECK_OBJ_NOTNULL(e->ws, ZK_WS_MAGIC);
  AN(e->prefix);
  fullpath = zk_workspace_nodecat(e->ws,e->path,e->prefix,NULL);
  AN(fullpath);
  e->state |= ZKELEC_STATE_RELINQUISH;
  return zeke_adelete(NULL,e->conn,fullpath,-1,relinquish_complete,
                      ZKELECTION_SESSION_KEY(e));
}

ZEKE_HIDDEN
zk_status_t zkelection_create(zkelection_t **elec,
                          const char *root,
                          const char *ident,
                          zkelection_callback_fn update_fn,
                          const zeke_connection_t *conn,
                          apr_pool_t *pool)
{
  static apr_pool_t *p = NULL;
  zk_status_t st = APR_SUCCESS;
  zkelection_t *e;
  if(!elec || !conn || (root && !*root))
    return APR_EINVAL;

  if (pool == NULL) {
    if(!p) {
      AAOK(apr_pool_create_unmanaged(&p));
      zeke_pool_tag(p,"global perm zkjobs election pool");
    }
    AN(p);
    pool = p;
  }
  AAOK(apr_pool_create(&pool,pool));
  AN(pool);
  e = (zkelection_t*)apr_pcalloc(pool,sizeof(*e));
  AN(e);
  zeke_pool_tag(pool,"zkelection pool");
  e->pool = pool;
  e->ws = zk_workspace_new(pool);
  ZK_CHECK_OBJ_NOTNULL(e->ws, ZK_WS_MAGIC);
  e->path = apr_pstrdup(pool,(root ? root : "/"));
  if(!ident) {
    ident = zeke_get_fqdn(pool);
    if(ident == NULL) {
      st = APR_ENOMEM;
      goto zkelec_create_error;
    }
  }
  AN(ident);
  e->name = apr_psprintf(e->pool,"%s:%lu",ident,(unsigned long)getpid());
  e->prefix = apr_pstrcat(e->pool,e->name,"_",NULL);
  e->plen = strlen(e->prefix);
  AAOK(apr_pool_userdata_setn(e->prefix,ZKELECTION_SESSION_PREFIX "prefix",
                              NULL,e->pool));
  e->members = NULL;
  e->winner = NULL;
  e->timers[ZKELECTION_TIMER_DEFAULT] = APR_TIME_C(-1);
  e->conn = conn;
  e->update_fn = update_fn;
  if(st != APR_SUCCESS)
    goto zkelec_create_error;
  zeke_pool_cleanup_register(e->pool,e,zkelection_cleanup);
  *elec = e;
  return st;

zkelec_create_error:
  if(pool && (!p || p != pool))
    apr_pool_destroy(pool);
  return st;
}

ZEKE_HIDDEN
apr_pool_t *zkelection_pool_get(const zkelection_t *e)
{
  AN(e); AN(e->pool);
  return e->pool;
}

ZEKE_HIDDEN
const apr_array_header_t *zkelection_members(const zkelection_t *e)
{
  AN(e);
  return e->members;
}

ZEKE_HIDDEN
struct zkws *zkelection_workspace(const zkelection_t *e)
{
  AN(e);
  return e->ws;
}

ZEKE_HIDDEN
apr_uint32_t zkelection_state(const zkelection_t *e)
{
  AN(e);
  return e->state;
}

ZEKE_HIDDEN
const char *zkelection_path(const zkelection_t *e)
{
  AN(e);
  return e->path;
}

ZEKE_HIDDEN
zk_status_t zkelection_name_get(const zkelection_t *e, const char **buf,
                                 apr_size_t *blen)
{
  AN(e);
  if(e->name) {
    if(buf)
      *buf = e->name;
    if(blen)
      *blen = strlen(e->name);
    return APR_SUCCESS;
  }
  return APR_ENOENT;
}

ZEKE_HIDDEN
const char *zkelection_winner(const zkelection_t *e)
{
  AN(e);
  return e->winner;
}
