#include "internal.h"
#include "connection.h"

#include "libzeke_event.h"
#include "libzeke_tree.h"
#include "libzeke_indirect.h"

#define STOP_MARKER APR_TIME_C(1)
#define STOP_MARKER_SENTINEL (STOP_MARKER * APR_TIME_C(-1))
#define NULL_TIME APR_TIME_C(0)

#define ZKTREE_ELEM_INIT(ep) APR_RING_ELEM_INIT((ep),l)
#define ZKTREE_ELEM_PARENT(ep) (ZKTREE_FIELD((ep),parent))
#define ZKTREE_ELEM_PARENT_HEAD(ep,or) ((ep) && ZKTREE_ELEM_PARENT(ep) ? ZKTREE_ELEM_HEAD(ZKTREE_ELEM_PARENT(ep),or) : (or))
#define ZKTREE_ELEM_HEAD(ep,or) ((ep) ? ZKTREE_ELEM_CHILDREN(ep) : (or))
#define ZKTREE_ELEM_CHILDREN(ep) &(ZKTREE_FIELD((ep),children))
#define ZKTREE_ELEM_FIRST_CHILD(ep) ZKTREE_FIRST(&(ZKTREE_FIELD((ep),children)))
#define ZKTREE_ELEM_HAS_CHILDREN(ep) !ZKTREE_EMPTY(ZKTREE_ELEM_CHILDREN(ep))

#define ZKTREE_NODE_CHECK(n) do {\
  ZEKE_MAGIC_CHECK((n), ZEKE_TREE_NODE_MAGIC); \
  ZEKE_MAGIC_CHECK((n)->priv, ZEKE_TREE_DATA_MAGIC); \
} while(0)

/*****************************************************************/

#define ZEKE_TREE_SESSION_PREFIX "zekeTreeSession:"
#define ZEKE_TREE_CONNECTION_KEY "zeke-tree-conn"
#define ZEKE_TREE_CONNECTION_REFKEY "zeke-tree-conn-ref"

#define ZKTFLAG_SET_FINAL_STATE(t,f) do { \
  ZKTFLAG_SET_STOPPED(t); \
  (t)->flags &= 0x0003; \
  (t)->flags |= (f); \
} while(0)

enum {
  zktflag_started = 0x0001,
#define ZKTFLAG_STARTED zktflag_started
#define ZKTFLAG_SET_STARTED(t) ((t)->flags |= ZKTFLAG_STARTED)
#define ZKTFLAG_SET_STOPPED(t) ((t)->flags &= ~ZKTFLAG_STARTED)
  zktflag_finished = 0x0002,
#define ZKTFLAG_FINISHED zktflag_finished
#define ZKTFLAG_SET_FINISHED(t) \
        ZKTFLAG_SET_FINAL_STATE((t),ZKTFLAG_FINISHED)
  zktflag_error = 0x0004,
#define ZKTFLAG_ERROR zktflag_error
#define ZKTFLAG_SET_ERROR(t,st) do { \
        ZKTFLAG_SET_FINAL_STATE((t),ZKTFLAG_FINISHED|ZKTFLAG_ERROR); \
        (t)->status = (st); \
} while(0)
  zktflag_cancelled = 0x0008,
#define ZKTFLAG_CANCELLED zktflag_cancelled
#define ZKTFLAG_SET_CANCELLED(t) \
        ZKTFLAG_SET_FINAL_STATE((t),ZKTFLAG_CANCELLED)
  zktflag_hold  = 0xff00,
#define ZKTFLAG_HOLD zktflag_hold
#define ZKTFLAG_SET_HOLD(t) do { \
        (t)->flags &= ~ZKTFLAG_CANCELLED; \
        ZKTFLAG_SET_FINAL_STATE((t),ZKTFLAG_HOLD); \
        (t)->status = ZEOK; \
} while(0)
};

#define ZKT_IS_STOPPED(t) (((t)->flags & ZKTFLAG_STARTED) == 0)
#define ZKT_IS_STARTED(t) (((t)->flags & ZKTFLAG_STARTED) != 0)
#define ZKT_IS_FINISHED(t) (((t)->flags & ZKTFLAG_FINISHED) != 0)
#define ZKT_IS_ERROR(t) (((t)->flags & ZKTFLAG_ERROR) != 0)
#define ZKT_IS_CANCELLED(t) (((t)->flags & ZKTFLAG_CANCELLED) != 0)
#define ZKT_IS_HELD(t) ((t)->flags >= ZKTFLAG_HOLD)

typedef struct {
  zeke_magic_t magic;
#define ZEKE_TREE_SESSION_MAGIC 0xb2cc36eb
  apr_pool_t *pool,*tpool;
  struct zeke_tree *tree;
  apr_time_t start_time, end_time;
  apr_interval_time_t *tp;
  const char *path;
  apr_int32_t num_nodes;
  zeke_tree_node_t *pos;

  ZKTREE_HEAD_VAR(ring) root;
} zeke_tree_context_t;

struct zeke_tree {
  zeke_magic_t magic;
#ifndef ZEKE_TREE_MAGIC
#define ZEKE_TREE_MAGIC 0xbe63cc2b
#endif
  apr_pool_t *pool;
  zk_status_t status;
  const zeke_connection_t *conn;
  const char *session_context_key;
  zeke_context_t *user_context;
  apr_interval_time_t timeout;
  zeke_tree_complete_fn complete_fn;
  zeke_tree_timeout_fn timeout_fn;
  zeke_tree_filter_fn filter_fn;
  apr_uint32_t flags;
  const char *path;
};

struct zeke_tree_data {
  zeke_magic_t magic;
#define ZEKE_TREE_DATA_MAGIC 0x7a328bb4
  char *name,*absname;
  struct zeke_tree_node *parent;
  apr_time_t last;
  apr_hash_t *userdata;
  apr_uint32_t flags;
  ZKTREE_HEAD_VAR(ring) children;
};

struct zeke_tree_iter {
  zeke_magic_t magic;
#define ZEKE_TREE_ITER_MAGIC 0xb38ff013
  apr_pool_t *pool;
  zeke_tree_iter_type_e type;
  zeke_tree_context_t *ctx;
  zeke_tree_ring_t *head;
  zeke_tree_node_t *cur,*next;
  apr_time_t start;
  void *userdata;
  void *state;
};

static inline struct zeke_tree_data *ZKTREE_NODE(const zeke_tree_node_t *n)
{
  ZEKE_MAGIC_CHECK((n), ZEKE_TREE_NODE_MAGIC);
  ZEKE_MAGIC_CHECK((n)->priv, ZEKE_TREE_DATA_MAGIC);
  return n->priv;
}

#define ZKTREE_FIELD(n,f) (n)->priv->f
#define ZKTREE_NODE_GET(n,f) ZKTREE_NODE(n)->f
#define ZKTREE_NODE_SET(n,f,val) do { \
  struct zeke_tree_data * f##_obj = ZKTREE_NODE(n); \
  f##_obj->f = val; \
} while(0)

ZEKE_POOL_IMPLEMENT_ACCESSOR(tree)
ZEKE_IMPLEMENT_ACCESSOR(tree,context,user_context)

#undef ZAN
#define ZAN(c, ...) assert(((void*)(c) != (void*)0), ## __VA_ARGS__ )
#undef ZAZ
#define ZAZ(c, ...) assert(((void*)(c) == (void*)0), ## __VA_ARGS__ )
#ifndef ZISOKAY
#define ZISOKAY ZEKE_STATUS_IS_OKAY
#endif

static apr_time_t now = NULL_TIME;

static int tree_iter_check_last(apr_array_header_t *tsa, zeke_tree_node_t *node)
{
  int i;
  apr_interval_time_t ts;

  if(tsa && tsa->nelts > 0) {
    for(i = 0; i < tsa->nelts; i++) {
      ts = APR_ARRAY_IDX(tsa,i,apr_interval_time_t);
      if(ts < APR_TIME_C(0) && ZKTREE_FIELD(node,last) == (APR_TIME_C(-1) * ts)) {
        ZKTREE_NODE_SET(node,last,now);
        return -1; /* ignore this one */
      } else if(ts > APR_TIME_C(0) && ZKTREE_FIELD(node,last) < ts)
        return 1;
    }
  }
  return 0;
}

/* function to return a sequence number which must *always* be numerically
 * greater than the previous number returned. Usually this is just the time
 * but it may be higher if called frequently. It minimum it will increase
 * 100 usec per call.
 */
static
apr_time_t monotonic_seq(void)
{
  static apr_time_t base = NULL_TIME;
  apr_time_t update = NULL_TIME;
#ifdef ZEKE_USE_THREADS
  static apr_thread_mutex_t *mutex = NULL;
  static apr_pool_t *pool = NULL;

  if(mutex == NULL) {
    static volatile apr_uint32_t spinlock = 0;
    while(apr_atomic_cas32(&spinlock,1,0) == 0)
      ;
    if(mutex == NULL) {
      ZAZ(pool);
      apr_pool_create_unmanaged(&pool);
      ZAN(pool);
      zeke_pool_tag(pool,"unmanaged monotonic seq pool");
      assert(apr_thread_mutex_create(&mutex,APR_THREAD_MUTEX_DEFAULT,pool) == APR_SUCCESS);
    }
    assert(apr_atomic_dec32(&spinlock) == 0);
  }
  assert(apr_thread_mutex_lock(mutex) == APR_SUCCESS);
#endif

  if(now < APR_TIME_C(10000)) {
    now = update = apr_time_now();
  }
  if(base < APR_TIME_C(10000) || base < now)
    base = now;
  else {
    if(update == NULL_TIME)
      update = apr_time_now();
    if (now < update)
      now = update;
    while(update <= base)
      update += APR_TIME_C(100);
    base = update;
  }
#ifdef ZEKE_USE_THREADS
  assert(apr_thread_mutex_unlock(mutex) == APR_SUCCESS);
#endif
  return base;
}

static
zeke_tree_node_t *tree_iter_v(zeke_tree_ring_t **headp,
                              zeke_tree_node_t *node,
                              zeke_tree_ring_t *root,
                              void **state,
                              apr_pool_t *p,
                              va_list ap)
{
  int i;
  apr_interval_time_t ts;
  apr_time_t update = NULL_TIME;
  apr_array_header_t *tsa;
  zeke_tree_ring_t *head;
  ZAN(p);
  ZAN(headp);
  ZAN(root);
  ZAN(state);

  tsa = *state;

  update = monotonic_seq();

  if(tsa == NULL) {
    tsa = apr_array_make(p,1,sizeof(apr_interval_time_t));
    *state = tsa;

    for(ts = va_arg(ap,apr_interval_time_t); ts != NULL_TIME;
        ts = va_arg(ap,apr_interval_time_t)) {
      APR_ARRAY_PUSH(tsa,apr_interval_time_t) = ts;
    }

    head = ZKTREE_ELEM_PARENT_HEAD(node,root);
    ZAN(head);
  } else {
    head = *headp;
    assert(ZKTREE_ELEM_PARENT_HEAD(node,root) == head);
  }

  if(!node) {
    if(!ZKTREE_EMPTY(head)) {
      node = ZKTREE_FIRST(head);
      LOG("ITER: first [%s]",ZKTREE_FIELD(node,name));
      if(ZKTREE_NODE_GET(node,last) == STOP_MARKER) {
        ZKTREE_NODE_SET(node,last,update);

      } else if(ZKTREE_NODE_GET(node,last) == NULL_TIME)
        goto tree_iter_done;
      LOG("ITER: redo [%s]",ZKTREE_FIELD(node,name));
      goto tree_iter_dive;
    } else {
      *headp = NULL;
      *state = NULL;
      return NULL;
    }
  } else {
    assert(node != ZKTREE_SENTINEL(head));
    LOG("ITER [%s]",ZKTREE_FIELD(node,absname));
    ZKTREE_NODE_CHECK(node);
  }
  do {
    ZKTREE_NODE_CHECK(node);
    if(ZKTREE_NEXT(node) != ZKTREE_SENTINEL(head)) {
      LOG("ITER [next %s]",ZKTREE_FIELD(ZKTREE_NEXT(node),absname));
    }
    while(ZKTREE_NEXT(node) == ZKTREE_SENTINEL(head)) {
      LOG("ITER POP");
      node = ZKTREE_ELEM_PARENT(node);
      if(!node) {
        head = root;
        ZKTREE_FOREACH(node,head) {
          LOG("CHECK: %s %"APR_UINT64_T_FMT,
                ZKTREE_FIELD(node,absname),ZKTREE_FIELD(node,last));
          if(ZKTREE_FIELD(node,last) == STOP_MARKER)
            ZKTREE_NODE_SET(node,last,update);
          else if(ZKTREE_FIELD(node,last) == NULL_TIME) {
            WARN("missed %s %" APR_UINT64_T_FMT " ?? ",
                ZKTREE_FIELD(node,absname),ZKTREE_FIELD(node,last));
            goto tree_iter_dive;
          }
        }
        LOG("ITER: FINISHED");
        *headp = NULL;
        *state = NULL;
        return NULL;
      }
      ZKTREE_NODE_CHECK(node);
      LOG("ITER: PARENT");
      head = ZKTREE_ELEM_PARENT_HEAD(node,root);
      i = tree_iter_check_last(tsa,node);
      if(i == 1)
        goto tree_iter_done;
      else if(i != -1 && ZKTREE_FIELD(node,last) == NULL_TIME)
        goto tree_iter_done;
    }
    node = ZKTREE_NEXT(node);
  } while(node == ZKTREE_SENTINEL(head));

tree_iter_dive:
  assert(node != ZKTREE_SENTINEL(head));
  if(node)
    ZKTREE_NODE_CHECK(node);
  ZKTREE_CHECK_CONSISTENCY(ZKTREE_ELEM_CHILDREN(node));
  while(!ZKTREE_EMPTY(ZKTREE_ELEM_CHILDREN(node))) {
    LOG("ITER: diving");
    if(ZKTREE_FIELD(node,last) == STOP_MARKER) {
      ZKTREE_NODE_SET(node,last,update);
      if(ZKTREE_NEXT(node) != ZKTREE_SENTINEL(head))
        node = ZKTREE_NEXT(node);
      else {
        head = NULL;
        node = NULL;
        goto tree_iter_done;
      }
    } else if(ZKTREE_FIELD(node,last) == NULL_TIME ||
              tree_iter_check_last(tsa,node) == 1) {
      head = ZKTREE_ELEM_CHILDREN(node);
      node = ZKTREE_FIRST(head);
    }
    assert(node != ZKTREE_SENTINEL(head));
  }

tree_iter_done:
  *headp = head;
  return node;
}

static
zeke_tree_node_t *tree_iter(zeke_tree_ring_t **headp,
                            zeke_tree_node_t *node,
                            zeke_tree_ring_t *root,
                            void **state,
                            apr_pool_t *p,...)
{
  va_list ap;
  zeke_tree_node_t *next;
  va_start(ap,p);
  next = tree_iter_v(headp,node,root,state,p,ap);
  va_end(ap);
  return next;
}

static zeke_connection_t *zeke_tree_connection(const zeke_tree_t *t)
{
  ZEKE_MAY_ALIAS(zeke_connection_t) *conn = NULL;
  zk_status_t st;

  ZEKE_MAGIC_CHECK(t, ZEKE_TREE_MAGIC);

  st = apr_pool_userdata_get((void**)&conn,
                                         ZEKE_TREE_CONNECTION_REFKEY,
                                         t->pool);
  if(!ZISOKAY(st))
    zeke_fatal("no connection found for tree object",st);

  ZEKE_ASSERT(conn,"no connection found for tree object [assert]");
  return conn;
}

ZEKE_API(zeke_context_t*) zeke_tree_context(const zeke_tree_t *t)
{
  ZEKE_MAGIC_CHECK(t, ZEKE_TREE_MAGIC);
  return t->user_context;
}

ZEKE_API(zk_status_t) zeke_tree_build_filter_set(zeke_tree_t *t,
                                                 zeke_tree_filter_fn f)
{
  ZEKE_MAGIC_CHECK(t, ZEKE_TREE_MAGIC);
  t->filter_fn = f;
  return ZEOK;
}

ZEKE_API(void) zeke_tree_context_set(zeke_tree_t *t, zeke_context_t *ctx)
{
  ZEKE_MAGIC_CHECK(t, ZEKE_TREE_MAGIC);
  t->user_context = ctx;
}

ZEKE_API(zk_status_t) zeke_tree_request_time_get(apr_interval_time_t *tp,
                                                 const zeke_tree_t *t)
{
  const zeke_connection_t *c = zeke_tree_connection(t);
  zeke_tree_context_t *ctx;
  ZEKE_MAGIC_CHECK(t, ZEKE_TREE_MAGIC);
  ZAN(c);
  ctx = zeke_session_context(c, t->session_context_key);
  if(!ctx || !tp)
    return APR_EINVAL;
  if(!ctx->end_time) {
    *tp = apr_time_now() - ctx->start_time;
    return APR_EBUSY;
  }

  *tp = (ctx->end_time - ctx->start_time);
  return ZEOK;
}

ZEKE_API(zk_status_t) zeke_tree_cancel(zeke_tree_t *t)
{
  const zeke_connection_t *c = zeke_tree_connection(t);
  zeke_tree_context_t *ctx;
  ZEKE_MAGIC_CHECK(t, ZEKE_TREE_MAGIC);
  ZAN(c);
  ctx = zeke_session_context(c, t->session_context_key);

  if(ctx->start_time && !ctx->end_time)
    ctx->end_time = apr_time_now();
  if(ctx && ctx->tree) {
    ZKTFLAG_SET_CANCELLED(t);
    ctx->tree = NULL;
  } else
    return APR_ENOENT;
  return ZEOK;
}

ZEKE_API(zk_status_t) zeke_tree_destroy(zeke_tree_t *t)
{
  apr_pool_t *p;
  zeke_connection_t *c;
  ZEKE_MAGIC_CHECK(t, ZEKE_TREE_MAGIC);
  p = t->pool;

  if((!ZKT_IS_FINISHED(t) && !ZKT_IS_CANCELLED(t)) || ZKT_IS_HELD(t)) {
    ERR("cannot destroy a running or locked tree operation (%s)",t->path);
    return APR_EINVAL;
  }

  c = zeke_tree_connection(t);
  if(c)
    zeke_session_context_release(c,t->session_context_key);

  t->pool = NULL;
  if(p != NULL)
    apr_pool_destroy(p);
  return ZEOK;
}

ZEKE_API(zk_status_t) zeke_tree_status(const zeke_tree_t *t)
{
  ZEKE_MAGIC_CHECK(t, ZEKE_TREE_MAGIC);
  return t->status;
}

ZEKE_API(int) zeke_tree_is_started(const zeke_tree_t *t)
{
  ZEKE_MAGIC_CHECK(t, ZEKE_TREE_MAGIC);
  return (ZKT_IS_STARTED(t) || ZKT_IS_FINISHED(t));
}

ZEKE_API(int) zeke_tree_is_complete(const zeke_tree_t *t)
{
  ZEKE_MAGIC_CHECK(t, ZEKE_TREE_MAGIC);
  return ZKT_IS_FINISHED(t);
}

ZEKE_API(int) zeke_tree_is_error(const zeke_tree_t *t, zk_status_t *stp)
{
  ZEKE_MAGIC_CHECK(t, ZEKE_TREE_MAGIC);
  if(ZKT_IS_FINISHED(t) && ZKT_IS_ERROR(t)) {
    if(stp)
      *stp = t->status;
    return 1;
  }
  return 0;
}

ZEKE_API(int) zeke_tree_is_cancelled(const zeke_tree_t *t)
{
  ZEKE_MAGIC_CHECK(t, ZEKE_TREE_MAGIC);
  return (ZKT_IS_FINISHED(t) && ZKT_IS_CANCELLED(t));
}

ZEKE_API(int) zeke_tree_is_held(const zeke_tree_t *t)
{
  ZEKE_MAGIC_CHECK(t, ZEKE_TREE_MAGIC);
  return ZKT_IS_HELD(t);
}

ZEKE_API(int) zeke_tree_hold_set(zeke_tree_t *t, int on)
{
  int was = zeke_tree_is_held(t);
  if(on)
    t->flags |= ZKTFLAG_HOLD;
  else
    t->flags &= ~ZKTFLAG_HOLD;
  return was;
}

ZEKE_API(int) zeke_tree_check_consistency(ZKTREE_HEAD_VAR(ring) *head)
{
  /* struct zeke_tree_node *n; */
  int i = 0;
  APR_RING_CHECK_CONSISTENCY(head,zeke_tree_node,l);
  /*
  ZKTREE_FOREACH(n,head) {
    ZEKE_MAGIC_CHECK(n,ZEKE_TREE_NODE_MAGIC);
    APR_RING_CHECK_ELEM(head,zeke_node_tree,l,ZKTREE_FIELD(n,absname));
    i++;
    if(ZKTREE_ELEM_HAS_CHILDREN(n))
      i += zeke_tree_check_consistency(&n->children);
  }
  */
  return i;
}

ZEKE_API(apr_status_t) zeke_tree_node_userdata_get(void **dp,
                                               const char *key,
                                              zeke_tree_node_t *node)
{
  void *val = NULL;
  ZKTREE_NODE_CHECK(node);

  if(!dp || !key)
    return APR_EINVAL;
  if(ZKTREE_FIELD(node,userdata) == NULL)
    return APR_ENOENT;

  val = apr_hash_get(ZKTREE_FIELD(node,userdata),key,APR_HASH_KEY_STRING);
  if(val == NULL)
    return APR_ENOENT;
  *dp = val;
  return APR_SUCCESS;
}

ZEKE_API(void) zeke_tree_node_userdata_set(const void *d,
                                           const char *key,
                                           zeke_tree_node_t *node)

{
  apr_pool_t *p;
  ZKTREE_NODE_CHECK(node);

  if(!key)
    return;
  if(ZKTREE_FIELD(node,userdata) == NULL) {
    zeke_tree_node_t *n;
    for(n = node; ZKTREE_FIELD(n,parent) != NULL; n = ZKTREE_FIELD(n,parent))
      ;
    ZAN(n);
    ZAN(ZKTREE_FIELD(node,userdata));
    p = apr_hash_pool_get(ZKTREE_FIELD(n,userdata));
    if(n != node)
      ZKTREE_FIELD(node,userdata) = apr_hash_make(p);
  } else p = apr_hash_pool_get(ZKTREE_FIELD(node,userdata));

  ZAN(ZKTREE_FIELD(node,userdata));
  apr_hash_set(ZKTREE_FIELD(node,userdata),apr_pstrdup(p,key),APR_HASH_KEY_STRING,d);
}

ZEKE_API(void) zeke_tree_node_info_get(const zeke_tree_node_t *n,
                                       const char **name,
                                       const char **absname,
                                       apr_interval_time_t *last)
{
  ZKTREE_NODE_CHECK(n);

  if(name)
    *name = ZKTREE_FIELD(n,name);
  if(absname)
    *absname = ZKTREE_FIELD(n,absname);
  if(last)
    *last = ZKTREE_FIELD(n,last);
}

ZEKE_API(int) zeke_tree_node_count(const zeke_tree_t *t)
{
  zeke_tree_context_t *ctx;
  ZEKE_MAGIC_CHECK(t, ZEKE_TREE_MAGIC);

  if(!ZKT_IS_STARTED(t) && !ZKT_IS_FINISHED(t))
    return -1;

  ctx = zeke_session_context(t->conn,t->session_context_key);
  ZEKE_MAGIC_CHECK(ctx, ZEKE_TREE_SESSION_MAGIC);

  return ctx->num_nodes;
}

ZEKE_API(zk_status_t) zeke_tree_head_get(const zeke_tree_t *t,
                                         const char **path,
                                         const ZKTREE_HEAD_VAR(ring) **headp)
{
  zeke_tree_context_t *ctx;
  ZEKE_MAGIC_CHECK(t, ZEKE_TREE_MAGIC);

  if(!ZKT_IS_FINISHED(t))
    return APR_EBUSY;

  ctx = zeke_session_context(t->conn,t->session_context_key);
  ZEKE_MAGIC_CHECK(ctx, ZEKE_TREE_SESSION_MAGIC);
  if(path)
    *path = ctx->path;
  if(headp)
    *headp = &ctx->root;
  return ZEOK;
}

ZEKE_API(const zeke_tree_node_t*)
zeke_tree_node_parent_get(const zeke_tree_node_t *n)
{
  ZKTREE_NODE_CHECK(n);
  return ZKTREE_FIELD(n,parent);
}

ZEKE_API(const zeke_tree_ring_t*)
zeke_tree_node_children_get(const zeke_tree_node_t *n)
{
  ZKTREE_NODE_CHECK(n);
  return ZKTREE_ELEM_CHILDREN(n);
}

static apr_status_t tree_cleanup(void *vctx)
{
  zeke_tree_context_t *ctx = (zeke_tree_context_t*)vctx;

  ZEKE_MAGIC_CHECK(ctx, ZEKE_TREE_SESSION_MAGIC);

  if(ctx->tpool && !ZKTREE_EMPTY(&ctx->root)) {
    zeke_tree_node_t *n,*tmp;
    ZKTREE_FOREACH_SAFE(n,tmp,&ctx->root)
      ZKTREE_REMOVE(n);
  }

  return APR_SUCCESS;
}

static void tree_insert(zeke_tree_ring_t *head, zeke_tree_node_t *node)
{
  zeke_tree_node_t *n;
  const char *name;

  ZEKE_MAGIC_CHECK(node, ZEKE_TREE_NODE_MAGIC);
  ZEKE_MAGIC_CHECK(node->priv, ZEKE_TREE_DATA_MAGIC);

  name = ZKTREE_NODE_GET(node,name);
  ZAN(name);
  ZKTREE_FOREACH(n,head) {
    if(apr_strnatcmp(name,ZKTREE_NODE_GET(n,name)) > 0)
      continue;
    ZKTREE_INSERT_BEFORE(n,node);
    return;
  }

  ZKTREE_INSERT_TAIL(head,node);
}

static zeke_cb_t tree_get(const zeke_callback_data_t *cbd)
{
  int i;
  zk_status_t st = APR_SUCCESS;
  apr_pool_t *p;
  const zeke_connection_t *c = cbd->connection;
  zeke_tree_context_t *ctx = (zeke_tree_context_t*)cbd->ctx;
  zeke_tree_t *t;
  ZKTREE_HEAD_VAR(ring) *head;
  zeke_tree_node_t *pos,*next = NULL;
  void *state = NULL;
  apr_time_t update;

  update = monotonic_seq();
  ZEKE_MAGIC_CHECK(ctx, ZEKE_TREE_SESSION_MAGIC);
  st = zeke_callback_status_get(cbd);
  if(st != ZEOK) {
    t = ctx->tree;
    if(t) {
      ZEKE_MAGIC_CHECK(t, ZEKE_TREE_MAGIC);
      ctx->end_time = now;
      ZKTFLAG_SET_ERROR(t,st);
      if(t->complete_fn && t->complete_fn(t,t->user_context)) {
        if(ctx->tree && ZKT_IS_HELD(ctx->tree))
          return ZEKE_CALLBACK_IGNORE;
      }
    }
    zeke_callback_status_set(cbd,ZEOK);
    return ZEKE_CALLBACK_DESTROY;
  }

  t = ctx->tree;
  if(!t)
    return ZEKE_CALLBACK_DESTROY;

  ZEKE_MAGIC_CHECK(t, ZEKE_TREE_MAGIC);

  if(ctx->tpool == NULL) {
    assert(apr_pool_create(&ctx->tpool,ctx->pool) == APR_SUCCESS);
    zeke_pool_tag(ctx->tpool,apr_psprintf(ctx->tpool,"tree nodes for '%s'",t->path));
    zeke_pool_cleanup_indirect(ctx->tpool,ctx->tpool);
    apr_pool_pre_cleanup_register(ctx->tpool,ctx,tree_cleanup);
  }
  p = ctx->tpool;
  pos = ctx->pos;
  if(pos) {

    LOG("SET --- %s was:%" APR_UINT64_T_FMT " to:%" APR_UINT64_T_FMT,
         ZKTREE_FIELD(pos,absname),
         ZKTREE_FIELD(pos,last),update);
    ZKTREE_FIELD(pos,last) = update;
  }
  head = ZKTREE_ELEM_HEAD(pos,&ctx->root);
  ZAN(head);
  for(i = 0; i < cbd->vectors.strings->count; i++) {
    apr_uint32_t ff = 0; /* filter flags */
    zeke_tree_node_t *node;
    const char *absname = NULL;
    const char *name = NULL;
    if(ctx->tree->filter_fn) {
      name = cbd->vectors.strings->data[i];
      ff = ctx->tree->filter_fn(pos,&name,&absname,ctx->tree->user_context);
      if(ff == ZKTFILTER_DISCARD)
        continue;
      if((ff & ZKTFILTER_REPLACE_NAME) && name) {
        name = apr_pstrdup(p,name);
      } else
        ff &= ~ZKTFILTER_REPLACE_NAME;
      if((ff & ZKTFILTER_REPLACE_ABSNAME) && absname)
        absname = apr_pstrdup(p,absname);
      else
        ff &= ~ZKTFILTER_REPLACE_ABSNAME;
    }
    if((ff & ZKTFILTER_REPLACE_NAME) == 0)
      name = cbd->vectors.strings->data[i];
    if((ff & ZKTFILTER_REPLACE_ABSNAME) == 0)
      absname = NULL;
    node = apr_palloc(p,sizeof(*node));
    ZAN(node);
    node->magic = ZEKE_TREE_NODE_MAGIC;
    node->priv = apr_palloc(p,sizeof(*node->priv));
    ZAN(node->priv);
    node->priv->magic = ZEKE_TREE_DATA_MAGIC;
    ZKTREE_ELEM_INIT(node);
    ZKTREE_INIT(&ZKTREE_FIELD(node,children));
    ZKTREE_FIELD(node,absname) = (char*)absname;
    ZKTREE_NODE_SET(node,parent,pos);
    ZKTREE_NODE_SET(node,flags,ff);
    /* ghetto mark ZKTREE_FIELD(node,last) for later to prevent descending into it */
    if((ff & ZKTFILTER_STOP_DESCENT) == ZKTFILTER_STOP_DESCENT)
      ZKTREE_NODE_SET(node,last,STOP_MARKER);
    else
      ZKTREE_NODE_SET(node,last,NULL_TIME);
    ZKTREE_FIELD(node,userdata) = NULL;
    if(!absname) {
      assert(apr_filepath_merge(&ZKTREE_FIELD(node,absname),(pos ? ZKTREE_FIELD(pos,absname) : ctx->path),
                                name,0,p) == APR_SUCCESS);
    }
    if((ff & ZKTFILTER_REPLACE_NAME) == 0)
      ZKTREE_FIELD(node,name) = (ZKTREE_FIELD(node,absname) + strlen(ZKTREE_FIELD(node,absname))) - strlen(name);
    else
      ZKTREE_FIELD(node,name) = (char*)name;
    ZAN(ZKTREE_FIELD(node,name));
    tree_insert(head,node);
    ctx->num_nodes++;
  }

  ZKTREE_CHECK_CONSISTENCY(head);
  do {
    zeke_tree_node_t *node;

    ZKTREE_FOREACH(node,head) {
      if((ZKTREE_FIELD(node,flags) & ZKTFILTER_STOP_DESCENT) != ZKTFILTER_STOP_DESCENT) {
        next = node;
        break;
      }
    }
  } while(0);

  if(next) {
    pos = next;
    st = zeke_aget_children((zeke_callback_data_t**)&cbd,
                            c,ZKTREE_NODE_GET(pos,absname),NULL,tree_get,ctx);
  } else {
    ZKTREE_FOREACH(pos,head) {
      if(ZKTREE_FIELD(pos,last) == STOP_MARKER)
        ZKTREE_NODE_SET(pos,last,update);
    }
    if(ctx->pos) {
      LOG("pos=%pp%s",ctx->pos,ZKTREE_FIELD(ctx->pos,absname));
      ZKTREE_NODE_CHECK(ctx->pos);
    } else {
      LOG("pos=%pp",ctx->pos);
    }
    for(pos = tree_iter(&head,ctx->pos,&ctx->root,&state,cbd->pool,
                              STOP_MARKER_SENTINEL,NULL_TIME);
        pos != NULL;
        pos = tree_iter(&head,pos,&ctx->root,&state,cbd->pool,
                              STOP_MARKER_SENTINEL,NULL_TIME))
    {
      ZAN(pos);
      ZAN(head);
      ZKTREE_CHECK_CONSISTENCY(head);
      if(ZKTREE_NODE_GET(pos,last) == STOP_MARKER) {
        LOG("not descending into %s",ZKTREE_NODE_GET(pos,absname));
        ZKTREE_NODE_SET(pos,last,update);
        continue;
      }
      LOG("descending into %s",ZKTREE_NODE_GET(pos,absname));
      st = zeke_aget_children((zeke_callback_data_t**)&cbd,
                              c,ZKTREE_NODE_GET(pos,absname),NULL,tree_get,ctx);
      break;
    }
  }
  ctx->pos = pos;
  if(pos)
    ZKTREE_NODE_CHECK(pos);
  if(pos == NULL && ZISOKAY(st)) {
    ctx->pos = pos = NULL;

    if(!ZKT_IS_FINISHED(t) && ctx->end_time <= NULL_TIME)
      ctx->end_time = now;
    t->status = ZEOK;
    ZKTFLAG_SET_FINISHED(t);
    if(t->complete_fn && t->complete_fn(t,t->user_context)) {
      if(ctx->tree && ZKT_IS_HELD(ctx->tree))
        return ZEKE_CALLBACK_IGNORE;
    }
    apr_pool_destroy(ctx->tpool);
    return ZEKE_CALLBACK_DESTROY;
  }

  if(!ZISOKAY(st)) {
    const char *emsg = zeke_perrstr(st,p);
    if(ctx->tree) {
      t = ctx->tree;
      ZEKE_MAGIC_CHECK(t, ZEKE_TREE_MAGIC);
      ZKTFLAG_SET_ERROR(t,cbd->status);
      if(t->complete_fn && t->complete_fn(t,t->user_context)) {
        if(ctx->tree && ZKT_IS_HELD(ctx->tree))
          return ZEKE_CALLBACK_IGNORE;
      }
      return ZEKE_CALLBACK_DESTROY;
    }
    ERR("tree add from zeke_aget_children: %s",emsg);
    zeke_safe_shutdown(200);
    return ZEKE_CALLBACK_DESTROY;
  }
  zeke_callback_status_set(cbd,ZEOK);
  return ZEKE_CALLBACK_IGNORE;
}

static void remove_tree_cleanup(zeke_context_t *zctx, void *ignored)
{
  zeke_tree_context_t *ctx = (zeke_tree_context_t*)zctx;

  if(ctx) {
    ZEKE_MAGIC_CHECK(ctx, ZEKE_TREE_SESSION_MAGIC);
    if(ctx->tree && ctx->tree->pool) {
      ZEKE_MAGIC_CHECK(ctx->tree, ZEKE_TREE_MAGIC);
      apr_pool_cleanup_kill(ctx->tree->pool,&ctx->tree,zeke_indirect_wipe);
    }
  }
}

static zk_status_t tree_add(zeke_connection_t *c,
                            zeke_tree_t *t)
{
  zeke_tree_context_t *ctx;
  apr_pool_t *p = zeke_session_pool(c);
  ZEKE_MAGIC_CHECK(t,ZEKE_TREE_MAGIC);

  ctx = zeke_session_context_create(c,t->session_context_key,sizeof(*ctx));
  ZAN(ctx);
  ZAZ(ctx->pool);
  ctx->magic = ZEKE_TREE_SESSION_MAGIC;
  ctx->pool = p;
  ctx->tpool = NULL;
  ctx->tp = &t->timeout;
  ctx->path = apr_pstrdup(p,t->path);

  ctx->start_time = apr_time_now();
  ctx->end_time = NULL_TIME;
  ctx->num_nodes = 0;
  ctx->tree = t;
  ctx->pos = NULL;
  ZKTREE_INIT(&ctx->root);
  zeke_session_cleanup_register(c,t->session_context_key,NULL,remove_tree_cleanup);
  zeke_pool_cleanup_indirect(t->pool,ctx->tree);

  return zeke_aget_children(NULL,c,ctx->path,NULL,tree_get,ctx);
}

static void tree_post_connect(const zeke_watcher_cb_data_t *cbd)
{
  if(cbd->type == ZOO_SESSION_EVENT && cbd->state == ZOO_CONNECTED_STATE) {
    zk_status_t st;
    int i;
    ZEKE_MAY_ALIAS(apr_array_header_t) *pending = NULL;
    ZEKE_MAY_ALIAS(const zeke_connection_t) *conn = cbd->connection;
    apr_pool_t *p = cbd->connection->pool;
    if(apr_pool_userdata_get((void**)&pending,ZEKE_TREE_CONNECTION_KEY,p) != APR_SUCCESS)
      return;
    if(pending->nelts == 0)
      return;

    for(i = 0; i < pending->nelts; i++) {
      zeke_tree_t *t = APR_ARRAY_IDX(pending,i,zeke_tree_t*);
      ZEKE_MAGIC_CHECK(t, ZEKE_TREE_MAGIC);
      st = tree_add((zeke_connection_t*)conn,t);
      if(st != ZEOK)
        ERR("tree_add: %s",zeke_perrstr(st,cbd->pool));
    }
    apr_array_clear(pending);
  }
}

ZEKE_API(zk_status_t) zeke_tree_create(zeke_tree_t **tp,
                                       const char *root,
                                       zeke_tree_complete_fn completed,
                                       zeke_tree_timeout_fn timeout,
                                       zeke_context_t *ctx,
                                       const zeke_connection_t *conn)
{
  zk_status_t st = ZEOK;
  apr_pool_t *pool = NULL;
  zeke_indirect_t *ind;

  assert(apr_pool_create(&pool,conn->pool) == APR_SUCCESS);
  zeke_pool_tag(pool,apr_psprintf(pool,"zeke tree pool for '%s'",root));
  apr_pool_userdata_setn(conn,ZEKE_TREE_CONNECTION_REFKEY,NULL,pool);
  ZAN(tp);
  (*tp) = apr_palloc(pool,sizeof(**tp));
  ZAN(*tp);
  (*tp)->magic = ZEKE_TREE_MAGIC;
  (*tp)->pool = pool;
  (*tp)->path = apr_pstrdup(pool,root);
  (*tp)->session_context_key = apr_pstrcat(pool,ZEKE_TREE_SESSION_PREFIX,
                                           root,NULL);
  (*tp)->conn = conn;
  (*tp)->timeout_fn = timeout;
  (*tp)->complete_fn = completed;
  (*tp)->filter_fn = NULL;
  (*tp)->flags = 0;
  (*tp)->status = ZEOK;
  (*tp)->user_context = ctx;

  ind = zeke_indirect_make(&(*tp)->conn,pool);
  ZAN(ind);
  zeke_pool_cleanup_register(conn->pool,ind,zeke_indirect_deref_wipe);
  return st;
}

ZEKE_API(zk_status_t) zeke_tree_start(zeke_tree_t *t)
{
  zk_status_t st = ZEOK;
  apr_pool_t *pool;
  int defer;
  ZEKE_MAGIC_CHECK(t,ZEKE_TREE_MAGIC);
  if(ZKT_IS_HELD(t)) {
    zeke_tree_context_t *ctx;
    ZAN(t->session_context_key);
    ctx = zeke_session_context(t->conn,t->session_context_key);
    ZEKE_MAGIC_CHECK(ctx, ZEKE_TREE_SESSION_MAGIC);
    t->flags = 0;
    if(ctx->tpool != NULL)
      apr_pool_destroy(ctx->tpool);
    ZAZ(ctx->tpool);
  }
  if(ZKT_IS_STARTED(t) || ZKT_IS_FINISHED(t))
    return APR_EINVAL;

  pool = t->pool;
  ZAN(t->conn);
  ZAN(t->pool);
  defer = (!t->conn->zh || zoo_state(t->conn->zh) != ZOO_CONNECTED_STATE);

  if(!defer) {
    pool = zeke_session_pool(t->conn);
    if(pool == NULL) {
      defer++;
      pool = t->conn->pool;
    }
  }

  ZKTFLAG_SET_STARTED(t);

  if(defer) {
    ZEKE_MAY_ALIAS(apr_array_header_t) *pending = NULL;

    st = apr_pool_userdata_get((void**)&pending,ZEKE_TREE_CONNECTION_KEY,t->conn->pool);
    if(st != APR_SUCCESS || !pending) {
      pending = apr_array_make(pool,1,sizeof(zeke_tree_t*));
      ZAN(pending);
      apr_pool_userdata_setn(pending,ZEKE_TREE_CONNECTION_KEY,NULL,t->conn->pool);
      zeke_connection_watcher_add((zeke_connection_t*)t->conn,tree_post_connect);
      st = APR_SUCCESS;
    }
    APR_ARRAY_PUSH(pending,zeke_tree_t*) = t;
  } else
    st = tree_add((zeke_connection_t*)t->conn,t);

  if(!ZISOKAY(st))
    t->flags &= ~ZKTFLAG_STARTED;
  return st;
}

static
void iter_down(zeke_tree_iter_t *iter)
{
  assert(ZKTREE_SENTINEL(&iter->ctx->root) != iter->cur);
  ZEKE_MAGIC_CHECK(iter->cur, ZEKE_TREE_NODE_MAGIC);

  if(ZKTREE_ELEM_HAS_CHILDREN(iter->cur)) {
    iter->head = ZKTREE_ELEM_CHILDREN(iter->cur);
    iter->next = ZKTREE_FIRST(iter->head);
  } else {
    iter->next = ZKTREE_NEXT(iter->cur);
    while(iter->next && iter->next == ZKTREE_SENTINEL(iter->head)) {
      zeke_tree_node_t *parent;
      parent = ZKTREE_ELEM_PARENT(iter->cur);
      iter->head = ZKTREE_ELEM_PARENT_HEAD(parent,&iter->ctx->root);
      iter->next = (parent ? ZKTREE_NEXT(parent) : NULL);
      if(iter->next && iter->next == ZKTREE_SENTINEL(iter->head))
        iter->next = NULL;
    }
  }
}

static
void iter_up(zeke_tree_iter_t *iter)
{
  assert(ZKTREE_SENTINEL(&iter->ctx->root) != iter->cur);
  ZEKE_MAGIC_CHECK(iter->cur, ZEKE_TREE_NODE_MAGIC);
  if(iter->cur) {
    ZKTREE_NODE_SET(iter->cur,last,monotonic_seq());
  }
}

ZEKE_API(zeke_tree_iter_t*)
zeke_tree_iter_next(zeke_tree_iter_t *iter, void **userdatap)
{
  ZEKE_MAGIC_CHECK(iter, ZEKE_TREE_ITER_MAGIC);

  if(iter->start == NULL_TIME || !iter->ctx)
    goto zeke_tree_iter_exhausted;

  if(!iter->next || !iter->head || (ZKTREE_SENTINEL(iter->head) == iter->next))
    goto zeke_tree_iter_exhausted;

  ZEKE_MAGIC_CHECK(iter->ctx, ZEKE_TREE_SESSION_MAGIC);

  if(userdatap)
    *userdatap = iter->userdata;

  switch(iter->type) {
  case zeke_tree_iter_top:
    iter->cur = iter->next;
    ZAN(iter->cur);
    iter_down(iter);
    break;
  case zeke_tree_iter_bottom:
    iter->cur = iter->next;
    iter->next = tree_iter(&iter->head,iter->cur,&iter->ctx->root,&iter->state,
                           iter->pool,iter->start,NULL_TIME);
    if(iter->cur)
      iter_up(iter);
    break;
  default:
    abort();
  }

  return iter;

zeke_tree_iter_exhausted:
  iter->ctx = NULL;
  iter->head = NULL;
  iter->cur = NULL;
  iter->next = NULL;
  return NULL;
}

static apr_status_t cleanup_iterator(void *iterp)
{
  zeke_tree_iter_t *iter = zeke_indirect_consume((zeke_indirect_t*)iterp);

  if(iter) {
    ZEKE_MAGIC_CHECK(iter, ZEKE_TREE_ITER_MAGIC);

    iter->start = APR_TIME_C(0);
  }
  return APR_SUCCESS;
}

ZEKE_API(zeke_tree_iter_t*)
zeke_tree_iter_first(const zeke_tree_t *t, zeke_tree_iter_type_e type,
                     void *nonce, apr_pool_t *pool)
{
  zeke_tree_context_t *ctx;
  zeke_tree_iter_t *iter;
  apr_pool_t *p;


  ZEKE_MAGIC_CHECK(t, ZEKE_TREE_MAGIC);
  if(!zeke_tree_is_complete(t) || zeke_tree_is_error(t,NULL))
    return NULL;

  ctx = zeke_session_context(t->conn, t->session_context_key);
  ZEKE_MAGIC_CHECK(ctx, ZEKE_TREE_SESSION_MAGIC);

  assert(apr_pool_create(&p,(pool ? pool : ctx->pool)) == APR_SUCCESS);

  iter = apr_palloc(p,sizeof(*iter));
  iter->magic = ZEKE_TREE_ITER_MAGIC;
  iter->pool = p;
  iter->type = type;
  iter->ctx = ctx;
  iter->head = &ctx->root;
  iter->cur = NULL;
  iter->next = NULL;
  iter->state = NULL;
  iter->userdata = nonce;
  iter->start = monotonic_seq();

  apr_pool_pre_cleanup_register(ctx->pool,
                                zeke_indirect_make(iter,p),
                                cleanup_iterator);

  switch(type) {
  case zeke_tree_iter_top:
    if(!ZKTREE_EMPTY(iter->head)) {
      iter->cur = ZKTREE_FIRST(iter->head);
      iter_down(iter);
    } else {
      apr_pool_destroy(p);
      return NULL;
    }
    break;
  case zeke_tree_iter_bottom:
    if(!ZKTREE_EMPTY(iter->head)) {
      iter->cur = tree_iter(&iter->head,NULL,&ctx->root,&iter->state,
                            iter->pool,iter->start,NULL_TIME);
      if(iter->cur) {
        iter->next = tree_iter(&iter->head,iter->cur,&ctx->root,&iter->state,
                               iter->pool,iter->start,NULL_TIME);
        iter_up(iter);
      }
    } else {
      apr_pool_destroy(p);
      return NULL;
    }
    break;
  default:
    abort();
  }

  return iter;
}

ZEKE_API(zeke_tree_node_t*)
zeke_tree_iter_this(zeke_tree_iter_t *iter, void **userdatap)
{

  ZEKE_MAGIC_CHECK(iter, ZEKE_TREE_ITER_MAGIC);

  if(iter->start == NULL_TIME || !iter->ctx)
    return NULL;

  ZEKE_MAGIC_CHECK(iter->ctx, ZEKE_TREE_SESSION_MAGIC);

  if(userdatap)
    *userdatap = iter->userdata;

  return iter->cur;
}

ZEKE_API(void)
zeke_tree_iter_destroy(zeke_tree_iter_t *iter)
{
  apr_pool_t *p;
  if(!iter)
    return;

  ZEKE_MAGIC_CHECK(iter, ZEKE_TREE_ITER_MAGIC);

  p = iter->pool;
  if(p) {
    iter->start = NULL_TIME;
    iter->ctx = NULL;
    iter->pool = NULL;
    apr_pool_destroy(p);
  }
}
