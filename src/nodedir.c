#include "internal.h"
#include "connection.h"
#include "dispatch.h"

#include "libzeke_nodedir.h"

typedef int (*zeke_nodedir_compr_fn)(const char*name1,const char *name2,zeke_context_t *ctx);

typedef struct {
  apr_pool_t *pool;
  zeke_nodedir_t *nodedir;
  apr_time_t start_time;
  apr_time_t end_time;
  apr_interval_time_t timeout;
  zeke_nodedir_compr_fn compare;
  apr_table_t *table;
  const char *path;
  int nchildren,ncomplete;
} zeke_nodedir_context_t;

struct zeke_nodedir_t {
  apr_pool_t *pool;
  zk_status_t status;
  const char *session_context_key;
  zeke_context_t *user_context;
  apr_interval_time_t timeout;
  zeke_nodedir_complete_fn completed;
  zeke_nodedir_timeout_fn timedout;
  int finished;
  const char *root;
};

typedef struct {
  const char *path;
  const char *name;
  const char *session_context_key;
} zeke_nodedir_fetch_t;

typedef struct {
  apr_pool_t *pool;
  apr_hash_t *hash;
  const char *null_value;
} zeke_nodedir_hash_t;

ZEKE_POOL_IMPLEMENT_ACCESSOR(nodedir)

static const char *zeke_nodedir_value_null = "(null)";
static const char *zeke_nodedir_value_pending = "PENDING (should never see this)";

#define ZEKE_NODEDIR_VALUE_NULL zeke_nodedir_value_null
#define ZEKE_NODEDIR_VALUE_PENDING zeke_nodedir_value_pending
#define ZEKE_NODEDIR_SESSION_PREFIX "zekeNodeDirSession:"
#define ZEKE_NODEDIR_CONNECTION_KEY "zeke-nodedirs"
#define ZEKE_NODEDIR_CONNECTION_REFKEY "zeke-nodedir-conn-ref"

/* This might be made library-private at some point */
static const zeke_connection_t *zeke_nodedir_connection(const zeke_nodedir_t *nodedir)
{
  ZEKE_MAY_ALIAS(const zeke_connection_t) *conn = NULL;

  zk_status_t st = apr_pool_userdata_get((void**)&conn,ZEKE_NODEDIR_CONNECTION_REFKEY,nodedir->pool);

  if(!ZEKE_STATUS_IS_OKAY(st))
    zeke_fatal("no connection found for nodedir object",st);

  ZEKE_ASSERT(conn != NULL,"no connection found for nodedir object");
  return conn;
}

static int populate_hash(void *vh, const char *key, const char *value)
{
  ZEKE_MAY_ALIAS(zeke_nodedir_hash_t) *nh = (zeke_nodedir_hash_t*)vh;

  if(value == ZEKE_NODEDIR_VALUE_NULL)
    apr_hash_set(nh->hash,apr_pstrdup(nh->pool,key),APR_HASH_KEY_STRING,nh->null_value);
  else
    apr_hash_set(nh->hash,apr_pstrdup(nh->pool,key),APR_HASH_KEY_STRING,apr_pstrdup(nh->pool,value));
  return 1;
}

ZEKE_API(apr_hash_t*) zeke_nodedir_hash_make(const zeke_nodedir_t *nodedir, 
                                             const char *null_value,
                                             apr_pool_t *pool)
{
  const zeke_connection_t *conn = zeke_nodedir_connection(nodedir);
  zeke_nodedir_context_t *ctx = zeke_session_context(conn,nodedir->session_context_key);
  apr_hash_t *h = NULL;

  assert(null_value != NULL);

  if(nodedir->finished < 1) {
    ERR("cannot create a hash table for incomplete results of unfinished nodedir: %s",
        nodedir->root);
  } else {
    zeke_nodedir_hash_t nh;
    nh.pool = pool;
    nh.hash = h = apr_hash_make(pool);
    nh.null_value = null_value;
    assert(h != NULL);
    assert(apr_table_do(populate_hash,&nh,ctx->table,NULL) != 0);
  }

  return h;
}

ZEKE_API(zeke_context_t*) zeke_nodedir_context(const zeke_nodedir_t *nodedir)
{
  return nodedir->user_context;
}

ZEKE_API(void) zeke_nodedir_context_set(zeke_nodedir_t *nodedir, zeke_context_t *context)
{
  nodedir->user_context = context;
}

ZEKE_API(int) zeke_nodedir_node_is_null(const apr_table_t *table,
                                        const char *name)
{
  const char *val = apr_table_get(table,name);
  if(!val || val == ZEKE_NODEDIR_VALUE_NULL)
    return 1;
  return 0;
}

ZEKE_API(apr_interval_time_t) zeke_nodedir_request_time_get(const zeke_nodedir_t *nodedir)
{
  const zeke_connection_t *conn = zeke_nodedir_connection(nodedir);
  zeke_nodedir_context_t *ctx = zeke_session_context(conn,
                                                     nodedir->session_context_key);
  if(ctx == NULL)
    return -1;

  if(!ctx->end_time)
    return apr_time_now() - ctx->start_time;
  return ctx->end_time - ctx->start_time;
}

ZEKE_API(zk_status_t) zeke_nodedir_cancel(const zeke_nodedir_t *nodedir)
{
  const zeke_connection_t *conn = zeke_nodedir_connection(nodedir);
  zk_status_t st = ZEOK;
  zeke_nodedir_context_t *ctx = zeke_session_context(conn,
                                                     nodedir->session_context_key);
  if(ctx && ctx->nodedir)
    ctx->nodedir = NULL;
  else
    st = APR_ENOENT;
  return st;
}

ZEKE_API(zk_status_t) zeke_nodedir_status(const zeke_nodedir_t *nodedir)
{
  assert(nodedir != NULL);
  return nodedir->status;
}

ZEKE_EXPORT int zeke_nodedir_is_complete(const zeke_nodedir_t *nodedir)
{
  assert(nodedir != NULL);
  return nodedir->finished > 0;
}

static int nodedir_complete(zeke_nodedir_context_t *ctx)
{
  zeke_nodedir_complete_fn completed = ctx->nodedir->completed;
  zeke_nodedir_timeout_fn timedout = ctx->nodedir->timedout;
  zk_status_t st = ctx->nodedir->status;
  int valid = 0;
  
  if(!ctx->end_time)
    ctx->end_time = apr_time_now();
  ctx->nodedir->finished = 1;
  ctx->nodedir->status = ZEOK;
  ctx->nodedir->completed = NULL;
  ctx->nodedir->timedout = NULL;
  
  if(ctx->table)
    ctx->table = zeke_table_sort(ctx->table,NULL,ctx->pool,NULL);

  if(completed) {
    valid = completed(ctx->nodedir,ctx->table,(void*)ctx->nodedir->user_context);
    if(valid) {
      ctx->nodedir->status = st;
      ctx->nodedir->timedout = timedout;

      if(ctx->nodedir->finished < 1)
        ctx->nodedir->completed = completed;
    }
  }

  return valid;
}

static zeke_cb_t nodedir_fill(const zeke_callback_data_t *cbd)
{
  zeke_nodedir_fetch_t *fetch = (zeke_nodedir_fetch_t*)cbd->ctx;
  zeke_nodedir_context_t *ctx = zeke_session_context(cbd->connection,
                                                     fetch->session_context_key);

  if(!ctx || !ctx->nodedir)
    return ZEKE_CALLBACK_DESTROY;

  if(cbd->status != ZEOK) {
    apr_table_unset(ctx->table,fetch->name);
    zeke_callback_status_set(cbd,ZEOK);
  } else {
    if(cbd->value == NULL || cbd->value_len == -1)
      apr_table_setn(ctx->table,fetch->name,ZEKE_NODEDIR_VALUE_NULL);
    else {
      char *value;
      apr_size_t value_len = cbd->value_len;
      value = apr_palloc(ctx->pool,value_len+1);
      assert(value != NULL);
      memcpy(value,cbd->value,value_len);
      *(value+value_len) = '\0';
      apr_table_setn(ctx->table,fetch->name,value);
    }
  }

  if(ctx->timeout != -1) {
    apr_interval_time_t elapsed = apr_time_now() - ctx->start_time;
    if(elapsed > ctx->timeout) {
      ctx->nodedir->status = APR_TIMEUP;
      if(ctx->nodedir->timedout) {
        int finished = ctx->nodedir->finished;
        ctx->nodedir->finished = 1;
        ctx->nodedir->timedout(ctx->nodedir,elapsed,ctx->nodedir->user_context);
        if(ctx->nodedir->finished < 1) {
          ctx->nodedir->finished = 0;
          return ZEKE_CALLBACK_IGNORE;
        }
        ctx->nodedir->finished = finished;
        ctx->nodedir->timedout = NULL;
      }
      ctx->timeout = -1;
      ctx->nodedir->completed = NULL;
    }
  }
  ctx->ncomplete++;
  if(ctx->ncomplete >= ctx->nchildren) {
    if(nodedir_complete(ctx) && ctx->nodedir->finished < 1) {
      ctx->nodedir->finished = 0;
      return ZEKE_CALLBACK_IGNORE;
    }
    zeke_session_context_release(cbd->connection,fetch->session_context_key);
    return ZEKE_CALLBACK_DESTROY;
  }

  return ZEKE_CALLBACK_IGNORE;
}

static zeke_cb_t nodedir_start(const zeke_callback_data_t *cbd)
{
  int i;
  const zeke_connection_t *conn = cbd->connection;
  zeke_nodedir_context_t *ctx = (zeke_nodedir_context_t*)cbd->ctx;
  apr_table_t *table = ctx->table;

  if(cbd->status != ZEOK) {
    if(ctx->nodedir) {
      ctx->nodedir->status = cbd->status;
      ctx->nodedir->finished = 1;
      if(ctx->nodedir->completed &&
         ctx->nodedir->completed(ctx->nodedir,NULL,(void*)ctx->nodedir->user_context)) {
        if(ctx->nodedir && ctx->nodedir->finished < 1)
          return ZEKE_CALLBACK_IGNORE;
      }
    }
    zeke_callback_status_set(cbd,ZEOK);
    return ZEKE_CALLBACK_DESTROY;
  }

  if(!ctx->nodedir)
    return ZEKE_CALLBACK_DESTROY;

  ctx->nchildren = cbd->vectors.strings->count;
  if(ctx->nchildren == 0) {
    if(nodedir_complete(ctx) && ctx->nodedir && ctx->nodedir->finished < 1)
      return ZEKE_CALLBACK_IGNORE;
    return ZEKE_CALLBACK_DESTROY;
  }
  for(i = 0; i < ctx->nchildren; i++) {
    const char *node = cbd->vectors.strings->data[i];
    char *path = NULL;
    zeke_nodedir_fetch_t *fetch = apr_palloc(conn->pool,sizeof(zeke_nodedir_fetch_t));

    assert(fetch != NULL);
    assert(apr_filepath_merge(&path,ctx->path,node,0,ctx->pool) == APR_SUCCESS);
    assert(path != NULL);
    fetch->path = path;
    fetch->name = apr_pstrdup(ctx->pool,node);
    fetch->session_context_key = apr_pstrdup(conn->pool,
                    ctx->nodedir->session_context_key);
    apr_table_setn(table,fetch->name,ZEKE_NODEDIR_VALUE_PENDING);
    zeke_aget(NULL,cbd->connection,path,NULL,nodedir_fill,fetch);
  }

  return ZEKE_CALLBACK_IGNORE;
}

static void remove_nodedir_cleanup(zeke_context_t *zctx, void *ignored)
{
  zeke_nodedir_context_t *ctx = (zeke_nodedir_context_t*)zctx;

  if(ctx && ctx->nodedir && ctx->nodedir->pool)
    apr_pool_cleanup_kill(ctx->nodedir->pool,&ctx->nodedir,zk_indirect_wipe);
}

static zk_status_t nodedir_restart(const zeke_connection_t *conn,
                                   zeke_nodedir_t *nodedir)
{
  zeke_nodedir_context_t *ctx = zeke_session_context(conn,nodedir->session_context_key);

  ctx->timeout = nodedir->timeout;
  ctx->start_time = apr_time_now();
  ctx->end_time = APR_TIME_C(0);
  if(ctx->table)
    apr_table_clear(ctx->table);
  ctx->nchildren = -1;
  ctx->ncomplete = 0;
  ctx->nodedir->finished = -1;
  nodedir->status = ZEOK;

  return zeke_aget_children(NULL,conn,ctx->path,NULL,nodedir_start,ctx);
}

static zk_status_t nodedir_add(const zeke_connection_t *conn,
                               zeke_nodedir_t *nodedir)
{
  zeke_nodedir_context_t *ctx = zeke_session_context_create(conn,
                                                            nodedir->session_context_key,
                                                            sizeof(zeke_nodedir_context_t));
  apr_pool_t *pool = zeke_session_pool(conn);
  assert(ctx && ctx->pool == NULL);
  ctx->pool = pool;
  ctx->timeout= nodedir->timeout;
  if(strcmp(nodedir->root,"/") == 0 || !*nodedir->root)
    ctx->path = apr_pstrcat(pool,"/",nodedir->root,NULL);
  else
    ctx->path = apr_pstrdup(pool,nodedir->root);

  ctx->start_time = apr_time_now();
  ctx->table = apr_table_make(pool,5);
  assert(ctx->table != NULL);
  ctx->nchildren = -1;
  ctx->nodedir = nodedir;
  zeke_session_cleanup_register(conn,nodedir->session_context_key,NULL,remove_nodedir_cleanup);
  apr_pool_cleanup_register(nodedir->pool,&ctx->nodedir,zk_indirect_wipe,apr_pool_cleanup_null);

  return zeke_aget_children(NULL,conn,ctx->path,NULL,nodedir_start,ctx);
}

static void nodedir_post_connect(const zeke_watcher_cb_data_t *cbd)
{
  if(cbd->type == ZOO_SESSION_EVENT && cbd->state == ZOO_CONNECTED_STATE) {
    zk_status_t st;
    int i;
    ZEKE_MAY_ALIAS(apr_array_header_t) *pending = NULL;
    ZEKE_MAY_ALIAS(const zeke_connection_t) *conn = cbd->connection;

    if(apr_pool_userdata_get((void**)&pending,ZEKE_NODEDIR_CONNECTION_KEY,conn->pool) != APR_SUCCESS)
      return;
    if(pending->nelts == 0)
      return;

    for(i = 0; i < pending->nelts; i++) {
      st = nodedir_add(conn,APR_ARRAY_IDX(pending,i,zeke_nodedir_t*));
      if(st != ZEOK)
        ERR("nodedir_add: %s",zeke_perrstr(st, cbd->pool));
    }
    apr_array_clear(pending);
  }
}

ZEKE_API(zk_status_t) zeke_nodedir_restart(const zeke_nodedir_t *nodedir)
{
  const zeke_connection_t *conn = zeke_nodedir_connection(nodedir);

  if(nodedir->finished <= 0) {
    ERR("cannot restart nodedir for %s: restarting only possible from completion or timeout handler",
        nodedir->root);
    return APR_EINVAL;
  }
  return nodedir_restart(conn,(zeke_nodedir_t*)nodedir);
}

ZEKE_API(zk_status_t) zeke_nodedir_destroy(zeke_nodedir_t *nodedir)
{
  apr_pool_t *pool = nodedir->pool;
  const zeke_connection_t *conn = zeke_nodedir_connection(nodedir);

  if(nodedir->finished < 1) {
    ERR("cannot destroy a running nodedir (%s)",nodedir->root);
    return APR_EINVAL;
  }
  
  if(conn)
    zeke_session_context_release(conn,nodedir->session_context_key);
  
  nodedir->pool = NULL;
  if(pool != NULL)
    apr_pool_destroy(pool);

  return ZEOK;
}

ZEKE_API(zk_status_t) zeke_nodedir_create(zeke_nodedir_t **nodedir,
                                          const char *root,
                                          apr_interval_time_t timeout,
                                          zeke_nodedir_complete_fn completed,
                                          zeke_nodedir_timeout_fn timedout,
                                          zeke_context_t *context,
                                          const zeke_connection_t *conn)
{
  zk_status_t st = ZEOK;
  apr_pool_t *pool = NULL;
  int defer = (!conn->zh || zoo_state(conn->zh) != ZOO_CONNECTED_STATE);

  assert(apr_pool_create(&pool,conn->pool) == APR_SUCCESS);
  zeke_pool_tag(pool,apr_psprintf(pool,"nodedir pool for '%s'",root));
  apr_pool_userdata_setn(conn,ZEKE_NODEDIR_CONNECTION_REFKEY,NULL,pool);
  assert(nodedir != NULL);
  *nodedir = apr_palloc(pool,sizeof(zeke_nodedir_t));
  assert(*nodedir != NULL);
  (*nodedir)->pool = pool;
  (*nodedir)->root = apr_pstrdup(pool,root);
  (*nodedir)->session_context_key = apr_pstrcat(pool,
                                                 ZEKE_NODEDIR_SESSION_PREFIX,
                                                root,NULL);
  (*nodedir)->timeout = timeout;
  (*nodedir)->completed = completed;
  (*nodedir)->timedout = timedout;
  (*nodedir)->finished = 0;
  (*nodedir)->status = ZEOK;
  (*nodedir)->user_context = context;

  if(!defer) {
    pool = zeke_session_pool(conn);
    if(pool == NULL) {
      defer++;
      pool = conn->pool;
    }
  }

  if(defer) {
    ZEKE_MAY_ALIAS(apr_array_header_t) *pending = NULL;

    st = apr_pool_userdata_get((void**)&pending,ZEKE_NODEDIR_CONNECTION_KEY,conn->pool);
    if(st != APR_SUCCESS || !pending) {
      pending = apr_array_make(pool,1,sizeof(zeke_nodedir_t*));
      apr_pool_userdata_setn(pending,ZEKE_NODEDIR_CONNECTION_KEY,NULL,conn->pool);
      zeke_connection_watcher_add((zeke_connection_t*)conn,nodedir_post_connect);
      st = APR_SUCCESS;
    }
    APR_ARRAY_PUSH(pending,zeke_nodedir_t*) = *nodedir;
  } else
    st = nodedir_add(conn,*nodedir);

  return st;
}
