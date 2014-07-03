#include "internal.h"
#include "dispatch.h"
#include "connection.h"
#include "io.h"

#define ZEKE_DISPATCH_POOL_CLEANUP_KEY "zeke_dispatch_pool_cleanup_key"

struct zeke_private {
  const char *op;
  apr_int16_t self_owned_pool;
  zk_status_t status;
  int rc;
  zhandle_t *zh;
  zeke_callback_fn callback;
  zeke_watcher_fn watcher;
  zeke_watcher_cb_data_t *watcher_ctx;
  const zeke_context_t *watcher_user_ctx;
  /* for holding multis */
  zoo_op_result_t *multi_results;
};

static apr_pool_t *dispatch_pool = NULL;

static apr_status_t cleanup_dispatch_pool(void *v)
{
  apr_pool_t **p = (apr_pool_t**)v;
  *p = NULL;
  return APR_SUCCESS;
}

static apr_status_t cleanup_callback_data(void *data)
{
  zeke_callback_data_t *cbd = (zeke_callback_data_t*)data;

  LOG("cleanup_callback_data() fired for %lx",(unsigned long)cbd->pool);;
  if(cbd->connection) {
    apr_pool_t *pool = cbd->connection->pool;
    apr_pool_cleanup_kill(pool,&cbd->connection,zk_indirect_wipe);
    apr_pool_cleanup_kill(pool,&cbd->priv->zh,zk_indirect_wipe);
    cbd->priv->zh = NULL;
    cbd->connection = NULL;
  } else {
    ERR(" --> cleanup_callback_data() -> no connection to cleanup");
  }

  return APR_SUCCESS;
}
  
static apr_status_t cleanup_watcher_cb_data(void *data)
{
  zeke_watcher_cb_data_t *cbd = (zeke_watcher_cb_data_t*)data;

  LOG("cleanup_watcher_cb_data() fired for %lx",(unsigned long)cbd->pool);
  if(cbd->connection) {
    apr_pool_t *pool = cbd->connection->pool;
    apr_pool_cleanup_kill(pool,&cbd->connection,zk_indirect_wipe);
    apr_pool_cleanup_kill(pool,&cbd->priv->zh,zk_indirect_wipe);
    cbd->priv->zh = NULL;
    cbd->connection = NULL;
  }

  return APR_SUCCESS;
}

static apr_status_t child_cleanup_dispatch_pool(void *v)
{
  apr_pool_t **p = (apr_pool_t**)v;

  if(*p) {
    apr_pool_cleanup_kill(*p,v,cleanup_dispatch_pool);
    apr_pool_destroy(*p);
    *p = NULL;
  }

  return APR_SUCCESS;
}

ZEKE_PRIVATE(void) zeke_dispatch_init(apr_pool_t *pool) {
  apr_status_t st;

  if(!dispatch_pool) {
    if((st = apr_pool_create(&dispatch_pool,pool)) != APR_SUCCESS)
      zeke_fatal("apr_pool_create",st);
    zeke_pool_tag(dispatch_pool,"ZEKE Global Dispatch Pool");
    apr_pool_cleanup_register(dispatch_pool,&dispatch_pool,
                              &cleanup_dispatch_pool,
                              &child_cleanup_dispatch_pool);
  }
}

ZEKE_PRIVATE(void) zeke_dispatch_release(int destroy)
{
  apr_pool_cleanup_kill(dispatch_pool,&dispatch_pool,cleanup_dispatch_pool);
  if(destroy) {
    apr_pool_destroy(dispatch_pool);
    dispatch_pool = NULL;
  } else {
    apr_pool_clear(dispatch_pool);
    apr_pool_cleanup_register(dispatch_pool,&dispatch_pool,
                              &cleanup_dispatch_pool,
                              &apr_pool_cleanup_null);
  }
}

ZEKE_PRIVATE(apr_pool_t*) zeke_dispatch_subpool_create(void)
{
  apr_pool_t *pool = NULL;
  apr_status_t st;

  if(!dispatch_pool)
    zeke_dispatch_init(NULL);

  if((st = apr_pool_create(&pool,dispatch_pool)) != APR_SUCCESS)
    zeke_fatal("apr_pool_create",st);
  zeke_pool_tag(pool,"Dispatch Subpool");
  return pool;
}

/****************************************************************/
/* Callback data descriptor                                     */
/****************************************************************/
ZEKE_API(zeke_callback_data_t*) zeke_callback_data_create_ex(const zeke_connection_t *conn, 
                                                          zeke_watcher_fn watcher,
                                                          apr_pool_t *pool,
                                                          int create_pool)
{
  zeke_callback_data_t *cbd;
  apr_int16_t self_owned_pool = 0;
  zk_status_t rc = APR_SUCCESS;

  if(create_pool) {
    apr_pool_t *new_pool = NULL;
    if((rc = apr_pool_create(&new_pool,pool ? pool : dispatch_pool)) != APR_SUCCESS)
      zeke_fatal("apr_pool_create",rc);
    pool = new_pool;
    zeke_pool_tag(pool,"dispatch pool");
    self_owned_pool++;
  }

  cbd = apr_pcalloc(pool,sizeof(zeke_callback_data_t));
  assert(cbd != NULL);
  cbd->pool = pool;
  cbd->priv = apr_pcalloc(pool,sizeof(struct zeke_private));
  cbd->priv->zh = conn->zh;
  cbd->priv->self_owned_pool = self_owned_pool;
  cbd->priv->watcher = watcher;
  cbd->priv->status = APR_SUCCESS;
  cbd->priv->multi_results = NULL;
  cbd->connection = conn;
  apr_pool_cleanup_register(pool,cbd,cleanup_callback_data,apr_pool_cleanup_null);
  if(conn->pool != pool) {
    apr_pool_cleanup_register(conn->pool,&cbd->connection,
                              zk_indirect_wipe,apr_pool_cleanup_null);
    apr_pool_cleanup_register(conn->pool,&cbd->priv->zh,
                              zk_indirect_wipe,apr_pool_cleanup_null);
  }
  return cbd;
}

zeke_callback_data_t *zeke_callback_data_create(const zeke_connection_t *conn,
                                                zeke_watcher_fn watcher,
                                                apr_pool_t *pool)
{
  return zeke_callback_data_create_ex(conn,watcher,pool,(pool == NULL));
}

zk_status_t zeke_callback_data_destroy(zeke_callback_data_t *cbd)
{
  zk_status_t st = cbd->priv->status == APR_SUCCESS ? cbd->status : APR_SUCCESS;

  assert(cbd->priv != NULL);
  if(cbd->priv->self_owned_pool) {
    apr_pool_t *pool = cbd->pool;
    if(pool) {
      apr_pool_destroy(pool);
      return st;
    }
  } else
    apr_pool_cleanup_run(cbd->pool,cbd,cleanup_callback_data);

  return st;
}

/****************************************************************/
/* Watcher callback data descriptor                             */
/****************************************************************/
ZEKE_PRIVATE(zeke_watcher_cb_data_t) *zeke_watcher_cb_data_create_ex(const zeke_connection_t *conn,
                                                                     const zeke_context_t *ctx,
                                                                     apr_pool_t *pool,
                                                                     apr_pool_t *parent_pool)
{
  /* always create a new pool from the root, as these have no determinate
     lifespan.
   */
  zeke_watcher_cb_data_t *cbd;
  zk_status_t rc = APR_SUCCESS;
  unsigned self_owned_pool = 1;

  pool =parent_pool = NULL;
  if(pool == NULL) {
    if((rc = apr_pool_create(&pool,parent_pool ? parent_pool : dispatch_pool)) != APR_SUCCESS)
      zeke_fatal("apr_pool_create",rc);
    self_owned_pool++;
  }

  cbd = apr_pcalloc(pool,sizeof(zeke_watcher_cb_data_t));
  assert(cbd != NULL);
  cbd->pool = pool;
  cbd->priv = apr_pcalloc(pool,sizeof(struct zeke_private));
  cbd->priv->self_owned_pool = self_owned_pool;
  cbd->priv->zh = conn->zh;
  cbd->priv->watcher_user_ctx = ctx;
  cbd->ctx = ctx;
  cbd->connection = conn;

  apr_pool_cleanup_register(pool,cbd,cleanup_watcher_cb_data,
                            apr_pool_cleanup_null);
  apr_pool_cleanup_register(conn->pool,&cbd->connection,zk_indirect_wipe,
                            apr_pool_cleanup_null);
  apr_pool_cleanup_register(conn->pool,&cbd->priv->zh,zk_indirect_wipe,
                            apr_pool_cleanup_null);
  return cbd;
}

ZEKE_PRIVATE(zeke_watcher_cb_data_t) *zeke_watcher_cb_data_create(const zeke_connection_t *conn,
                                                           const zeke_context_t *ctx)
{
  return zeke_watcher_cb_data_create_ex(conn,ctx,NULL,NULL);
}

ZEKE_API(void) zeke_set_watcher_context(zeke_callback_data_t *cbd,
                                               const zeke_context_t *ctx)
{
  cbd->priv->watcher_user_ctx = ctx;
  if(cbd->priv->watcher && cbd->priv->watcher_ctx)
    cbd->priv->watcher_ctx->ctx = ctx;
}

ZEKE_API(void) zeke_callback_separate_watcher(zeke_callback_data_t *cbd, const zeke_context_t *ctx)
{
  zeke_set_watcher_context(cbd,ctx);
}
#if 0
  assert(cbd != NULL);
  assert(cbd->priv != NULL);
  assert(cbd->connection != NULL);
  assert(cbd->priv->watcher_ctx == NULL);
  cbd->priv->watcher_ctx = zeke_watcher_cb_data_create(cbd->connection, ctx);
  assert(cbd->priv->watcher_ctx != NULL);
}
#endif

ZEKE_PRIVATE(void) zeke_watcher_cb_data_context_set(zeke_watcher_cb_data_t *cbd,
                                                    const zeke_context_t *ctx)
{
  cbd->priv->watcher_user_ctx = ctx;
  cbd->ctx = ctx;
}

ZEKE_PRIVATE(zk_status_t) zeke_watcher_cb_data_destroy(zeke_watcher_cb_data_t *cbd)
{
  zk_status_t st = cbd->priv->status;
  
  LOG("running watcher callback data cleanup handler for %lx",
        (unsigned long)cbd->pool);
  apr_pool_cleanup_run(cbd->pool,cbd,cleanup_watcher_cb_data);

  if(cbd->priv->self_owned_pool) {
    apr_pool_t *pool = cbd->pool;
    if(pool) {
      cbd->pool = NULL;
      apr_pool_destroy(pool);
    }
  }

  return st;
}

/* Generic watcher dispatcher */
static void dispatch_watcher(zhandle_t *zh, int type,
                             int state, const char *path,
                             void *watcherCtx)
{
  zeke_watcher_cb_data_t *cbd = (zeke_watcher_cb_data_t*)watcherCtx;
  apr_pool_t *pool = cbd->pool;

#if 0
  if(cbd->connection)
    pool = zeke_session_pool(cbd->connection);
  if(!pool)
    pool = cbd->pool;
#endif

  if(cbd->priv->watcher) {
    cbd->type = type;
    cbd->state = state;
    cbd->path = path ? apr_pstrdup(pool,path) : NULL;
    assert(cbd->priv->zh == zh);
    cbd->ctx = cbd->priv->watcher_user_ctx;
    cbd->priv->watcher(cbd);
  }
  if(cbd->pool && cbd->priv->self_owned_pool) {
    apr_status_t st = zeke_watcher_cb_data_destroy(cbd);
    if(st != APR_SUCCESS)
      zeke_apr_error("during dispatch of watcher",st);
  }
}

static void dispatch_aget_children(int rc, const struct String_vector *strings,
                                   const struct Stat *stat,
                                   const void *data)
{
  zeke_callback_data_t *cbd = (zeke_callback_data_t*)data;
  zk_status_t st = APR_FROM_ZK_ERROR(rc);

  assert(cbd != NULL);
  cbd->vectors.strings = strings;
  cbd->stat = stat;
  cbd->status = st;
  cbd->priv->rc = rc;
  if(!cbd->priv->callback || cbd->priv->callback(cbd) == ZEKE_CALLBACK_DESTROY) {
    st = zeke_callback_data_destroy(cbd);
#ifdef DEBUGGING
    if(st != APR_SUCCESS)
      zeke_apr_error("awget_children2()",st);
#endif
    return;
  }

#ifdef DEBUGGING
  if(cbd->status != APR_SUCCESS)
    zeke_apr_error("awget_children2",cbd->status);
#endif
}

static void dispatch_stat_value(int rc, const char *value, int value_len,
                                const struct Stat *stat,
                                const void *data)
{
  char op[64] = "??";
  zeke_callback_data_t *cbd = (zeke_callback_data_t*)data;
  zk_status_t st = APR_FROM_ZK_ERROR(rc);
  
  assert(cbd != NULL);
  cbd->value_len = value_len;
  cbd->value = value; 
  cbd->stat = stat;
  cbd->status = st;
  cbd->priv->rc = rc;
  if(cbd->priv->op) {
    strncpy(op,cbd->priv->op,sizeof(op));
    op[sizeof(op)-1] = '\0';
  }
  if(!cbd->priv->callback || cbd->priv->callback(cbd) == ZEKE_CALLBACK_DESTROY) {
    st = zeke_callback_data_destroy(cbd);
#ifdef DEBUGGING
    if(st != APR_SUCCESS)
      zeke_apr_error(op,st);
#endif
    return;
  }

#ifdef DEBUGGING
  if(cbd->status != APR_SUCCESS)
    zeke_apr_error(op,cbd->status);
#endif
}

static void dispatch_const_char(int rc, const char *value, const void *data)
{
  char op[64] = "??";
  zeke_callback_data_t *cbd = (zeke_callback_data_t*)data;
  zk_status_t st = APR_FROM_ZK_ERROR(rc);

  assert(cbd != NULL);
  cbd->value = value;
  cbd->value_len = cbd->value ? strlen(value) : -1;
  cbd->status = st;
  cbd->stat = NULL;
  cbd->priv->rc = rc;
  if(cbd->priv->op) {
    strncpy(op,cbd->priv->op,sizeof(op));
    op[sizeof(op)-1] = '\0';
  }
  if(!cbd->priv->callback || cbd->priv->callback(cbd) == ZEKE_CALLBACK_DESTROY) {

    st = zeke_callback_data_destroy(cbd);
#ifdef DEBUGGING
    if(st != APR_SUCCESS)
      zeke_apr_error(op,st);
#endif
    return;
  }

#ifdef DEBUGGING
  if(cbd->status != APR_SUCCESS)
    zeke_apr_error(op,cbd->status);
#endif
}

static void dispatch_stat_void(int rc, const struct Stat *stat, const void *data)
{
  char op[64] = "??";
  zeke_callback_data_t *cbd = (zeke_callback_data_t*)data;
  zk_status_t st = APR_FROM_ZK_ERROR(rc);

  assert(cbd != NULL);
  cbd->value = NULL;
  cbd->value_len = -1;
  cbd->status = st;
  cbd->stat = stat;
  cbd->priv->rc = rc;
  if(cbd->priv->op) {
    strncpy(op,cbd->priv->op,sizeof(op));
    op[sizeof(op)-1] = '\0';
  }
  if(!cbd->priv->callback || cbd->priv->callback(cbd) == ZEKE_CALLBACK_DESTROY) {
    st = zeke_callback_data_destroy(cbd);
#ifdef DEBUGGING
    if(st != APR_SUCCESS)
      zeke_apr_error(op,st);
#endif
    return;
  }

#ifdef DEBUGGING
  if(cbd->status != APR_SUCCESS)
    zeke_apr_error(op,cbd->status);
#endif
}

static void dispatch_get_acl(int rc, struct ACL_vector *acl,
                             struct Stat *stat, const void *data)
{
  char op[64] = "??";
  zeke_callback_data_t *cbd = (zeke_callback_data_t*)data;
  zk_status_t st = APR_FROM_ZK_ERROR(rc);

  assert(cbd != NULL);
  cbd->value = NULL;
  cbd->value_len = -1;
  cbd->status = st;
  cbd->stat = stat;
  cbd->vectors.acl = acl;
  if(cbd->priv->op) {
    strncpy(op,cbd->priv->op,sizeof(op));
    op[sizeof(op)-1] = '\0';
  }
  
  if(!cbd->priv->callback || cbd->priv->callback(cbd) == ZEKE_CALLBACK_DESTROY) {
    st = zeke_callback_data_destroy(cbd);
#ifdef DEBUGGING
    if(st != APR_SUCCESS)
      zeke_apr_error(op,st);
#endif
    return;
  }

#ifdef DEBUGGING
  if(cbd->status != APR_SUCCESS)
    zeke_apr_error(op,cbd->status);
#endif
}

static void dispatch_void(int rc, const void *data)
{
  char op[64] = "??";
  zeke_callback_data_t *cbd = (zeke_callback_data_t*)data;
  zk_status_t st = APR_FROM_ZK_ERROR(rc);

  assert(cbd != NULL);
  cbd->value = NULL;
  cbd->value_len = 0;
  cbd->status = st;
  cbd->stat = NULL;
  cbd->priv->rc = rc;
  if(cbd->priv->op) {
    strncpy(op,cbd->priv->op,sizeof(op));
    op[sizeof(op)-1] = '\0';
  }
  if(!cbd->priv->callback || cbd->priv->callback(cbd) == ZEKE_CALLBACK_DESTROY) {
    st = zeke_callback_data_destroy(cbd);
#ifdef DEBUGGING
    if(st != APR_SUCCESS)
      zeke_apr_error(op,st);
#endif
    return;
  }

#ifdef DEBUGGING
  if(cbd->status != APR_SUCCESS)
    zeke_apr_error(op,cbd->status);
#endif
}


/* dispatch helpers */
static inline void  setup_dispatch(const char *op,
                                    zeke_callback_data_t ***cbdp,
                                    const zeke_connection_t *conn,
                                    zeke_watcher_fn watcher,
                                    zeke_callback_fn callback,
                                    const zeke_context_t **ctx,
                                    zeke_watcher_cb_data_t **watcher_ctx)
{
  zeke_callback_data_t **cbd = *cbdp;
  apr_pool_t *pool = zeke_session_pool(conn);
  assert(pool != NULL);

  if(cbd == NULL) {
    cbd = apr_palloc(pool,sizeof(zeke_callback_data_t*));
    assert(cbd != NULL);
    *cbd = NULL;
    *cbdp = cbd;
  }
  
  if(*ctx == NULL)
    *ctx = pool;

  if(*cbd == NULL)
    *cbd = zeke_callback_data_create_ex(conn,watcher,pool,1);

  (*cbd)->ctx = *ctx;
  (*cbd)->priv->callback = callback;
  if(op)
    (*cbd)->priv->op = apr_pstrdup(pool,op);

  if(watcher_ctx)
    *watcher_ctx = NULL;

  if(watcher && watcher_ctx) {
    *watcher_ctx = zeke_watcher_cb_data_create(conn,(*cbd)->priv->watcher_user_ctx);
    (*cbd)->priv->watcher_ctx = *watcher_ctx;
    (*watcher_ctx)->priv->watcher = watcher;
    (*watcher_ctx)->priv->watcher_ctx = *watcher_ctx;
    (*watcher_ctx)->ctx = *watcher_ctx;
  }
}

static inline zk_status_t finish_dispatch(int rc,
                                          zeke_callback_data_t **cbd,
                                          zeke_watcher_cb_data_t **watcher_ctx)

{
  zk_status_t st = APR_FROM_ZK_ERROR(rc);
  if(st != APR_SUCCESS) {
    zeke_fatal("finish_dispatch",st);
    if(cbd) {
      if(*cbd)
        zeke_callback_data_destroy(*cbd);
      *cbd = NULL;
    }
    if(watcher_ctx) {
      if(*watcher_ctx)
        zeke_watcher_cb_data_destroy(*watcher_ctx);
      *watcher_ctx = NULL;
    }
  }

  return st;
}

/* Misc */
ZEKE_API(zk_status_t) zeke_callback_status_set(const zeke_callback_data_t *cbd,
                                               zk_status_t new_status)
{
  zk_status_t st = cbd->status;
  ((zeke_callback_data_t*)cbd)->status = new_status;
  return st;
}

ZEKE_API(zk_status_t) zeke_callback_status_get(const zeke_callback_data_t *cbd)
{
  if(cbd->priv->status != APR_SUCCESS)
    return cbd->priv->status;
  return cbd->status;
}

/* Dispatch handlers */
ZEKE_API(zk_status_t) zeke_aget(zeke_callback_data_t **cbd,
                                  const zeke_connection_t *conn, 
                                  const char *path,
                                  zeke_watcher_fn watcher,
                                  zeke_callback_fn callback,
                                  const zeke_context_t *ctx)
{
  zeke_watcher_cb_data_t *watcher_ctx = NULL;
  int rc;

  if(cbd && *cbd && (*cbd)->priv)
    watcher_ctx = (*cbd)->priv->watcher_ctx;
  setup_dispatch("zoo_awget",&cbd,conn,watcher,callback,&ctx,&watcher_ctx);
  rc = zoo_awget(conn->zh,path,watcher ? dispatch_watcher : NULL,
                 watcher ? watcher_ctx : NULL, dispatch_stat_value,*cbd);
  
  return finish_dispatch(rc,cbd,&watcher_ctx);
}

ZEKE_API(zk_status_t) zeke_aget_children(zeke_callback_data_t **cbd,
                               const zeke_connection_t *conn,
                               const char *path,
                               zeke_watcher_fn watcher,
                               zeke_callback_fn callback,
                               const zeke_context_t *ctx)
{
  zeke_watcher_cb_data_t *watcher_ctx = NULL;
  int rc;

  if(cbd && *cbd && (*cbd)->priv)
    watcher_ctx = (*cbd)->priv->watcher_ctx;
  setup_dispatch("zoo_awget_children2",&cbd,conn,watcher,
                  callback,&ctx,&watcher_ctx);
  rc = zoo_awget_children2(conn->zh,path,watcher ? dispatch_watcher : NULL,
                           watcher ? watcher_ctx : NULL,
                           dispatch_aget_children,*cbd);

  return finish_dispatch(rc,cbd,&watcher_ctx);
}

ZEKE_API(zk_status_t) zeke_acreate(zeke_callback_data_t **cbd,
                                   const zeke_connection_t *conn,
                                   const char *path,
                                   const char *value, apr_ssize_t value_len,
                                   const struct ACL_vector *acl, int flags,
                                   zeke_callback_fn callback,
                                   const zeke_context_t *ctx)
{
  int rc;

  setup_dispatch("zoo_acreate",&cbd,conn,NULL,callback,&ctx,NULL);
  if(acl == NULL)
    acl = &ZOO_OPEN_ACL_UNSAFE;
  if(value == NULL)
    value_len = -1;

  rc = zoo_acreate(conn->zh,path,value,value_len,acl,flags,
                   dispatch_const_char,*cbd);
  return finish_dispatch(rc,cbd,NULL);
}

ZEKE_API(zk_status_t) zeke_adelete(zeke_callback_data_t **cbd,
                                   const zeke_connection_t *conn,
                                   const char *path,
                                   int version,
                                   zeke_callback_fn callback,
                                   const zeke_context_t *ctx)
{
  int rc;

  setup_dispatch("zoo_adelete",&cbd,conn,NULL,callback,&ctx,NULL);
  rc = zoo_adelete(conn->zh,path,version,dispatch_void,*cbd);
  return finish_dispatch(rc,cbd,NULL);
}

ZEKE_API(zk_status_t) zeke_aexists(zeke_callback_data_t **cbd,
                                   const zeke_connection_t *conn,
                                   const char *path,
                                   zeke_watcher_fn watcher,
                                   zeke_callback_fn callback,
                                   const zeke_context_t *ctx)
{
  int rc;
  zeke_watcher_cb_data_t *watcher_ctx = NULL;

  if(cbd && *cbd && (*cbd)->priv)
    watcher_ctx = (*cbd)->priv->watcher_ctx;
  setup_dispatch("zoo_awexists",&cbd,conn,watcher,callback,&ctx,&watcher_ctx);
  rc = zoo_awexists(conn->zh,path,watcher ? dispatch_watcher : NULL,
                    watcher ? watcher_ctx : NULL,dispatch_stat_void,*cbd);
  return finish_dispatch(rc,cbd,&watcher_ctx);
}

ZEKE_API(zk_status_t) zeke_aset(zeke_callback_data_t **cbd,
                                const zeke_connection_t *conn,
                                const char *path,
                                const char *buffer,
                                apr_size_t buflen,
                                int version,
                                zeke_callback_fn callback,
                                const zeke_context_t *ctx)
{
  int rc;
  setup_dispatch("zoo_aset",&cbd,conn,NULL,callback,&ctx,NULL);
  if(buffer == NULL)
    buflen = -1;
  rc = zoo_aset(conn->zh,path,buffer,buflen,version,
                dispatch_stat_void,*cbd);
  return finish_dispatch(rc,cbd,NULL);
}

ZEKE_API(zk_status_t) zeke_async(zeke_callback_data_t **cbd,
                                 const zeke_connection_t *conn,
                                 const char *path,
                                 zeke_callback_fn callback,
                                 const zeke_context_t *ctx)
{
  int rc;
  setup_dispatch("zoo_async",&cbd,conn,NULL,callback,&ctx,NULL);
  rc = zoo_async(conn->zh,path,dispatch_const_char,*cbd);
  return finish_dispatch(rc,cbd,NULL);
}

ZEKE_API(zk_status_t) zeke_aget_acl(zeke_callback_data_t **cbd,
                                    const zeke_connection_t *conn,
                                    const char *path,
                                    zeke_callback_fn callback,
                                    const zeke_context_t *ctx)
{
  int rc;
  setup_dispatch("zoo_aget_acl",&cbd,conn,NULL,callback,&ctx,NULL);
  rc = zoo_aget_acl(conn->zh,path,dispatch_get_acl,*cbd);
  return finish_dispatch(rc,cbd,NULL);
}

ZEKE_API(zk_status_t) zeke_aset_acl(zeke_callback_data_t **cbd,
                                    const zeke_connection_t *conn,
                                    const char *path,
                                    int version,
                                    const struct ACL_vector *acl,
                                    zeke_callback_fn callback,
                                    const zeke_context_t *ctx)
{
  int rc;
  setup_dispatch("zoo_aset_acl",&cbd,conn,NULL,callback,&ctx,NULL);
  if(acl == NULL)
    acl = &ZOO_OPEN_ACL_UNSAFE;
  rc = zoo_aset_acl(conn->zh,path,version,(struct ACL_vector*)acl,
                    dispatch_void,*cbd);
  return finish_dispatch(rc,cbd,NULL);
}

ZEKE_API(zk_status_t) zeke_amulti(zeke_callback_data_t **cbd,
                                  const zeke_connection_t *conn,
                                  const apr_array_header_t *ops_in,
                                  zeke_callback_fn callback,
                                  const zeke_context_t *ctx)
{
  int rc;
  apr_array_header_t *ops;
  apr_size_t nops = ops_in->nelts;
  zeke_callback_data_t *c;

  setup_dispatch("zoo_amulti",&cbd,conn,NULL,callback,&ctx,NULL);
  c = *cbd;
  if(c->pool == ops_in->pool || zeke_session_pool(conn) == ops_in->pool)
    ops = (apr_array_header_t*)ops_in;
  else
    ops = apr_array_copy(c->pool,ops_in);
  assert(ops != NULL);
  c->vectors.results = apr_palloc(c->pool,(nops+1)* sizeof(zoo_op_result_t));
  c->priv->multi_results = c->vectors.results;
  rc = zoo_amulti(conn->zh,nops,(zoo_op_t*)ops->elts,c->priv->multi_results,
                  dispatch_void,c);
  return finish_dispatch(rc,cbd,NULL);
}


