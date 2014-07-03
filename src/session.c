#include "internal.h"
#include "connection.h"
#include "io.h"

#include "libzeke_session.h"

#define ZEKE_SESSION_KEY_PREFIX "zeke-user-session-context:"

typedef struct {
  zeke_session_cleanup_fn handler;
  const void *arg;
} session_cleanup_ctx_s;

typedef struct {
  apr_pool_t *pool;
  const char *key;
  apr_array_header_t *cleanups;
  zeke_context_t *user_context;
} session_context_mgr_t;

static apr_status_t session_context_run_cleanups(void *data)
{
  session_context_mgr_t *mgr = (session_context_mgr_t*)data;

  if(mgr->cleanups && mgr->cleanups->nelts) {
    unsigned long i;

    for(i = 0; i < mgr->cleanups->nelts; i++) {
      zeke_session_cleanup_fn handler;
      const void *arg;
      session_cleanup_ctx_s *cleanup = &APR_ARRAY_IDX(mgr->cleanups,i,session_cleanup_ctx_s);
      if(cleanup->handler) {
        handler = cleanup->handler;
        arg = cleanup->arg;
        cleanup->handler = NULL;
        cleanup->arg = NULL;
        handler(mgr->user_context,(void*)arg);
      }
    }
  }

  return APR_SUCCESS;
}

static session_context_mgr_t *session_context_mgr_get(apr_pool_t *pool,
                                                      const char *key)
{
  ZEKE_MAY_ALIAS(session_context_mgr_t) *mgr = NULL;
  char realkey[ZEKE_BUFSIZE] = { '\0' };

  strncat(realkey,key,sizeof(realkey)-1);
  realkey[sizeof(realkey)-1] = '\0';

  if(pool == NULL) {
    zk_eprintf("no session pool exists for the given connection\n");
    abort();
  }

  if(apr_pool_userdata_get((void**)&mgr,realkey,pool) != APR_SUCCESS || mgr == NULL) {
    mgr = apr_palloc(pool,sizeof(session_context_mgr_t));
    assert(mgr != NULL);
    mgr->pool = pool;
    mgr->key = apr_pstrdup(pool,realkey);
    mgr->cleanups = apr_array_make(pool,1,sizeof(session_cleanup_ctx_s));
    assert(mgr->cleanups != NULL);
    mgr->user_context = NULL;
    apr_pool_userdata_setn(mgr,mgr->key,session_context_run_cleanups,pool);
  }
  return mgr;
}

ZEKE_API(zeke_context_t*) zeke_session_context_create(const zeke_connection_t *conn,
                                                      const char *key,
                                                      apr_size_t sz)
{
  apr_pool_t *pool = zeke_session_pool(conn);
  session_context_mgr_t *mgr;

  mgr = session_context_mgr_get(pool,key);
  assert(mgr != NULL);

  if(mgr->user_context == NULL) {
    mgr->user_context = apr_pcalloc(mgr->pool,sz);
    assert(mgr->user_context != NULL);
  }

  return mgr->user_context;
}

ZEKE_API(void) zeke_session_context_release(const zeke_connection_t *conn,
                                            const char *key)
{
  apr_pool_t *pool = zeke_session_pool(conn);
  session_context_mgr_t *mgr;

  mgr = session_context_mgr_get(pool,key);
  assert(mgr != NULL);

  if(mgr->user_context != NULL) {
    session_context_run_cleanups(mgr);
    mgr->user_context = NULL;
  }
}

static session_cleanup_ctx_s *new_cleanup(apr_array_header_t *arr)
{
  session_cleanup_ctx_s *ctx = NULL;

  int i;

  for(i = 0; i < arr->nelts; i++, ctx = NULL) {
    ctx = &APR_ARRAY_IDX(arr,i,session_cleanup_ctx_s);
    if(ctx->handler == NULL && ctx->arg == NULL)
      break;

  }
  if(!ctx)
    ctx = &APR_ARRAY_PUSH(arr,session_cleanup_ctx_s);
  return ctx;
}

ZEKE_API(void) zeke_session_cleanup_register(const zeke_connection_t *conn,
                                             const char *key,
                                             const void *arg,
                                             zeke_session_cleanup_fn handler)
{
  apr_pool_t *pool = zeke_session_pool(conn);
  session_context_mgr_t *mgr = session_context_mgr_get(pool,key);
  session_cleanup_ctx_s *cleanup;

  cleanup = new_cleanup(mgr->cleanups);
  assert(cleanup != NULL);
  cleanup->handler = handler;
  cleanup->arg = arg;
}

ZEKE_API(zeke_context_t*) zeke_session_context(const zeke_connection_t *conn,
                                               const char *key)
{
  apr_pool_t *pool = zeke_session_pool(conn);
  session_context_mgr_t *mgr;

  if(!pool)
    return NULL;

  mgr = session_context_mgr_get(pool,key);
  if(!mgr)
    return NULL;

  return mgr->user_context;
}

ZEKE_API(int) zeke_session_context_exists(const zeke_connection_t *conn,
                                          const char *key)
{
  apr_pool_t *pool;
  session_context_mgr_t *mgr = NULL;

  if(conn && key && conn->zh != NULL && (pool = zeke_session_pool(conn)) != NULL) {
    mgr = session_context_mgr_get(pool,key);
  }

  return (mgr && mgr->user_context != NULL);
}

ZEKE_API(zk_status_t) zeke_session_cleanups_run(const zeke_connection_t *conn,
                                                const char *key)
{
  apr_pool_t *pool = zeke_session_pool(conn);
  session_context_mgr_t *mgr;

  if(!pool)
    return APR_ENOENT;
  mgr = session_context_mgr_get(pool,key);
  if(!mgr || !mgr->user_context)
    return APR_ENOENT;

  return session_context_run_cleanups(mgr);
}


