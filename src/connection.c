#include "internal.h"
#include "connection.h"
#include "io.h"
#include "hooks.h"

#define ZEKE_SOCKET_CONNECTION_KEY "_zeke_private:socket_connection_key"

#if APR_SIZEOF_VOIDP == 8
typedef apr_uint64_t zeke_uintptr_t;
#define ZEKE_UINTPTR_C APR_UINT64_C
#else
typedef apr_uint32_t zeke_uintptr_t;
#define ZEKE_UINTPTR_C(c) (APR_UINT64_C(c) & (0xffffffffU))
#endif

struct init_context_state {
  apr_uint64_t current,previous;
  apr_time_t last_change,init_time;
  apr_uint32_t auto_hash_index;
  apr_uint32_t in_use;
};

static zeke_uintptr_t default_watcher_hash_key = ZEKE_UINTPTR_C(0xf3e3f00f32abbc3f);

#define DEFAULT_WATCHER_HASH_KEY ((void*)default_watcher_hash_key)
#define DEFAULT_WATCHER_HASH_KEY_SIZE sizeof(zeke_uintptr_t)

ZEKE_EXTERN void update_zk_info(zhandle_t *zh, int type, int state, const char *path,
                                void *data);

ZEKE_POOL_IMPLEMENT_ACCESSOR(connection)
ZEKE_IMPLEMENT_HOOK_GET(ZEKE_PRIVATE,connection,session_started)
ZEKE_IMPLEMENT_HOOK_GET(ZEKE_PRIVATE,connection,closed)
ZEKE_IMPLEMENT_HOOK_GET(ZEKE_PRIVATE,connection,starting)
#ifdef ARGS_DECL
#undef ARGS_DECL
#endif
#ifdef ARGS_USE
#undef ARGS_USE
#endif
#define ARGS_DECL(...) ZEKE_HOOK_ARGS_DECL(connection, ## __VA_ARGS__)
#define ARGS_USE(...) ZEKE_HOOK_ARGS_USE(connection, ## __VA_ARGS__)
ZEKE_IMPLEMENT_PUBLIC_HOOK_VOID(connection,session_started,ARGS_DECL(apr_pool_t *session_pool),ARGS_USE(session_pool))
ZEKE_IMPLEMENT_PUBLIC_HOOK_VOID(connection,closed,ARGS_DECL(),ARGS_USE())
ZEKE_IMPLEMENT_PUBLIC_HOOK_VOID(connection,starting,ARGS_DECL(apr_socket_t *sock),ARGS_USE(sock))

static apr_pool_t *connection_context_get_best_pool(const zeke_connection_t *conn);
static init_context_s *connection_context_require(apr_pool_t *pool);
static init_context_s *connection_context_get(const zeke_connection_t*);

static zk_status_t nuke_zhandle(zhandle_t **zh, apr_pool_t *pool)
{
  zk_status_t st = APR_SUCCESS;
  if(zh && *zh) {
    if(pool)
      zeke_io_reset_state(pool);

    st = close_zhandle(*zh);
    *zh = NULL;
  }
  return st;
}

static unsigned int watcher_hashfunc(const char *key, apr_ssize_t *klen)
{
  if(key == DEFAULT_WATCHER_HASH_KEY && (*klen == DEFAULT_WATCHER_HASH_KEY_SIZE ||
                                         *klen == APR_HASH_KEY_STRING)) {
    *klen = DEFAULT_WATCHER_HASH_KEY_SIZE;
    return (zeke_uintptr_t)DEFAULT_WATCHER_HASH_KEY;
  }

  return apr_hashfunc_default(key,klen);
}

ZEKE_PRIVATE(void)
zeke_connection_sort_all_hooks(zeke_connection_t *conn)
{
  if((conn->flags & ZEKE_FLAG_CONNECTION_HOOKS_SORTED) == 0) {
    ZEKE_SORT_HOOK(connection,closed,conn);
    ZEKE_SORT_HOOK(connection,starting,conn);
    conn->flags |= ZEKE_FLAG_CONNECTION_HOOKS_SORTED;
  }
}

static
void connection_socket_associate(zeke_connection_t *conn, apr_socket_t *sock)
{
  apr_pool_t *pool = sock ? apr_socket_pool_get(sock) : NULL;

  assert(conn != NULL);

  if(conn->sock != sock) {
    if(conn->sock) {
      apr_pool_t *oldpool = apr_socket_pool_get(conn->sock);
      apr_socket_data_set(conn->sock,NULL,ZEKE_SOCKET_CONNECTION_KEY,NULL);
      apr_pool_cleanup_kill(oldpool,&conn->sock,zk_indirect_wipe);
      if(pool && oldpool != pool)
        apr_pool_cleanup_kill(pool,&conn->sock,zk_indirect_wipe);
    }

    conn->sock = sock;

    if(sock) {
      conn->flags |= ZEKE_FLAG_CONNECTION_STARTING;
      assert(apr_socket_data_set(sock,conn,ZEKE_SOCKET_CONNECTION_KEY,NULL) == APR_SUCCESS);
      zeke_pool_cleanup_indirect(pool,conn->sock);
      apr_socket_inherit_unset(sock);

      zeke_connection_sort_all_hooks(conn);
      conn->flags |= ZEKE_FLAG_CONNECTION_STARTING;
      zeke_connection_run_starting_hook(conn,conn->sock);
    } else
      conn->flags &= ~ZEKE_FLAG_CONNECTION_STARTING;
  }
}

ZEKE_API(const zeke_connection_t*) zeke_zhandle_connection_get(const zhandle_t *zh)
{
  const init_context_s *ctx = NULL;
  assert(zh != NULL);
  ctx = zoo_get_context((zhandle_t*)zh);

  return (ctx ? ctx->connection : NULL);
}

ZEKE_API(apr_pool_t*) zeke_connection_context_pool_get(const zeke_connection_t *conn)
{
  return (conn->zh ? zeke_zhandle_context_pool(conn->zh) : NULL);
}

static apr_pool_t *connection_context_get_best_pool(const zeke_connection_t *conn)
{
  apr_pool_t *pool = NULL;

  if(conn && conn->zh)
    pool = zeke_zhandle_context_pool(conn->zh);
  return pool ? pool : conn->pool;
}

ZEKE_API(apr_pool_t*) zeke_zhandle_context_pool(const zhandle_t *zh)
{
  const init_context_s *ctx = NULL;
  assert(zh != NULL);
  ctx = zoo_get_context((zhandle_t*)zh);

  return (ctx ? ctx->pool : NULL);
}

ZEKE_API(apr_pool_t*) zeke_session_pool(const zeke_connection_t *conn)
{
  assert(conn != NULL);
  return (conn->zh ?zeke_zhandle_pool(conn->zh) : NULL);
}

ZEKE_API(const apr_socket_t*) zeke_connection_socket_get(const zeke_connection_t *conn)
{
  return conn->sock;
}

ZEKE_API(const zhandle_t*) zeke_connection_zhandle_get(const zeke_connection_t *conn)
{
  return conn->zh;
}

ZEKE_API(zk_status_t) zeke_connection_create_ex(zeke_connection_t **conn,
                                               const char *zhosts,
                                               zeke_watcher_fn watcher,
                                               zhandle_t *zh,
                                               int use_subpool,
                                               apr_pool_t *pool)
{
  apr_status_t st = APR_SUCCESS;
  apr_pool_t *subpool = NULL;

  if(global_io_pool == NULL)
    global_io_pool = zeke_root_subpool_create();
  if(pool == NULL)
    pool = global_io_pool;

  if(use_subpool) {
    LOG("CONN SUBPOOL");
    if((st = apr_pool_create(&subpool,pool)) != APR_SUCCESS)
      return st;
    zeke_pool_tag(subpool,"per-connection subpool");
  } else
    subpool = pool;

  *conn = (zeke_connection_t*)apr_pcalloc(subpool,sizeof(zeke_connection_t));
  if(*conn == NULL)
    return APR_ENOMEM;

  (*conn)->pool = subpool;
  (*conn)->zh = zh;

  if(zhosts)
    (*conn)->zhosts = apr_pstrdup(subpool,zhosts);

  (*conn)->watchers = apr_hash_make_custom(subpool,watcher_hashfunc);
  if(watcher)
    apr_hash_set((*conn)->watchers,DEFAULT_WATCHER_HASH_KEY,
                                   DEFAULT_WATCHER_HASH_KEY_SIZE,
                                   watcher);

  /*
  apr_pool_cleanup_register(subpool,conn,debug_close,apr_pool_cleanup_null);
  */
  apr_pool_cleanup_register(subpool,&(*conn)->zh,
          close_zhandle_indirect,apr_pool_cleanup_null);

  zeke_global_hooks_sort(0);
  zeke_run_new_connection(*conn);

  return st;
}

ZEKE_API(zk_status_t) zeke_connection_create(zeke_connection_t **conn,
                                               const char *zhosts,
                                               zeke_watcher_fn watcher,
                                               apr_pool_t *pool)
{
  return zeke_connection_create_ex(conn,zhosts,watcher,NULL,1,pool);
}

ZEKE_API(zk_status_t) zeke_connection_shutdown(zeke_connection_t *conn)
{
  if(!zeke_connection_is_open(conn))
    return APR_EINVAL;

  zeke_connection_sort_all_hooks(conn);
  conn->flags |= ZEKE_FLAG_CONNECTION_CLOSED;
  zeke_connection_run_closed_hook(conn);
  if(conn->sock)
    return apr_socket_shutdown(conn->sock,APR_SHUTDOWN_WRITE);
  return APR_EBADF;
}

ZEKE_API(int) zeke_connection_is_closed(const zeke_connection_t *conn)
{
  return (!conn->zh || (conn->flags & ZEKE_FLAG_CONNECTION_CLOSED));
}

ZEKE_API(int) zeke_connection_is_starting(const zeke_connection_t *conn)
{
  return conn->flags & ZEKE_FLAG_CONNECTION_STARTING;
}

ZEKE_API(int) zeke_connection_is_open(const zeke_connection_t *conn)
{
  int state = 0;

  if (conn->zh != NULL && !(conn->flags & ZEKE_FLAG_CONNECTION_CLOSED))
  {
    state = zoo_state(conn->zh);
    state = (state != 0 && state != ZOO_AUTH_FAILED_STATE &&
                           state != ZOO_EXPIRED_SESSION_STATE);
  }
  return state;
}

ZEKE_API(int) zeke_connection_is_active(const zeke_connection_t *conn)
{
  int state = 0;

  if(conn->zh != NULL && conn->sock != NULL &&
                      !(conn->flags & (ZEKE_FLAG_CONNECTION_CLOSED|
                                       ZEKE_FLAG_CONNECTION_STARTING))) {
    state = zoo_state(conn->zh);
    state = !(state == 0 ||
              state == ZOO_AUTH_FAILED_STATE ||
              state == ZOO_EXPIRED_SESSION_STATE);
  }

  return state;
}

ZEKE_API(zk_status_t) zeke_connection_close(zeke_connection_t *conn)
{
  zeke_connection_sort_all_hooks(conn);
  conn->flags |= ZEKE_FLAG_CONNECTION_CLOSED;
  zeke_connection_run_closed_hook(conn);
  if(conn->sock) {
    apr_pool_destroy(apr_socket_pool_get(conn->sock));
    /* cleanup handler will nuke conn->sock */
    assert(conn->sock == NULL);
  }
  if(conn->zh)
    return nuke_zhandle(&conn->zh, connection_context_get_best_pool(conn));
  return APR_SUCCESS;
}

ZEKE_API(zk_status_t) zeke_connection_socket_maint(zeke_connection_t *conn,
                                                   apr_uint32_t *flags,
                                                   apr_interval_time_t *timeout)
{
  zk_status_t st = APR_SUCCESS;
  apr_socket_t *cursock = conn->sock;
  apr_os_sock_t fd = -1;
  apr_os_sock_t curfd = -1;
  int interest = 0;
  struct timeval tv = {0,0};

  if(!conn->zh)
    /* connection not started, cannot manage socket */
    return APR_FROM_ZK_ERROR(ZINVALIDSTATE);

  if(!cursock)
    return APR_EINVAL;
  if((st = apr_os_sock_get(&curfd,cursock)) != APR_SUCCESS)
    return st;
  fd = curfd;

  LOG("calling zookeeper_interest [maint]");
  if((st = APR_FROM_ZK_ERROR(zookeeper_interest(conn->zh,&fd,&interest,&tv)))
                                                != APR_SUCCESS)
    return st;

  if(curfd != fd) {
    apr_pool_t *pool = apr_socket_pool_get(cursock);
    /* Invalidate current socket */
    if(flags)
      *flags = 0;
    if(timeout)
      *timeout = APR_TIME_C(-1);
    connection_socket_associate(conn,NULL);
    cursock = NULL;
    apr_pool_clear(pool);
    return APR_EOF;
  }
  if(flags) {
    *flags = 0;
    if(interest & ZOOKEEPER_READ)
      *flags |= APR_READ;
    if(interest & ZOOKEEPER_WRITE)
      *flags |= APR_WRITE;
  }

  if(timeout)
    *timeout = apr_time_make(tv.tv_sec,tv.tv_usec);
  return APR_SUCCESS;
}

ZEKE_API(zk_status_t) zeke_connection_socket_update(zeke_connection_t *conn,
                                                    apr_socket_t **sock,
                                                    apr_int16_t *events,
                                                    apr_interval_time_t *timeout,
                                                    void **context)
{
  zk_status_t st = APR_SUCCESS;
  apr_pool_t *pool = NULL;
  apr_socket_t *cursock = conn->sock;
  apr_os_sock_t fd = -1;
  apr_os_sock_t curfd = -1;
  int interest = 0;
  struct timeval tv = {0,0};

  if(!conn->zh)
    /* connection not started, cannot manage socket */
    return APR_FROM_ZK_ERROR(ZINVALIDSTATE);

  if(cursock) {
    pool = apr_socket_pool_get(cursock);
    if((st = apr_os_sock_get(&curfd,cursock)) != APR_SUCCESS)
      goto conn_socket_update_exit;
    fd = curfd;
  } else if ((st = apr_pool_create(&pool,conn->pool))!= APR_SUCCESS)
    goto conn_socket_update_exit;

  LOG("calling zookeeper_interest");
  if((st = APR_FROM_ZK_ERROR(zookeeper_interest(conn->zh,&fd,&interest,&tv)))
                                                != APR_SUCCESS)
    goto conn_socket_update_exit;

  if(curfd != fd && cursock != NULL) {
    /* Invalidate current socket */
    connection_socket_associate(conn,NULL);
    cursock = NULL;
    apr_pool_clear(pool);
  }
  if(cursock == NULL) {
    if((st = apr_os_sock_put(&cursock,&fd,pool)) == APR_SUCCESS)
      connection_socket_associate(conn,cursock);
  }

conn_socket_update_exit:
  if(st == APR_SUCCESS) {
    assert(cursock != NULL);

    if(sock != NULL)
      *sock = cursock;

    if(events != NULL) {
      *events = 0;
      if(interest & ZOOKEEPER_READ)
        *events |= APR_POLLIN;
      if(interest & ZOOKEEPER_WRITE)
        *events |= APR_POLLOUT;
    }
    if(timeout != NULL)
      *timeout = apr_time_make(tv.tv_sec,tv.tv_usec);
    if(context != NULL)
      *context = conn;
  }

  return st;
}

ZEKE_API(zk_status_t) zeke_connection_pollfd_set(zeke_connection_t *conn,
                                                 apr_pollfd_t *pollfd,
                                                 apr_size_t *npollfds,
                                                 apr_interval_time_t *min_timeout)
{
  zk_status_t st = APR_SUCCESS;
  apr_pool_t *pool = NULL;
  apr_socket_t *sock = conn->sock;
  apr_size_t def_size = 1;
  apr_size_t i;

  if(!conn->zh)
    /* connection not started, cannot set pollfd */
    return APR_FROM_ZK_ERROR(ZINVALIDSTATE);

  if(!npollfds)
    npollfds = &def_size;

  if(sock) {
    pool = apr_socket_pool_get(sock);
  } else {
    if((st = apr_pool_create(&pool,conn->pool)) != APR_SUCCESS)
      goto conn_pollfd_set_exit;
  }
  if(!sock)
    zeke_pool_tag(pool,"polling pool");

  for(i = 0; i < *npollfds; i++, pollfd++) {
    int rc;
    apr_os_sock_t fd = -1;
    int interest = 0;
    apr_interval_time_t poll_timeout = APR_TIME_C(0);
    struct timeval tv = {0,0};


    if(i == 0) {
      if((rc = zookeeper_interest(conn->zh,&fd,&interest,&tv)) != ZOK) {
        st = APR_FROM_ZK_ERROR(rc);
        goto conn_pollfd_set_exit;
      }

      poll_timeout = apr_time_make(tv.tv_sec,tv.tv_usec);
      if(min_timeout && (*min_timeout == APR_TIME_C(-1) || poll_timeout < *min_timeout))
        *min_timeout = poll_timeout;
    }
    if(fd != -1) {
      apr_os_sock_t was = -1;
      if(sock) {
        assert(apr_os_sock_get(&was,sock) == APR_SUCCESS);
        if(was != fd) {
          connection_socket_associate(conn,NULL);
          sock = NULL;
          apr_pool_clear(pool);
        }
      }
      if(!sock) {
        if((st = apr_os_sock_put(&sock,&fd,pool)) != APR_SUCCESS)
          goto conn_pollfd_set_exit;
        connection_socket_associate(conn,sock);
      }
    } else if(sock && i == 0) {
      connection_socket_associate(conn,NULL);
      sock = NULL;
      apr_pool_clear(pool);
    }

    if(sock) {
      pollfd->p = pool;
      pollfd->reqevents = 0;
      pollfd->rtnevents = 0;
      pollfd->desc_type = APR_POLL_SOCKET;
      pollfd->desc.s = sock;
      pollfd->client_data = conn;

      if(interest & ZOOKEEPER_READ)
        pollfd->reqevents |= APR_POLLIN;
      if(interest & ZOOKEEPER_WRITE)
        pollfd->reqevents |= APR_POLLOUT;

      *npollfds = i+1;
    }
  }

conn_pollfd_set_exit:
  return st;
}

static apr_status_t cleanup_init_context(void *data)
{
  init_context_s *ctx = (init_context_s*)data;

  if(ctx && ctx->session_pool) {
    apr_pool_cleanup_kill(ctx->session_pool,&ctx->session_pool,zk_indirect_wipe);
    ctx->session_pool = NULL;
  }

  return APR_SUCCESS;
}

static init_context_s *connection_context_get(const zeke_connection_t *conn)
{
  ZEKE_MAY_ALIAS(init_context_s) *ctx = NULL;
  apr_pool_t *pool;

  assert(conn != NULL);
  pool = connection_context_get_best_pool(conn);
  if(pool)
    apr_pool_userdata_get((void**)&ctx, ZEKE_INIT_CONTEXT_KEY, pool);
  return ctx;
}

static init_context_s *connection_context_require(apr_pool_t *pool)
{
  apr_status_t st;
  ZEKE_MAY_ALIAS(init_context_s) *ctx = NULL;

  if((st = apr_pool_userdata_get((void**)&ctx,ZEKE_INIT_CONTEXT_KEY,pool)) != APR_SUCCESS || !ctx)
  {
    if((ctx = (init_context_s*)apr_pcalloc(pool,sizeof(init_context_s))) != NULL) {
      assert((ctx->state = apr_pcalloc(pool,sizeof(context_state_t))) != NULL);
      ctx->state->last_change = APR_TIME_C(-1);
      ctx->state->init_time = APR_TIME_C(-1);
      ctx->state->auto_hash_index = 1;
      ctx->state->in_use = 0;
      apr_pool_userdata_setn(ctx,ZEKE_INIT_CONTEXT_KEY,NULL,pool);
      apr_pool_cleanup_register(pool,ctx,cleanup_init_context,apr_pool_cleanup_null);
    }
  }

  assert(ctx != NULL);
  return ctx;
}

ZEKE_PRIVATE(zk_status_t) zeke_connection_maintenance(const zeke_connection_t *conn)
{
  init_context_s *ctx;

  if(conn->flags & ZEKE_FLAG_CONNECTION_CLOSED)
    return APR_ENOENT;

  ctx = connection_context_get(conn);

  if(ctx && ctx->state && ctx->state->init_time > APR_TIME_C(0) && !ctx->state->in_use) {
    ctx->state->in_use++;
    if((apr_time_now() - ctx->state->init_time) > (APR_TIME_C(ZK_TIMEOUT) * APR_TIME_C(1000))) {
      zhandle_t *zh = conn->zh;
      if(zh) {
        if(zoo_state(zh) == ZOO_CONNECTED_STATE) {
          ctx->state->init_time = APR_TIME_C(0);
          ctx->state->in_use--;
          return APR_SUCCESS;
        }
        LOG("init_Time = %" APR_UINT64_T_FMT,ctx->state->init_time);
        ctx->state->init_time = APR_TIME_C(-1);
        nuke_zhandle(&zh, connection_context_get_best_pool(conn));
      }
      ctx->state->in_use--;
      return APR_TIMEUP;
    }
    ctx->state->in_use--;
  }

  return APR_SUCCESS;
}

ZEKE_PRIVATE(void) zeke_connection_state_reset(const zeke_connection_t *conn)
{
  init_context_s *ctx = connection_context_get(conn);

  if(ctx) {
    ctx->state->current = ctx->state->previous = 0;
    ctx->state->last_change = APR_TIME_C(-1);
  }
}

ZEKE_PRIVATE(void) zeke_connection_state_update(const zeke_connection_t *conn)
{
  init_context_s *ctx = connection_context_require(conn->pool);

  if(!ctx->state->in_use) {
    if(zeke_connection_maintenance(conn) == APR_SUCCESS) {
      ctx->state->current++;
      ctx->state->last_change = apr_time_now();
    }
  }
}

ZEKE_PRIVATE(apr_uint64_t) zeke_connection_state_peek(const zeke_connection_t *conn,
                                                        apr_time_t *last_change)
{
  init_context_s *ctx = connection_context_require(conn->pool);

  if(last_change)
    *last_change = ctx->state->last_change;
  return ctx->state->current - ctx->state->previous;
}

ZEKE_PRIVATE(int) zeke_connection_state_ensure_current(const zeke_connection_t *conn,
                                                   const apr_time_t *older_than,
                                                   const apr_time_t *newer_than)
{
  int ensure = 0;
  init_context_s *ctx = NULL;

  while(ctx == NULL) {
    zeke_connection_maintenance(conn);
    ctx = connection_context_require(conn->pool);
  }

  assert(ctx->state->in_use == 0);

  if(ctx->state->current == ctx->state->previous) {
    ensure = 1;
    if(older_than) {
      if(ctx->state->last_change != APR_TIME_C(-1) &&
         ctx->state->last_change >= *older_than)
        ensure++;
    }
    if(newer_than) {
      if(ctx->state->last_change == APR_TIME_C(-1) ||
         ctx->state->last_change < *newer_than)
        ensure++;
    }
  }
  return ensure;
}

/* Destructive atomic read */
ZEKE_PRIVATE(apr_uint64_t) zeke_connection_state_nupdates(const zeke_connection_t *conn)
{
  init_context_s *ctx = connection_context_require(conn->pool);
  apr_uint64_t current = ctx->state->current;
  apr_uint64_t prev = ctx->state->previous;

  /* TODO: locking here for threading */
  ctx->state->previous = ctx->state->current;
  return current - prev;
}

ZEKE_PRIVATE(void) zeke_connection_flags_clear(zeke_connection_t *conn,
                                               apr_uint32_t flags)
{
  conn->flags &= ~flags;
}

ZEKE_PRIVATE(void) zeke_connection_flags_set(zeke_connection_t *conn,
                                             apr_uint32_t flags)
{
  conn->flags |= flags;
}

ZEKE_API(void) zeke_connection_named_watcher_add(zeke_connection_t *conn,
                                                        const char *name,
                                                        zeke_watcher_fn watcher)
{
    apr_hash_set(conn->watchers,name,APR_HASH_KEY_STRING,watcher);
}

ZEKE_API(zk_status_t) zeke_connection_named_watcher_remove(zeke_connection_t *conn,
                                                          const char *name)
{
  zk_status_t st = ZEOK;
  void *watcher;

  watcher = apr_hash_get(conn->watchers,name,APR_HASH_KEY_STRING);
  if(watcher == NULL)
    st = APR_ENOENT;
  else
    apr_hash_set(conn->watchers,name,APR_HASH_KEY_STRING,NULL);
  return st;
}

ZEKE_API(void) zeke_connection_watcher_add(zeke_connection_t *conn,
                                                  zeke_watcher_fn watcher)
{
  init_context_s *ctx = connection_context_require(conn->pool);
  apr_uint64_t next_key = default_watcher_hash_key + ctx->state->auto_hash_index++;
  char *key = apr_psprintf(conn->pool,"autowatcher key %" APR_UINT64_T_HEX_FMT, next_key);

  zeke_connection_named_watcher_add(conn,key,watcher);
}

ZEKE_API(zk_status_t) zeke_connection_start(zeke_connection_t *conn,
                                              clientid_t *client,
                                              unsigned *retries)
{
  apr_pool_t *pool = conn->pool;
  zk_status_t st = APR_SUCCESS;
  zhandle_t *zh = conn->zh;
  init_context_s *ctx = NULL;

  if(zh) {
    nuke_zhandle(&conn->zh,connection_context_get_best_pool(conn));
    zh = conn->zh;
  }

  if(conn->flags & ZEKE_FLAG_CONNECTION_CLOSED)
    return APR_FROM_ZK_ERROR(ZENOSESSION);

  if(retries && !*retries)
    return APR_FROM_ZK_ERROR(ZETOOMANYRETRIES);

  ctx = connection_context_require(pool);
  assert(ctx != NULL);
  ctx->pool = pool;
  ctx->connection = conn;

  while(--(*retries)) {
    int state = 0;
    client->client_id = 0;
    if(zh)
      zookeeper_close(zh);
    zh = zookeeper_init(conn->zhosts,&update_zk_info,ZK_TIMEOUT,client,ctx,0);
    if(zh == NULL)
      zeke_fatal("zookeeper_init",apr_get_os_error());
    state = zoo_state(zh);
    if(state == ZOO_EXPIRED_SESSION_STATE) {
      client->client_id = 0;
      zk_eprintf("FAIL, state is %d\n",zoo_state(zh));
      return st; /* will just restart again later */
    } else if(state != ZOO_AUTH_FAILED_STATE && state) {
      assert(ctx->state != NULL);
      ctx->state->init_time = apr_time_now();
      break;
    }
  }

  if((!retries || !*retries) && (!zh || (zoo_state(zh) != ZOO_CONNECTED_STATE &&
                                         zoo_state(zh) != ZOO_CONNECTING_STATE))) {
    if(zh) {
      if(conn->zh == zh)
        nuke_zhandle(&conn->zh,connection_context_get_best_pool(conn));
      else
        nuke_zhandle(&zh,connection_context_get_best_pool(conn));
    }
    return APR_FROM_ZK_ERROR(ZETOOMANYRETRIES);
  }

  if(st == APR_SUCCESS)
    conn->zh = zh;
  return st;
}

