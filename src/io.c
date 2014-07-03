/* zeke i/o loop */
#include "internal.h"
#include "dispatch.h"
#include "connection.h"
#include "io.h"
#include "hooks.h"
#include "event_private.h"

#include "libzeke_indirect.h"

#include <apr_atomic.h>

#ifdef APR_POLLSET_WAKEABLE
# ifdef ZEKE_USE_THREADS
# define ZEKE_DEFAULT_POLLSET_FLAGS (APR_POLLSET_THREADSAFE|APR_POLLSET_NOCOPY|APR_POLLSET_WAKEABLE)
# else /* !ZEKE_USE_THREADS */
# define ZEKE_DEFAULT_POLLSET_FLAGS (APR_POLLSET_NOCOPY|APR_POLLSET_WAKEABLE)
# endif /* ZEKE_USE_THREADS */
#else /* ! APR_POLLSET_WAKEABLE */
# ifdef ZEKE_USE_THREADS
# define ZEKE_DEFAULT_POLLSET_FLAGS (APR_POLLSET_THREADSAFE|APR_POLLSET_NOCOPY)
# else /* !ZEKE_USE_THREADS */
# define ZEKE_DEFAULT_POLLSET_FLAGS (APR_POLLSET_NOCOPY)
# endif /* ZEKE_USE_THREADS */
#endif /* ZEKE_POLLSET WAKEABLE */

#undef ZEKE_DEFAULT_POLLSET_FLAGS
#define ZEKE_DEFAULT_POLLSET_FLAGS 0

#define ZEKE_SOCKET_CONNECTION_KEY "_zeke_private:socket_connection_key"
#define ZEKE_POLLFD_KEY "_zeke_private:pollfd_key"
#define ZEKE_POLLSET_KEY "_zeke_private:pollset_key"

/* Number of ms to wait between calls to libzk during startup/reconnect, if
libzk is called more than once before session establishment, each call results in
an aborted connection which can lead to a spiral of death if other timing induces
a loop without enough delay. */
#define ZK_STARTUP_POLLTIMER 100

ZEKE_PRIVATE(apr_pool_t*) global_io_pool = NULL;
ZEKE_PRIVATE(const clientid_t*) zk_client_null = NULL;
ZEKE_PRIVATE(const zhandle_t*) zk_handle_null = NULL;

ZEKE_EXTERN zk_status_t zeke_connection_state_nupdates(const zeke_connection_t*);
ZEKE_EXTERN void zeke_connection_state_reset(const zeke_connection_t*);

ZEKE_PRIVATE(volatile int) in_io_loop = 0;
ZEKE_PRIVATE(volatile int) zeke_terminating = 0;
static int zeke_exit_code = 0;
static int (*interrupt_handler)(zeke_connection_t*) = NULL;
static clientid_t perm_client = {0,""};

#ifndef LIBZEKE_USE_LIBEVENT
#define ZK_POOL_CREATE apr_pool_create
#else /* LIBZEKE_USE_LIBEVENT */

struct zeke_io_meta {
  apr_pool_t *p;
  struct io_event_state *state;
  int code;
  struct event_base *base;
  struct event *ev;
};

#define ZK_POOL_CREATE zeke_event_base_pool_create

#if defined(APR_HAS_THREADS)
static apr_threadkey_t *global_state = NULL;
#else
static struct io_event_state *global_state = NULL;
#endif

static inline struct event_base *get_event_base(apr_pool_t *p);

ZEKE_PRIVATE(struct io_event_state *)io_current_event_state_get(void)
{
  struct io_event_state *state;
  if(!global_state)
    return NULL;

#if defined(APR_HAS_THREADS)
  if(apr_threadkey_private_get((void**)&state,global_state) != APR_SUCCESS)
    state = NULL;
#else
  state = global_state;
#endif
  ZEKE_MAGIC_CHECK_NULLOK(state, ZEKE_IO_EVENT_STATE_MAGIC);

  return state;
}

ZEKE_PRIVATE(apr_status_t) io_current_event_state_set(struct io_event_state *state)
{
  if(global_state == NULL) {
    if (global_io_pool == NULL)
      global_io_pool = zeke_root_subpool_create();
    assert(global_io_pool != NULL);
#if defined(APR_HAS_THREADS)
    assert(apr_threadkey_private_create(&global_state,NULL,global_io_pool) == APR_SUCCESS);
    zeke_pool_cleanup_indirect(global_io_pool,global_state);
    assert(global_state != NULL);
#else
    if(state)
      zeke_pool_cleanup_indirect(state->pool,global_state);
#endif
  }
#if defined(APR_HAS_THREADS)
  if(state) {
    assert(state->pool != NULL);
    return apr_threadkey_private_set(state,global_state);
  }

  return APR_EGENERAL;

#else
  global_state = state;
  return APR_SUCCESS;
#endif
}

ZEKE_PRIVATE(void)
event_loop_startup(evutil_socket_t ign, short what, void *mp)
{
  struct zeke_io_meta *m = (struct zeke_io_meta*)mp;

  if(m && m->p) {
    apr_pool_t *p = m->p;
    m->p = NULL;
    assert(m->base != NULL);
    zeke_global_hooks_sort(0);
    zeke_run_event_loop_begin(m->base,p);
    apr_pool_destroy(p);
  }
}

ZEKE_PRIVATE(apr_status_t) io_cleanup_event_state(void *sp)
{
  struct io_event_state *state = (struct io_event_state*)sp;
  struct io_event_state *gstate = NULL;

  ZEKE_MAGIC_CHECK(state, ZEKE_IO_EVENT_STATE_MAGIC);

#if defined(APR_HAS_THREADS)
  if(global_state) {
    if(apr_threadkey_private_get((void**)&gstate,global_state) == APR_SUCCESS &&
                                               gstate == state)
      apr_threadkey_private_set(NULL,global_state);
  }
#else
  if(state && global_state == state)
    global_state = NULL;
#endif
  if(state->pool)
    state->pool = NULL;

  return APR_SUCCESS;
}

ZEKE_PRIVATE(struct io_event_state*)
io_current_event_state_create_v(apr_pool_t *p, struct event_base *base,
                                 const char *fmt, va_list ap)
{
  apr_pool_t *pool;
  struct io_event_state *state;

  if(global_io_pool == NULL) {
    global_io_pool = zeke_root_subpool_create();
    zeke_pool_cleanup_indirect(global_io_pool,global_state);
  }
  assert(ZK_POOL_CREATE(&pool,(p ? p :global_io_pool)) == APR_SUCCESS);
  zeke_pool_tag(pool,"io event loop handle");
  state = apr_pcalloc(pool,sizeof(*state));
  state->pool = pool;
  state->magic = ZEKE_IO_EVENT_STATE_MAGIC;
  state->api = ZEKE_EVENT_API_INTERN;
  apr_pool_pre_cleanup_register(pool,state,io_cleanup_event_state);
  if(base)
    zeke_event_base_set(base,0,pool);
  if(state && fmt) {
    zeke_pool_tag(state->pool,apr_pvsprintf(state->pool,fmt,ap));
  }
  if(state) {
    struct zeke_io_meta *m;
    assert(ZK_POOL_CREATE(&pool,state->pool) == APR_SUCCESS);
    zeke_pool_tag(pool,"metadata startup pool");
    m = apr_palloc(pool,sizeof(*m));
    m->p = pool;
    m->state = state;
    m->base = (base ? base : zeke_event_base_get(pool));
    m->code = 0;
    assert(zeke_event_create_ex(&m->ev,0,0,event_loop_startup,m,m->p) == APR_SUCCESS);
    assert(zeke_generic_event_add(m->ev, ZEKE_IMMEDIATE) == APR_SUCCESS);
  }
  return state;
}

ZEKE_PRIVATE(struct io_event_state*) io_current_event_state_create(apr_pool_t *p,
                                                                  struct event_base *base)
{
  struct io_event_state *state = io_current_event_state_create_v(p,base,NULL,NULL);

  if(state) {
    assert(io_current_event_state_set(state) == APR_SUCCESS);
  }

  return state;
}

ZEKE_API(struct io_event_state*) zeke_io_event_loop_create_ex(apr_pool_t *p,
                                                              struct event_base *base,
                                                              const char *fmt, ...)
{
  va_list ap;
  struct io_event_state *state;

  if(fmt)
    va_start(ap,fmt);
  state = io_current_event_state_create_v(p,base,fmt,ap);
  if(fmt)
    va_end(ap);

  return state;
}

ZEKE_API(struct io_event_state*) zeke_io_event_loop_create(apr_pool_t *p,
                                                           struct event_base *base)
{
  struct io_event_state *state = zeke_io_event_loop_create_ex(p,base,NULL);

  if(state)
    zeke_pool_tag(state->pool,"libzeke io event_loop [libevent low-level API]");
  return state;
}

ZEKE_API(struct event_base*) zeke_io_event_loop_base_get(const struct io_event_state *state)
{
  if(!state)
    state = io_current_event_state_get();

  ZEKE_MAGIC_CHECK_NULLOK(state, ZEKE_IO_EVENT_STATE_MAGIC);

  if(state) {
    assert(state->pool != NULL);
#ifdef DEBUGGING
    assert(zeke_event_base_exists(state->pool));
#endif
    return zeke_event_base_get(state->pool);
  }
  return NULL;
}

ZEKE_PRIVATE(struct event_base*) io_current_event_state_base(void)
{
  return zeke_io_event_loop_base_get(NULL);
}

ZEKE_API(void) zeke_io_event_loopbreak_ex(struct io_event_state *state)
{
  ZEKE_MAGIC_CHECK(state, ZEKE_IO_EVENT_STATE_MAGIC);

  if(!state->ev) {
    assert(state->pool != NULL);

    if(zeke_event_base_exists(state->pool)) {
      state->got_break++;
      event_base_loopbreak(zeke_event_base_get(state->pool));
    }
    return;
  }
#ifdef DEBUGGING
  assert(zeke_event_base_exists(state->pool));
#endif
  state->got_break++;
  event_base_loopbreak(zeke_event_base_get(state->pool));
}

ZEKE_API(void) zeke_io_event_loopbreak(void)
{
  zeke_io_event_loopbreak_ex(io_current_event_state_get());
}

static
void zeke_io_loopexit_event(evutil_socket_t fd, short ignore, void *lev)
{
  apr_pool_t *p;
  struct zeke_io_meta *loopexit = (struct zeke_io_meta*)lev;

  assert(loopexit != NULL && loopexit->p != NULL && loopexit->state != NULL);
  loopexit->state->exit_code = loopexit->code;
  loopexit->state->got_exit++;
  p = loopexit->p;
  loopexit->p = NULL;
  zeke_global_hooks_sort(0);
  zeke_run_event_loop_end(loopexit->base,p);
  event_base_loopbreak(loopexit->base);
  loopexit->base = NULL;
  apr_pool_destroy(p);
}

ZEKE_API(void) zeke_io_event_loopexit_ex(struct io_event_state *state,
                                         int code)
{
  apr_pool_t *p;
  struct zeke_io_meta *le;

  ZEKE_MAGIC_CHECK(state, ZEKE_IO_EVENT_STATE_MAGIC);

  ZEKE_ASSERT(state->ev != NULL,
               "no libzeke io event loop state or active io event loop event");

#ifdef DEBUGGING
  assert(zeke_event_base_exists(state->pool));
#endif
  assert(ZK_POOL_CREATE(&p,state->pool) == APR_SUCCESS);
  zeke_pool_tag(p,"loopexit state pool");
  le = apr_palloc(p,sizeof(*le));
  le->p = p;
  le->state = state;
  le->base = zeke_event_base_get(state->pool);
  le->ev = NULL;
  le->code = code;

  assert(zeke_event_create(&le->ev,p) == APR_SUCCESS);
  assert(zeke_generic_event_callback_set(le->ev,zeke_io_loopexit_event,le) == APR_SUCCESS);
  assert(zeke_generic_event_add(le->ev,ZEKE_IMMEDIATE) == APR_SUCCESS);
}

ZEKE_API(void) zeke_io_event_loopexit(int code)
{
  zeke_io_event_loopexit_ex(io_current_event_state_get(),code);
}

ZEKE_API(int) zeke_io_event_loop_gotbreak(struct io_event_state *state)
{
  if(!state)
    state = io_current_event_state_get();

  ZEKE_MAGIC_CHECK_NULLOK(state, ZEKE_IO_EVENT_STATE_MAGIC);

  if(state) {
    int got_break = state->got_break;
    state->got_break = 0;
#ifdef DEBUGGING
    assert(zeke_event_base_exists(state->pool));
#endif
    return (got_break && event_base_got_break(zeke_event_base_get(state->pool)));
  }
  return 0;
}

ZEKE_API(int) zeke_io_event_loop_gotexit(struct io_event_state *state,
                                         int *codep)
{
  if(!state)
    state = io_current_event_state_get();

  ZEKE_MAGIC_CHECK_NULLOK(state, ZEKE_IO_EVENT_STATE_MAGIC);

  if(state) {
    int got_exit = state->got_exit;
#ifdef DEBUGGING
    assert(zeke_event_base_exists(state->pool));
#endif
    if(got_exit && codep)
      *codep = state->exit_code;
    state->got_exit = state->exit_code = 0;
    return (got_exit && event_base_got_break(zeke_event_base_get(state->pool)));
  }
  return 0;
}

ZEKE_API(void) zeke_io_event_loop_destroy(struct io_event_state *state)
{
  ZEKE_MAGIC_CHECK(state, ZEKE_IO_EVENT_STATE_MAGIC);
  assert(state->pool != NULL);
  apr_pool_destroy(state->pool);
}
#endif /* LIBZEKE_USE_LIBEVENT */

ZEKE_API(int) zeke_is_safe_shutdown(int *code)
{
  if(zeke_terminating && code)
    *code = zeke_exit_code;
  return zeke_terminating;
}

static int check_shutdown(int sort, int *recurse, int *code)
{
  int rc = ZEKE_HOOK_OKAY;

  if(!recurse || !(*recurse)++) {
    if(sort && (!recurse || *recurse == 1))
      zeke_global_hooks_sort(0);

    rc = zeke_run_shutdown(code);
  }

  switch(rc) {
    case ZEKE_HOOK_ABORT:
      zeke_terminating = 0;
      return 0;
      break;
    case ZEKE_HOOK_OKAY:
      return 1;
      break;
    case ZEKE_HOOK_DECLINE:
      return 1;
      break;
    default:
      break;
  }
  return -1;
}

ZEKE_API(void) zeke_safe_shutdown(int code)
{
#ifdef LIBZEKE_USE_LIBEVENT
  if(in_io_loop && io_current_event_state_base() && zeke_terminating == 0) {
    zeke_terminating++;
    zeke_io_event_loopexit(code);
    return;
  }
#endif
  if(in_io_loop && check_shutdown(1,NULL,&code)) {
    zeke_exit_code = code;
    zeke_terminating++;
  }

  if(!in_io_loop)
    exit(code);
}

ZEKE_API(void) zeke_safe_shutdown_r(int code)
{
  static volatile apr_uint32_t spinlock = 0;

  if(check_shutdown(0,NULL,&code)) {
    if(apr_atomic_cas32(&spinlock,1,0) == 0) {
      zeke_exit_code = code;
      zeke_terminating++;
      assert(apr_atomic_dec32(&spinlock) == 0);
    }
  }
}

ZEKE_API(void) zeke_interrupt_handler_set(zeke_interrupt_fn fn)
{
  interrupt_handler = fn;
}

ZEKE_API(zeke_interrupt_fn) zeke_interrupt_handler_get(void)
{
  return interrupt_handler;
}

static apr_status_t zeke_terminate_print_msg(void *data)
{
  const char *msg = data;
  apr_file_t *f = (zstderr == NULL ? zstdout : zstderr);

  if(f) {
    apr_file_puts(msg,f);
    if(msg && (*(msg+strlen(msg)-1)) != '\n')
      apr_file_putc('\n',f);
    apr_file_flush(f);
  } else {
    fputs(msg,stderr);
    if(msg && (*(msg+strlen(msg)-1)) != '\n')
      fputc('\n',stderr);
  }
  return APR_SUCCESS;
}

static
void zeke_safe_shutdown_vconn(const zeke_connection_t *c,
                              int code, const char *fmt, va_list ap)
{
  const char *msg = NULL;
  int sort = 1;
  int recurse = 0;

  if(fmt) {
    if(!global_io_pool) {
      global_io_pool = zeke_root_subpool_create();
      zeke_pool_cleanup_indirect(global_io_pool, global_state);
    }
    msg = apr_pvsprintf(global_io_pool,fmt,ap);
    if(msg)
      apr_pool_cleanup_register(global_io_pool,msg,zeke_terminate_print_msg,apr_pool_cleanup_null);
  }

  if(c) {
    if(c->sock) {
#ifdef LIBZEKE_USE_LIBEVENT
      struct event *ev = zeke_socket_event_get(c->sock);
      struct io_event_state *state = io_current_event_state_get();
      if(ev && state && state->ev == ev) {
        zeke_io_event_loopexit_ex(state,code);
      } else if(check_shutdown(sort--,&recurse,&code)) {
        zeke_terminating++;
        zeke_exit_code = code;
        event_base_loopexit(event_get_base(ev),NULL);
      }

      apr_pool_destroy(apr_socket_pool_get(c->sock));
      if(in_io_loop)
        return;
#else
      apr_socket_close(c->sock);
#endif
    }
  }

  if(check_shutdown(sort,&recurse,&code)) {
    zeke_terminating++;
    zeke_exit_code = code;
    if(!in_io_loop)
      exit(code);
  }
}

ZEKE_API(void) zeke_safe_shutdown_va(int code, const char *fmt, va_list ap)
{
  zeke_safe_shutdown_vconn(NULL,code,fmt,ap);
}


ZEKE_API(void) zeke_safe_shutdown_conn(const zeke_connection_t *conn, int code)
{
  va_list ap;

  zeke_safe_shutdown_vconn(conn,code,NULL,ap);
}

ZEKE_API(void) zeke_safe_shutdown_ex(int code, const char *fmt, ...)
{
  va_list ap;

  va_start(ap,fmt);
  zeke_safe_shutdown_vconn(NULL,code,fmt,ap);
  va_end(ap);
}

ZEKE_PRIVATE(void) zeke_io_reset_state(apr_pool_t *pool)
{
  ZEKE_MAY_ALIAS(const init_context_s) *ctx = NULL;

  if(pool) {
    apr_pool_userdata_get((void**)&ctx,ZEKE_INIT_CONTEXT_KEY,pool);
    apr_pool_userdata_set(NULL,ZEKE_CLIENT_ID_KEY,NULL,pool);
    apr_pool_userdata_set(NULL,ZEKE_ZHANDLE_KEY,NULL,pool);
    if(ctx) {
      apr_pool_userdata_set(NULL,ZEKE_CLIENT_ID_KEY,NULL,ctx->pool);
      apr_pool_userdata_set(NULL,ZEKE_ZHANDLE_KEY,NULL,ctx->pool);
    }
  }

  LOG("IO RESET STATE context=%lx",(unsigned long)ctx);
  if(ctx)
    zeke_connection_state_update(ctx->connection);
}

static void io_state_reset(apr_pool_t *pool)
{
  ZEKE_MAY_ALIAS(const init_context_s) *ctx = NULL;
  if(pool) {
    apr_pool_userdata_get((void**)&ctx,ZEKE_INIT_CONTEXT_KEY,pool);
    apr_pool_userdata_set(NULL,ZEKE_CLIENT_ID_KEY,NULL,pool);
    apr_pool_userdata_set(NULL,ZEKE_ZHANDLE_KEY,NULL,pool);
    if(ctx) {
      apr_pool_userdata_set(NULL,ZEKE_CLIENT_ID_KEY,NULL,ctx->pool);
      apr_pool_userdata_set(NULL,ZEKE_ZHANDLE_KEY,NULL,ctx->pool);
    }
  }

  LOG("IO HARD RESET INTERNAL context=%lx",(unsigned long)ctx);
  if(ctx)
    zeke_connection_state_reset(ctx->connection);
}

ZEKE_PRIVATE(zk_status_t) zeke_io_set_state(const zeke_connection_t *conn,
                                     const clientid_t *client,
                                     const zhandle_t *zh)
{
  apr_pool_t *pool = zeke_connection_context_pool_get(conn);
  zk_status_t st = APR_SUCCESS;
  int update = 0;

  if(client && client != zk_client_null) {
    st = apr_pool_userdata_set(client,ZEKE_CLIENT_ID_KEY,NULL,pool);
    update++;
  }
  if(zh && zh != zk_handle_null) {
    st = apr_pool_userdata_set(zh,ZEKE_ZHANDLE_KEY,NULL,pool);
    update++;
  }
  if(client == NULL) {
    update++;
  }
  if(zh == NULL) {
    update++;
  }

  if(update)
    zeke_connection_state_update(conn);
  return st;
}

ZEKE_PRIVATE(zk_status_t) close_zhandle(void *data)
{
  apr_pool_t *pool = NULL;
  zk_status_t st = APR_SUCCESS;

  if(data) {
    pool = zeke_zhandle_pool((zhandle_t*)data);
    st = ZEKE_FROM_ZK_ERROR(zookeeper_close((zhandle_t*)data));
  }

  if(pool)
    apr_pool_destroy(pool);

  return st;
}

ZEKE_PRIVATE(zk_status_t) close_zhandle_indirect(void *data)
{
  zhandle_t **zhp = (zhandle_t**)data;
  zk_status_t st = APR_SUCCESS;

  if(zhp) {
    if(*zhp) {
      LOG("CLOSE ZHANDLE %pp",*zhp);
      st = close_zhandle(*zhp);
    }
    *zhp = NULL;
  }

  return st;
}

ZEKE_PRIVATE(void) update_zk_info(zhandle_t *zh, int type, int state, const char *path,
                      void *data)
{
  zhandle_t *zh_state = zh;
  init_context_s *ctx = (init_context_s*)data;
  zeke_connection_t *conn = ctx->connection;
  const clientid_t *id = NULL;
  apr_hash_index_t *hi;
  int update = 0;
  zk_status_t st = APR_SUCCESS;
  apr_pool_t *subpool = NULL;
  int run_session_started_hook = 0;

  if(type == ZOO_SESSION_EVENT) {
    if(state == ZOO_CONNECTED_STATE) {
      zeke_connection_flags_clear(conn,ZEKE_FLAG_CONNECTION_STARTING);
      id = zoo_client_id(zh);
      if(id) {
        if((st = apr_pool_userdata_set(id,ZEKE_CLIENT_ID_KEY,NULL,ctx->pool)) != APR_SUCCESS)
          zeke_fatal("apr_pool_userdata_set/" ZEKE_CLIENT_ID_KEY,st);
        update++;
      } else
        LOG("UPDATE CALLBACK: should i have set clientid to %lx",(unsigned long)id);
      if(zh) {
        if((st = apr_pool_userdata_set(zh,ZEKE_ZHANDLE_KEY,NULL,ctx->pool)) != APR_SUCCESS)
          zeke_fatal("apr_pool_userdata_set/" ZEKE_ZHANDLE_KEY,st);
        update++;
      } else
        LOG("UPDATE CALLBACK: should i have set zhandle to %lx",(unsigned long)zh);
    } else if(state == ZOO_EXPIRED_SESSION_STATE || state == ZOO_AUTH_FAILED_STATE) {
      zeke_connection_flags_clear(conn,ZEKE_FLAG_CONNECTION_STARTING);
      zeke_connection_flags_set(conn,ZEKE_FLAG_CONNECTION_CLOSED);
      if(1) {
        if((st = apr_pool_userdata_set(NULL,ZEKE_CLIENT_ID_KEY,NULL,ctx->pool)) != APR_SUCCESS)
          zeke_fatal("apr_ppol_userdata_set[clear]/" ZEKE_CLIENT_ID_KEY,st);
        id = NULL;
        update++;
      }
      if(1) {
        if((st = apr_pool_userdata_set(NULL,ZEKE_ZHANDLE_KEY,NULL,ctx->pool)) != APR_SUCCESS)
          zeke_fatal("apr_pool_userdata_set[clear]/" ZEKE_ZHANDLE_KEY,st);
        zh_state = NULL;
        update++;
      }
    }
  }

  if(ctx->session_pool == NULL) {
    st = ZK_POOL_CREATE(&ctx->session_pool,global_io_pool);
    zeke_pool_tag(ctx->session_pool,"zookeeper connection session context pool");
    apr_pool_cleanup_register(ctx->session_pool,&ctx->session_pool,zk_indirect_wipe,
                              apr_pool_cleanup_null);
    assert(st == APR_SUCCESS);
    run_session_started_hook++;
  }

  if(update)
    zeke_connection_state_update(conn);

  if(apr_hash_count(conn->watchers) > 0)
    subpool = zeke_dispatch_subpool_create();

  for(hi = apr_hash_first(subpool,conn->watchers);
                                               hi;
                           hi = apr_hash_next(hi))
  {
    ZEKE_MAY_ALIAS(zeke_watcher_fn) watcher;
    apr_hash_this(hi, NULL, NULL, (void**)&watcher);
    assert(watcher != NULL);
    zeke_watcher_cb_data_t *cbd = zeke_watcher_cb_data_create_ex(conn,NULL,subpool,NULL);
    assert(cbd != NULL);
    zeke_watcher_cb_data_context_set(cbd,cbd->pool);
    cbd->type = type;
    cbd->state = state;
    cbd->path = path;
    zeke_pool_tag(cbd->pool,"initial connection watcher pool");
    if(id && (st = apr_pool_userdata_set(id,ZEKE_CLIENT_ID_KEY,NULL,cbd->pool)) != APR_SUCCESS)
      zeke_fatal("apr_pool_userdata_set/" ZEKE_CLIENT_ID_KEY,st);
    if(zh_state && (st = apr_pool_userdata_set(zh_state,ZEKE_ZHANDLE_KEY,NULL,cbd->pool)) != APR_SUCCESS)
      zeke_fatal("apr_pool_userdata_set/" ZEKE_ZHANDLE_KEY,st);
    watcher(cbd);
    zeke_watcher_cb_data_destroy(cbd);
  }
  if(subpool != NULL)
    apr_pool_destroy(subpool);

  if(run_session_started_hook) {
    if((conn->flags & ZEKE_FLAG_CONNECTION_HOOKS_SORTED) == 0) {
      zeke_hook_sort_all(conn->pool,NULL);
      zeke_connection_flags_set(conn,ZEKE_FLAG_CONNECTION_HOOKS_SORTED);
    }
    zeke_connection_run_session_started_hook(conn,ctx->session_pool);
  }
}

/* returns the session memory pool associated with a zhandle. */
ZEKE_PRIVATE(apr_pool_t*) zeke_zhandle_pool(zhandle_t *zh)
{
  const init_context_s *ctx;

  assert(zh != NULL);
  ctx = zoo_get_context(zh);
  if(ctx != NULL)
    return ctx->session_pool;
  return NULL;
}

#ifdef LIBZEKE_USE_LIBEVENT
ZEKE_PRIVATE(void) io_event_maint(evutil_socket_t fd, short flags, void *p)
{
  zk_status_t st;
  static int in_event_maint = 0;
  struct io_event_state *state = (struct io_event_state*)p;

  ZEKE_MAGIC_CHECK(state, ZEKE_IO_EVENT_STATE_MAGIC);
  if(in_event_maint)
    return;

  in_event_maint++;
  if(state->api == ZEKE_EVENT_API_EXTERN && (state->got_break || state->got_exit)) {
    if(state->maint_ev)
      event_del(state->maint_ev);
    in_event_maint--;
    return;
  }
  in_io_loop++;
  if(state->ev && state->conn && (st = zeke_connection_maintenance(state->conn)) != APR_SUCCESS) {
    state->st = st;
    if(!state->got_break && state->api == ZEKE_EVENT_API_INTERN)
      zeke_io_event_loopbreak_ex(state);
  }
  in_io_loop--;
  in_event_maint--;
}

ZEKE_PRIVATE(void) io_event(evutil_socket_t fd, short flags, void *p)
{
  struct io_event_state *state = (struct io_event_state*)p;
  unsigned events = 0;
  struct event_base *base;

  ZEKE_MAGIC_CHECK(state, ZEKE_IO_EVENT_STATE_MAGIC);

  assert(state != NULL);
  assert(state->ev != NULL);
  assert(state->conn != NULL);
  assert(state->conn->sock != NULL);

  ZEKE_MAGIC_CHECK(state, ZEKE_IO_EVENT_STATE_MAGIC);
  if(state->api == ZEKE_EVENT_API_EXTERN && (state->got_break || state->got_exit)) {
    if(state->maint_ev)
      event_del(state->maint_ev);
    if(state->ev)
      event_del(state->ev);
    return;
  }

  in_io_loop++;
  base = event_get_base(state->ev);
#ifdef DEBUGGING
  assert(base == zeke_event_base_get(state->pool));
#endif

  if(flags & EV_TIMEOUT) {
    state->st = APR_TIMEUP;
    if(state->api == ZEKE_EVENT_API_INTERN && !state->got_break) {
      if(state->ev)
        event_del(state->ev);
      zeke_io_event_loopbreak_ex(state);
      goto io_event_end;
    }
  }

  if(flags & EV_READ)
    events |= ZOOKEEPER_READ;
  if(flags & EV_WRITE)
    events |= ZOOKEEPER_WRITE;
  if(events || (flags & EV_TIMEOUT)) {
    zk_status_t st = APR_FROM_ZK_ERROR(zookeeper_process(state->zh,events));
    switch(APR_TO_ZK_ERROR(st)) {
    case ZINVALIDSTATE:
      state->zh = state->wzh = NULL;
      do {
        if(state->conn) {
          assert(state->conn->sock != NULL);
          apr_pool_destroy(apr_socket_pool_get(state->conn->sock));
          assert(state->conn->sock == NULL);
        }
        state->st = st;
        if(!state->got_break) {
          if(state->api == ZEKE_EVENT_API_INTERN)
            zeke_io_event_loopbreak_ex(state);
          else if(state->api == ZEKE_EVENT_API_EXTERN) {
            if(state->ev)
              event_del(state->ev);
            zeke_io_event_reconnect(state);
          }
        }
        goto io_event_end;
      } while(0);
      break;
    case ZCONNECTIONLOSS:
    case ZSESSIONEXPIRED:
      state->zh = state->conn->zh;
      state->st = st;
      if(state->conn && state->conn->sock) {
        apr_pool_destroy(apr_socket_pool_get(state->conn->sock));
        state->started = -1;
      }
      if(!state->got_break) {
        perm_client.client_id = 0;
        if(state->api == ZEKE_EVENT_API_INTERN)
          zeke_io_event_loopbreak_ex(state);
        else if(state->api == ZEKE_EVENT_API_EXTERN) {
          if(state->ev)
            event_del(state->ev);
          zeke_io_event_reconnect(state);
        }
      }
      goto io_event_end;
      break; /* keep compiler happy */
    case ZNOTHING:
      st = APR_SUCCESS;
    default:
      if(zeke_terminating) {
        st = APR_SUCCESS;
        break;
      }
      if(state->conn && state->conn->sock) {
        apr_interval_time_t new_timeout = APR_TIME_C(-1);
        short persist = 0;
        int pending = 1;
        apr_uint32_t new_flags = 0, flags = 0;

        assert(zeke_socket_event_type_get(state->conn->sock,&flags,&persist) == APR_SUCCESS);
        st = zeke_connection_socket_maint(state->conn,&new_flags,&new_timeout);
        if(state->api == ZEKE_EVENT_API_EXTERN) {
          state->st = st;
          if(st == APR_EOF || !state->conn->sock) {
            zeke_io_event_reconnect(state);
            goto io_event_end;
          }
        }

        if(st == APR_EOF) {
          ERR("EOF");
          st = APR_TIMEUP;
          break;
        }
        state->st = st;
        assert(state->conn->sock != NULL);
        if(!ZEKE_STATUS_IS_OKAY(st)) {
          zeke_apr_error("zeke_connection_socket_maint",st);
          state->st = st;
          break;
        }
        if(new_flags != flags) {
          st = zeke_socket_event_type_set(state->conn->sock,new_flags,persist);
          if(!ZEKE_STATUS_IS_OKAY(st)) {
            zeke_apr_error("zeke_socket_event_type_set",st);
            break;
          }
        }
        pending = zeke_socket_event_pending(state->conn->sock,NULL);
        if(persist || (new_timeout != state->cur_timeout) || !pending) {
          LOG("TIMEOUT CHANGE %" APR_UINT64_T_FMT "ms -> %"APR_UINT64_T_FMT "ms",
              state->cur_timeout / APR_TIME_C(1000), new_timeout / APR_TIME_C(1000));
          if(pending)
            zeke_socket_event_del(state->conn->sock);
          st = zeke_socket_event_add(state->conn->sock,new_timeout);
          if(!ZEKE_STATUS_IS_OKAY(st)) {
            zeke_apr_error("zeke_socket_event_add",st);
            break;
          }
          state->cur_timeout = new_timeout;
        }
        goto io_event_end;
      }
      break;
    }
    state->st = st;
    if(state->api == ZEKE_EVENT_API_INTERN && !state->got_break)
      zeke_io_event_loopbreak_ex(state);
  }

io_event_end:
  in_io_loop--;
}

static zk_status_t io_event_loop(zeke_connection_t *conn,
                                 apr_uint32_t *numloops,
                                 apr_interval_time_t *timeout,
                                 unsigned *reconnect_attempts)
{
  struct io_event_state *state;
  short evflags = 0;
  struct event *ev = NULL;
  apr_pool_t *pool = zeke_connection_pool_get(conn);
  zk_status_t st = APR_SUCCESS;
  unsigned default_retries = 10;
  int newev=0, fd = -1;
  apr_time_t update, last_update = APR_TIME_C(0);
  struct event_base *base = NULL;

  if(!reconnect_attempts)
    reconnect_attempts = &default_retries;

  if(global_io_pool == NULL) {
    global_io_pool = zeke_root_subpool_create();
    zeke_pool_tag(global_io_pool,"ZEKE GLOBAL I/O Pool");
    zeke_pool_cleanup_indirect(global_io_pool,global_state);
  }
  if(pool == NULL)
    pool = global_io_pool;

  if(conn->pool && zeke_event_base_exists(conn->pool))
    base = get_event_base(conn->pool);

  state = io_current_event_state_create(pool,base);

  ZEKE_MAGIC_CHECK(state, ZEKE_IO_EVENT_STATE_MAGIC);
  state->start = apr_time_now();
  state->api = ZEKE_EVENT_API_INTERN;
  state->conn = conn;
  state->elapsed = APR_TIME_C(0);
  state->timeout = timeout;
  state->reconnect_attempts = reconnect_attempts;
  state->zh = conn->zh;
  state->wzh = NULL;
  state->client = NULL;
  state->ev = ev;
  state->started = 0;
  state->cur_timeout = APR_TIME_C(0);

  assert(base != NULL);

  if(apr_pool_userdata_get((void**)&state->client,ZEKE_CLIENT_ID_KEY,pool) == APR_SUCCESS && state->client) {
    memcpy(&perm_client,state->client,sizeof(perm_client));
  }
  if(!state->zh && apr_pool_userdata_get((void**)&state->zh,ZEKE_ZHANDLE_KEY,pool) == APR_SUCCESS && state->zh) {
    assert(conn->zh == state->zh);
    state->wzh = conn->zh;
  }

  if(conn->zh && zeke_connection_is_closed(conn))
    return APR_FROM_ZK_ERROR(ZENOSESSION);
  apr_pool_pre_cleanup_register(conn->pool,zeke_indirect_make(&state->conn,state->pool),
                                zeke_indirect_deref_wipe);
  if(conn->sock)
    apr_pool_pre_cleanup_register(apr_socket_pool_get(conn->sock),
                                  zeke_indirect_make(&state->ev,state->pool),
                                  zeke_indirect_deref_wipe);


  assert(io_current_event_state_set(state) == APR_SUCCESS);
  for(; (!numloops || *numloops) && st == APR_SUCCESS && state->started != -1;
        (state->started != -1 && numloops ? (*numloops)++ : 0))
  {
    apr_uint64_t state_updates = 0;
    apr_interval_time_t ev_timeout;
    update = apr_time_now();

    if(!conn) {
      if(state->pool)
        zeke_io_event_loop_destroy(state);
      return APR_FROM_ZK_ERROR(ZENOSESSION);
    }
    if (!last_update)
      last_update = update;
    if (zeke_terminating)
      break;
    if(timeout) {
      if(state->elapsed > (update - state->start))
        *timeout = 0;
      else
        *timeout -= (update - state->start) - state->elapsed;
      state->elapsed = update - state->start;
      if(*timeout <= 0)
        break;
    } else
      state->elapsed = update - state->start;

    if(in_io_loop == 0)
      in_io_loop++;

    /* loop forever w/out guidance */
    if(reconnect_attempts == &default_retries && default_retries < 10)
      default_retries = 10;

    ev_timeout = (timeout ? *timeout : APR_TIME_C(-1));

    if(state->zh || state->wzh)
      state_updates = zeke_connection_state_nupdates(conn);

    if(state_updates) {
      apr_pool_t *cpool = zeke_connection_context_pool_get(conn);
      if(cpool) {
        apr_pool_userdata_get((void**)&state->wzh,ZEKE_ZHANDLE_KEY,cpool);
        if(apr_pool_userdata_get((void**)&state->client,ZEKE_CLIENT_ID_KEY,cpool) == APR_SUCCESS) {
          if(state->client && state->client->client_id != perm_client.client_id)
            memcpy(&perm_client,state->client,sizeof(perm_client));
        }
      } else
        state->wzh = NULL;

      LOG("state(1) updated, wzh=%lx, zh=%lx, count = %" APR_UINT64_T_FMT,
                (unsigned long)state->wzh,(unsigned long)state->zh,state_updates);

      if((!state->wzh && state->zh) || (state->wzh && state->zh && state->wzh != state->zh)) {
        st = zeke_connection_close(conn);
        assert(state->conn != NULL);
        state->conn = NULL;
        LOG("connection state reset");
        io_state_reset(conn->pool);
        if(st != APR_SUCCESS) {
          zeke_apr_error("zeke_connection_close",st);
          perm_client.client_id = 0;
          state->zh = NULL;
        } else state->zh = conn->zh;
      }

      LOG("state(2) updated, wzh=%lx, zh=%lx, count = %" APR_UINT64_T_FMT,
                (unsigned long)state->wzh,(unsigned long)state->zh,state_updates);
    }

    if(!state->zh && !state->wzh) {
      if(zeke_connection_is_starting(conn) && zeke_connection_is_closed(conn))
        zeke_connection_flags_clear(conn,ZEKE_FLAG_CONNECTION_CLOSED);
      if((st = zeke_connection_start(conn,&perm_client,reconnect_attempts)) != APR_SUCCESS)
        break;
      state->zh = conn->zh;
      state->conn = conn;
    }
    assert(state->zh != NULL);

    switch(state->started) {
    case -1:
      continue;
    case 0:
      if(zeke_connection_is_active(conn)) {
        LOG("INC(new->active) connection pre-established, started to %d",state->started+2);
        state->started += 2;
      } else if(zeke_connection_is_open(conn)) {
        LOG("INC(new->open) started to %d",state->started+1);
        state->started++;
      } else {
        WARN("NO LOOP STARTED CHANGE: %d",state->started);
      }
      break;
    case 1:
      if(!zeke_connection_is_open(conn)) {
        INFO("CONNECTION closed, resetting started (%d->-1)",state->started);
        state->started = -1;
        continue;
      }
      if(zeke_connection_is_active(conn)) {
        LOG("INC(open->active) started to %d",state->started+1);
        state->started++;
      }
      break;
    default:
      if(!zeke_connection_is_active(conn)) {
        INFO("CONNECTION no longer active, resetting started (%d->-1)",state->started);
        state->started = -1;
        continue;
      }
    }
    /* Don't ask zookeeper too often about file descriptors or it will storm */
    if(update - last_update > ZK_STARTUP_POLLTIMER*1000 || !zeke_connection_is_starting(conn)
                                                        || !conn->sock) {
      apr_int16_t interest;
      short newflags = 0;
      apr_interval_time_t req_timeout;

      last_update = update;

      st = zeke_connection_socket_update(conn,NULL,&interest,&req_timeout,NULL);
      if(st != APR_SUCCESS) {
        zeke_apr_error(apr_psprintf(pool,"zookeeper_interest client_id=%" APR_INT64_T_FMT,
                                        (apr_int64_t)perm_client.client_id),st);
        zeke_connection_close(conn);
        assert(state->conn != NULL);
        state->conn = NULL;
        perm_client.client_id = 0;
        st = APR_SUCCESS;
        in_io_loop--;
        state->started = 0;
        continue;
      }
      if (interest & APR_POLLIN)
        newflags |= APR_READ;
      if (interest & APR_POLLOUT)
        newflags |= APR_WRITE;
      if(timeout && (*timeout == APR_TIME_C(-1) || req_timeout < *timeout)) {
        *timeout = ev_timeout = req_timeout;
      } else if(!timeout && req_timeout > APR_TIME_C(0))
        ev_timeout = req_timeout;
      if(conn->sock) {
        int newfd = fd;
        st = apr_os_sock_get(&newfd,conn->sock);
        if(st != APR_SUCCESS) {
          zeke_apr_error(apr_psprintf(pool,"apr_os_socket_get client_id=%" APR_INT64_T_FMT,
                                          (apr_int64_t)perm_client.client_id),st);
          zeke_connection_close(conn);
          assert(state->conn != NULL);
          state->conn = NULL;
          perm_client.client_id = 0;
          st = APR_SUCCESS;
          in_io_loop--;
          state->started = 0;
          continue;
        }
        if(!state->ev || newfd != fd) {
          ev = zeke_socket_event_get(conn->sock);
          if(ev) {
            if(newev == 0) {
              assert(zeke_socket_event_set(conn->sock,io_event,state) == APR_SUCCESS);
              newev = -1;
            }
          } else {
            if(!zeke_event_base_exists(conn->pool)) {
              zeke_event_base_set(base,0,conn->pool);
            }
            st = zeke_socket_event_create(&ev,conn->sock,conn->pool);
            if(st == APR_SUCCESS) {
              newev++;
              st = zeke_socket_event_set(conn->sock,io_event,state);
            }
            if(st != APR_SUCCESS) {
              zeke_apr_eprintf(st,"zeke_socket_event_create client_id=%" APR_INT64_T_FMT,
                                    (apr_int64_t)perm_client.client_id);
              zeke_connection_close(conn);
              assert(state->conn != NULL);
              state->conn = NULL;
              perm_client.client_id = 0;
              in_io_loop--;
              state->started = 0;
              break;
            }
            do {
              zeke_indirect_t *ind;
              apr_pool_t *sp = apr_socket_pool_get(conn->sock);
              for(ind = zeke_indirect_find(&state->ev, state->pool); ind != NULL; ) {
                apr_pool_cleanup_kill(sp,ind,zeke_indirect_deref_wipe);
                assert((void*)zeke_indirect_consume(ind) == (void*)&state->ev);
                ind = zeke_indirect_find(&state->ev, state->pool);
              }

              ind = zeke_indirect_make(&state->ev, state->pool);
              assert(ind != NULL);
              apr_pool_pre_cleanup_register(apr_socket_pool_get(conn->sock),ind,
                                            zeke_indirect_deref_wipe);
            } while(0);
          }
        }
        assert(ev != NULL);
        state->ev = ev;
        if(newev || newfd != fd || newflags != evflags) {
          fd = newfd;
          evflags = newflags;
          LOG("EVENT MODE %d FD=%d ev=%pp sock=%pp",(int)newflags,newfd,ev,conn->sock);
          st = zeke_socket_event_type_set(conn->sock,newflags,1);
          if(st != APR_SUCCESS) {
            zeke_apr_eprintf(st,"zeke_socket_event_type_set fd=%d flags=%d",(int)newfd,newflags);
            abort();
          }
        }
      }
    }
    for(st = APR_EINTR; st == APR_EINTR && conn != NULL;) {
      int i;
      apr_interval_time_t remaining;
      state->st = st;

      if(!state->maint_ev)
        assert(zeke_event_create_ex(&state->maint_ev,0,1,io_event_maint,
                                    state,state->pool) == APR_SUCCESS);
      if(!zeke_generic_event_pending(state->maint_ev,EV_TIMEOUT,&remaining)) {
        assert(zeke_generic_event_add(state->maint_ev,APR_TIME_C(250000)) == APR_SUCCESS);
      }
      if(ev && conn->sock && fd > -1 && !zeke_socket_event_pending(conn->sock,&remaining)) {
        state->cur_timeout = ev_timeout;
        st = zeke_socket_event_add(conn->sock,ev_timeout);
        if(st != APR_SUCCESS) {
          zeke_apr_error("socket",st);
          apr_pool_destroy(apr_socket_pool_get(conn->sock));
          state->started = 0;
          st = APR_EINTR;
          break;
        }
      }
      i = event_base_loop(base,0);
      if(state->maint_ev)
        event_del(state->maint_ev);
      conn = state->conn;
      switch(i) {
      case -1:
        st = APR_FROM_OS_ERROR(errno);
        if(st == APR_SUCCESS)
          st = APR_EINVAL;
        break;
      case 0:
      case 1:
        do {
          int code = 0;
          if(zeke_io_event_loop_gotexit(state,&code)) {
            if(zeke_terminating || check_shutdown(1,NULL,&code)) {
              if(!zeke_terminating)
                zeke_terminating++;
              if(code && !zeke_exit_code)
                zeke_exit_code = code;
            }
            st = state->st = APR_SUCCESS;
          } else if(zeke_io_event_loop_gotbreak(state))
            st = state->st;
        } while(0);
        if(APR_STATUS_IS_ZEKE_ERROR(st)) {
          switch(APR_TO_ZK_ERROR(st)) {
            case ZCONNECTIONLOSS:
            case ZSESSIONEXPIRED:
              perm_client.client_id = 0;
            case ZINVALIDSTATE:
              state->started = 0;
              st = APR_TIMEUP;
              break;
            default:
              break;
          }
        }
        break;
      default:
        break;
      }
      if(zeke_terminating) {
        st = APR_SUCCESS;
        break;
      }
    }

    if(st == APR_EINTR)
      st = APR_SUCCESS;
    else if(!zeke_terminating)
      apr_proc_other_child_refresh_all(APR_OC_REASON_RUNNING);

    if(zeke_terminating) {
      st = APR_SUCCESS;
      break;
    }
    if(st == APR_TIMEUP) {
      st = APR_SUCCESS;
      in_io_loop--;
    }
  }
  state->elapsed += (apr_time_now() - state->start) - state->elapsed;
  if(timeout) {
    if(*timeout <= 0 && st == APR_SUCCESS)
      st = APR_TIMEUP;
    *timeout = state->elapsed;
  }

  if(state && state->pool)
    zeke_io_event_loop_destroy(state);

  if(zeke_terminating) {
    zeke_connection_close(conn);
    perm_client.client_id = 0;
    LOG("EXIT: %d",zeke_exit_code);
    exit(zeke_exit_code);
  }
  in_io_loop = 0;
  return st;
}

#endif /* LIBZEKE_USE_LIBEVENT */

static zk_status_t io_loop(zeke_connection_t *conn,
                           apr_uint32_t *numloops,
                           apr_interval_time_t *timeout,
                           unsigned *reconnect_attempts)
{
  ZEKE_MAY_ALIAS(zhandle_t) *zh = conn->zh;
  ZEKE_MAY_ALIAS(zhandle_t) *wzh = NULL;
  apr_pool_t *pool = zeke_connection_pool_get(conn);
  ZEKE_MAY_ALIAS(apr_pollfd_t) *pollfd = NULL;
  ZEKE_MAY_ALIAS(apr_pollset_t) *pollset = NULL;
  zk_status_t st = APR_SUCCESS;
  apr_time_t start = apr_time_now();
  apr_interval_time_t elapsed = 0;
  apr_time_t update = APR_TIME_C(0);
  apr_time_t last_update = APR_TIME_C(0);
  ZEKE_MAY_ALIAS(clientid_t) *client = NULL;
  unsigned default_retries = 10;
  int started = 0, intr = 0;

  if(!reconnect_attempts)
    reconnect_attempts = &default_retries;

  if(global_io_pool == NULL) {
    global_io_pool = zeke_root_subpool_create();
    zeke_pool_tag(global_io_pool,"ZEKE GLOBAL I/O Pool");
  } else if (pool == NULL)
    apr_pool_clear(global_io_pool);
  if(pool == NULL)
    pool = global_io_pool;

  if(apr_pool_userdata_get((void**)&client,ZEKE_CLIENT_ID_KEY,pool) == APR_SUCCESS && client) {
    memcpy(&perm_client,client,sizeof(perm_client));
  }
  if(!zh && apr_pool_userdata_get((void**)&zh,ZEKE_ZHANDLE_KEY,pool) == APR_SUCCESS && zh) {
    assert(conn->zh == zh);
    wzh = conn->zh;
  }

  if(apr_pool_userdata_get((void**)&pollfd,ZEKE_POLLFD_KEY,pool) != APR_SUCCESS || !pollfd) {
    pollfd = apr_pcalloc(pool,sizeof(apr_pollfd_t));
    assert(pollfd != NULL);
    assert(apr_pool_userdata_setn(pollfd,ZEKE_POLLFD_KEY,NULL,pool) == APR_SUCCESS);
  }

  if(pollset == NULL) {
    if(apr_pool_userdata_get((void**)&pollset,ZEKE_POLLSET_KEY,pool) != APR_SUCCESS || !pollset) {
      assert(apr_pollset_create(&pollset,10,pool,ZEKE_DEFAULT_POLLSET_FLAGS) == APR_SUCCESS);
      assert(apr_pool_userdata_setn(pollset,ZEKE_POLLSET_KEY,NULL,pool) == APR_SUCCESS);
    }
  }

  if(conn->zh && zeke_connection_is_closed(conn))
    return APR_FROM_ZK_ERROR(ZENOSESSION);

  for(; (!numloops || *numloops) && st == APR_SUCCESS && started != -1 && intr >= 0;
        (started != -1 && numloops ? (*numloops)++ : 0)) {
    int events = 0;
    apr_int32_t pending = 0;
    apr_uint64_t state_updates = 0;
    apr_interval_time_t poll_timeout;
    apr_socket_t *sock = conn->sock;
    apr_size_t npollfds = 1;
    const apr_pollfd_t *output_pollfds;

    update = apr_time_now();
    if(!last_update)
      last_update = update;

    if(zeke_terminating)
      break;

    if(in_io_loop == 0)
      in_io_loop++;

    /* loop forever w/out guidance */
    if(reconnect_attempts == &default_retries && default_retries < 10)
      default_retries = 10;

    LOG("  LOOP remaining attempts=%u",*reconnect_attempts);
    if(timeout) {
      if(elapsed > (update - start))
        *timeout = 0;
      else
        *timeout -= (update - start) - elapsed;
      elapsed = update - start;
      if(*timeout <= 0)
        break;
    } else
      elapsed = update - start;

    poll_timeout = (timeout ? *timeout : APR_TIME_C(-1));


    if(zh || wzh)
      state_updates = zeke_connection_state_nupdates(conn);

    if(state_updates) {
      apr_pool_t *cpool = zeke_connection_context_pool_get(conn);
      if(cpool) {
        apr_pool_userdata_get((void**)&wzh,ZEKE_ZHANDLE_KEY,cpool);
        if(apr_pool_userdata_get((void**)&client,ZEKE_CLIENT_ID_KEY,cpool) == APR_SUCCESS) {
          if(client && client->client_id != perm_client.client_id)
            memcpy(&perm_client,client,sizeof(perm_client));
        }
      } else
        wzh = NULL;

      LOG("state(1) updated, wzh=%lx, zh=%lx, count = %" APR_UINT64_T_FMT,
                (unsigned long)wzh,(unsigned long)zh,state_updates);

      if((!wzh && zh) || (wzh && zh && wzh != zh)) {
        pollfd->desc.s = NULL;
        st = zeke_connection_close(conn);
        LOG("connection state reset");
        io_state_reset(conn->pool);
        if(st != APR_SUCCESS) {
          zeke_apr_error("zeke_connection_close",st);
          perm_client.client_id = 0;
          zh = NULL;
        } else zh = conn->zh;
      }

      LOG("state(2) updated, wzh=%lx, zh=%lx, count = %" APR_UINT64_T_FMT,
                (unsigned long)wzh,(unsigned long)zh,state_updates);
    }

    if(!zh && !wzh) {
      if(zeke_connection_is_starting(conn) && zeke_connection_is_closed(conn))
        zeke_connection_flags_clear(conn,ZEKE_FLAG_CONNECTION_CLOSED);
      if((st = zeke_connection_start(conn,&perm_client,reconnect_attempts)) != APR_SUCCESS)
        break;
      pollfd->desc.s = NULL;
      zh = conn->zh;
      assert(zh != NULL);
    }

    assert(zh != NULL);

    switch(started) {
    case -1:
      continue;
    case 0:
      if(zeke_connection_is_active(conn)) {
        LOG("INC(new->active) connection pre-established, started to %d",started+2);
        started += 2;
      } else if(zeke_connection_is_open(conn)) {
        LOG("INC(new->open) started to %d",started+1);
        started++;
      } else {
        WARN("NO LOOP STARTED CHANGED: %d",started);
      }
      break;
    case 1:
      if(!zeke_connection_is_open(conn)) {
        INFO("CONNECTION closed, resetting started (%d->-1)",started);
        started = -1;
        continue;
      }
      if(zeke_connection_is_active(conn)) {
        INFO("INC(open->active) started to %d",started+1);
        started++;
      }
      break;
    default:
      if(!zeke_connection_is_active(conn)) {
        INFO("CONNECTION no longer active, resetting started (%d->-1)",started);
        started = -1;
        continue;
      }
    }

    /* Don't ask zookeeper too often about file descriptors or it will storm */
    if(update - last_update > ZK_STARTUP_POLLTIMER*1000 || !zeke_connection_is_starting(conn)
                                                        || !conn->sock
                                                        || pollfd->desc.s != conn->sock) {
      last_update = update;
      st = zeke_connection_pollfd_set(conn,pollfd,&npollfds,&poll_timeout);
      if(st != APR_SUCCESS) {
        zeke_apr_error(apr_psprintf(pool,"zeke_connection_pollfd_set client_id=%" APR_INT64_T_FMT,
                                        (apr_int64_t)perm_client.client_id),st);
        zeke_connection_close(conn);
        perm_client.client_id = 0;
        st = APR_SUCCESS;
        in_io_loop--;
        started = 0;
        npollfds = 0;
        continue;
      }
    }

    if(npollfds > 0) {
      st = apr_pollset_add(pollset,pollfd);
      if(st != APR_SUCCESS)
        zeke_fatal("pollset",st);
    }
    for(st = APR_EINTR; st == APR_EINTR;) {
      apr_int32_t num = 0;
      output_pollfds = NULL;
      st = apr_pollset_poll(pollset,poll_timeout,&num,&output_pollfds);
      if(zeke_terminating) {
        if(npollfds > 0)
          apr_pollset_remove(pollset,pollfd);
        st = APR_SUCCESS;
        break;
      }
      if(st != APR_EINTR) {
        apr_pollset_remove(pollset,pollfd);
        if(num > 0)
          pending += num;
        break;
      } else {
        if(interrupt_handler(conn))
          intr = interrupt_handler(conn);
        else {
          apr_proc_other_child_refresh_all(APR_OC_REASON_RUNNING);
          intr = -1;
        }
        if(zeke_terminating || intr >= 0) {
          if(npollfds > 0)
            apr_pollset_remove(pollset,pollfd);
          break;
        }
      }
    }

    if(st == APR_EINTR)
      st = APR_SUCCESS;
    else if(!zeke_terminating) {
      apr_proc_other_child_refresh_all(APR_OC_REASON_RUNNING);
    }

    if(zeke_terminating) {
      st = APR_SUCCESS;
      break;
    }
    if(st == APR_TIMEUP) {
      st = APR_SUCCESS;
      in_io_loop--;
      continue;
    }

    if(st != APR_SUCCESS) {
      zeke_apr_error(apr_psprintf(pool,"apr_poll with %lx socket",(unsigned long)sock),st);
      st = zeke_connection_close(conn);
      perm_client.client_id = 0;
      started = 0;
      in_io_loop--;
      continue;
    }
    if(pending) {
      const apr_pollfd_t *p;

      for(p = output_pollfds; pending > 0; pending--) {
        if(p->client_data == conn) {
          if(p->rtnevents & APR_POLLIN)
            events |= ZOOKEEPER_READ;
          if(p->rtnevents & APR_POLLOUT)
            events |= ZOOKEEPER_WRITE;
        }
        p++;
      }
    }
    if(events) {
      st = APR_FROM_ZK_ERROR(zookeeper_process(zh,events));
      switch(APR_TO_ZK_ERROR(st)) {
      case ZINVALIDSTATE:
        pollfd->desc.s = NULL;
        zeke_connection_close(conn);
        assert(conn->zh == NULL);
        started = 0;
        st = APR_SUCCESS;
        break;
      case ZCONNECTIONLOSS:
      case ZSESSIONEXPIRED:
        pollfd->desc.s = NULL;
        zeke_connection_close(conn);
        zh = conn->zh;
        perm_client.client_id = 0;
        started = 0;
      case ZNOTHING:
        st = APR_SUCCESS;
        break;
      default:
        break;
      }
    }
  }

  elapsed += (apr_time_now() - start) - elapsed;
  if(timeout) {
    if(*timeout <= 0 && st == APR_SUCCESS)
      st = APR_TIMEUP;
    *timeout = elapsed;
  }

  if(zeke_terminating) {
    zeke_connection_close(conn);
    perm_client.client_id = 0;
    LOG("EXIT: %d",zeke_exit_code);
    exit(zeke_exit_code);
  }
  in_io_loop = 0;
  return st;
}

#ifdef LIBZEKE_USE_LIBEVENT
static inline struct event_base *get_event_base(apr_pool_t *p)
{
  struct event_base *base;
  if(!global_io_pool)
    global_io_pool = zeke_root_subpool_create();

  if(p && zeke_event_base_exists(p))
    return zeke_event_base_get(p);

  base = zeke_event_base_get(global_io_pool);
  assert(base != NULL);
  if(p && p != global_io_pool)
    zeke_event_base_set(base,0,p);

  assert(base != NULL);
  return base;
}


ZEKE_API(zk_status_t) zeke_event_loopn(zeke_connection_t *conn,
                                       struct event_base *base,
                                       apr_uint32_t numloops,
                                       apr_interval_time_t *timeout,
                                       unsigned *reconnect_attempts)
{
  zk_status_t st;
  if(base && !zeke_event_base_exists(conn->pool))
    zeke_event_base_set(base,0,conn->pool);
  st = io_event_loop(conn,&numloops,timeout,reconnect_attempts);
  return st;
}

ZEKE_API(zk_status_t) zeke_event_loop(zeke_connection_t *conn,
                                      struct event_base *base,
                                      apr_interval_time_t *timeout,
                                      unsigned *reconnect_attempts)
{
  zk_status_t st;
  if(base && !zeke_event_base_exists(conn->pool))
    zeke_event_base_set(base,0,conn->pool);
  st = io_event_loop(conn,NULL,timeout,reconnect_attempts);
  return st;
}

#endif /* LIBZEKE_USE_LIBEVENT */

ZEKE_API(zk_status_t) zeke_io_loopn(zeke_connection_t *conn,
                                    apr_uint32_t numloops,
                                    apr_interval_time_t *timeout,
                                    unsigned *reconnect_attempts)
{
  return io_loop(conn,&numloops,timeout,reconnect_attempts);
}

ZEKE_API(zk_status_t) zeke_io_loop(zeke_connection_t *conn,
                                     apr_interval_time_t *timeout,
                                     unsigned *reconnect_attempts)
{
  return io_loop(conn,NULL,timeout,reconnect_attempts);
}
