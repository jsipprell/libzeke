#include "internal_config.h"
#include "internal.h"
#include "connection.h"
#include "util.h"
#include "io.h"

#include "event_private.h"
#include "libzeke_indirect.h"

#ifndef ZEKE_RECONNECT_ATTEMPTS
#define ZEKE_RECONNECT_ATTEMPTS 3
#endif

#if defined(LIBZEKE_USE_LIBEVENT) && defined(HAVE_UNISTD_H)
#include <unistd.h>
#endif

#ifndef LIBZEKE_USE_LIBEVENT
#ifndef ZK_POOL_CREATE
#define ZK_POOL_CREATE apr_pool_create
#endif

ZEKE_API(int) zeke_is_libevent_enabled(const char **version)
{
  return 0;
}
#else /* libevent api */

#ifndef ZK_POOL_CREATE
#define ZK_POOL_CREATE zeke_event_base_pool_create
#endif

#ifndef LIBZEKE_EVENT_BASE_POOL_KEY
#define LIBZEKE_EVENT_BASE_POOL_KEY "libzeke:pool:event_base"
#endif

#ifndef LIBZEKE_EVENT_POOL_KEY
#define LIBZEKE_EVENT_POOL_KEY "libzeke:pool:event"
#endif

#define ZEKE_EVENT_KEY(type) LIBZEKE_EVENT_POOL_KEY "(" APR_STRINGIFY(type) ")"

typedef struct {
  apr_uint32_t magic;
#define ZEKE_LIBEVENT_API_MAGIC 0xbe7ff124
  apr_pool_t *pool;
  const char *zkhosts;
  struct io_event_state *state;
  zeke_runloop_callback_fn setup;
  apr_uint32_t flags;
  void *userdata;
  apr_interval_time_t reconnect_delay;
  unsigned reconnect_attempts;
  apr_time_t last_attempt;
  clientid_t client;
} zeke_event_api_t;

extern volatile int in_io_loop;
extern volatile int zeke_terminating;

static inline apr_status_t get_libevent_error(void)
{
  apr_status_t st = apr_get_os_error();
  return (st == APR_SUCCESS ? APR_EGENERAL : st);
}

ZEKE_API(int) zeke_is_libevent_enabled(const char **version)
{
  if(version)
    *version = LIBZEKE_LIBEVENT_VERSION;
  return 1;
}

static
zeke_event_api_t *get_event_api(apr_pool_t *p)
{
  zeke_event_api_t *api = NULL;
  assert(p != NULL);

  if(apr_pool_userdata_get((void**)&api,ZEKE_EVENT_KEY(api),p) != APR_SUCCESS ||
                                                    api == NULL) {
    apr_pool_t *pool;

    ZK_POOL_CREATE(&pool,p);
    assert(pool != NULL);
    zeke_pool_tag(pool,"libevent low-level API");
    api = apr_pcalloc(pool,sizeof(*api));
    assert(api != NULL);
    api->magic = ZEKE_LIBEVENT_API_MAGIC;
    api->pool = pool;
    api->reconnect_attempts = ZEKE_RECONNECT_ATTEMPTS;
    assert(apr_pool_userdata_setn(api,ZEKE_EVENT_KEY(api),NULL,p) == APR_SUCCESS);
    assert(apr_pool_userdata_setn(api,ZEKE_EVENT_KEY(api),NULL,pool) == APR_SUCCESS);
  }

  return api;
}

static
zeke_event_api_t *get_event_loop_api(const struct io_event_state *state)
{
  zeke_event_api_t *api;
  ZEKE_MAGIC_CHECK(state, ZEKE_IO_EVENT_STATE_MAGIC);
  api = get_event_api(state->pool);
  if(api->state == NULL) {
    api->state = (struct io_event_state*)state;
    apr_pool_pre_cleanup_register(state->pool,&api->state,zeke_indirect_wipe);
  }
  return api;
}

static
int has_event_loop_api(const struct io_event_state *state,
                       zeke_event_api_t **apip)
{
  zeke_event_api_t *api;
  ZEKE_MAGIC_CHECK(state, ZEKE_IO_EVENT_STATE_MAGIC);

  if(apr_pool_userdata_get((void**)&api,ZEKE_EVENT_KEY(api),state->pool) == APR_SUCCESS &&
                                                                    api != NULL) {
    if(apip)
      *apip = api;
    return 1;
  }
  return 0;
}

static
zk_status_t reschedule_maintenance_ex(struct io_event_state *state,
                                     event_callback_fn maint_fn,
                                    void *data, apr_interval_time_t when)
{
  zeke_event_api_t *api = NULL;
  zk_status_t st = ZEOK;

  ZEKE_MAGIC_CHECK(state, ZEKE_IO_EVENT_STATE_MAGIC);
  if(!maint_fn) {
    maint_fn = io_event_maint;
    if(!data)
      data = state;
    else
      api = (zeke_event_api_t*)data;
    if(!api)
      assert(has_event_loop_api(state,&api));
  } else if(!data) {
    assert(has_event_loop_api(state,&api));
    data = api;
  } else {
    api = (zeke_event_api_t*)data;
    ZEKE_MAGIC_CHECK(api, ZEKE_LIBEVENT_API_MAGIC);
}

  if(state->maint_ev == NULL) {
    zk_status_t st;
    ZEKE_MAGIC_CHECK(api, ZEKE_LIBEVENT_API_MAGIC);
    st = zeke_event_create_ex(&state->maint_ev,0,0,maint_fn,data,api->pool);
    if(!ZEKE_STATUS_IS_OKAY(st))
      zeke_fatal("cannot create maintenance event",st);
    assert(state->maint_ev != NULL);
  } else event_del(state->maint_ev);
  if(when < ZEKE_IMMEDIATE)
    when = ZEKE_IMMEDIATE;
  assert((st = zeke_generic_event_callback_set(state->maint_ev,maint_fn,data)) ==
               APR_SUCCESS);
  assert((st = zeke_generic_event_add(state->maint_ev,when)) == APR_SUCCESS);
  return st;
}

#define reschedule_maintenance(s,fn) \
        (void)reschedule_maintenance_ex((s),(fn),NULL,ZEKE_IMMEDIATE)

ZEKE_PRIVATE(void)
io_event_initial(evutil_socket_t fd, short what, void *apip)
{
  zeke_event_api_t *api = (zeke_event_api_t*)apip;
  struct io_event_state *state;
  struct event *ev;
  zeke_connection_t *conn;
  apr_socket_t *sock;
  zk_status_t st;
  apr_uint32_t flags = 0;
  apr_int16_t poll_events = 0;
  apr_interval_time_t timeout = APR_TIME_C(-1);
  apr_time_t now = apr_time_now();

  ZEKE_MAGIC_CHECK(api, ZEKE_LIBEVENT_API_MAGIC);
  ZEKE_MAGIC_CHECK(api->state, ZEKE_IO_EVENT_STATE_MAGIC);

  state = api->state;
  conn = state->conn;

  if(state->got_break || state->got_exit) {
    if(state->maint_ev)
      event_del(state->maint_ev);
    return;
  }
  if(api->last_attempt > ZEKE_NULL_TIME && (now - api->last_attempt) > apr_time_from_sec(1))
    api->reconnect_delay = ZEKE_IMMEDIATE;

  in_io_loop++;

  if(conn != NULL) {
    sock = conn->sock;
    if(!sock && !state->zh) {
      state->started = 0;
      if(!state->client)
        state->client = &api->client;

      st = zeke_connection_start(conn,state->client,&api->reconnect_attempts);
      state->zh = conn->zh;
      if(!ZEKE_STATUS_IS_OKAY(st)) {
        state->zh = NULL;
        reschedule_maintenance_ex(state,io_event_initial,api,api->reconnect_delay);
        /* try again in 100 ms */
        if(api->reconnect_delay <= ZEKE_IMMEDIATE)
          api->reconnect_delay = apr_time_from_msec(100);
        else
          api->reconnect_delay += apr_time_from_msec(200);
        if(api->reconnect_attempts == 0) {
          api->reconnect_attempts = ZEKE_RECONNECT_ATTEMPTS;
          api->reconnect_delay += apr_time_from_msec(500);
          event_del(state->maint_ev);
          assert(zeke_generic_event_add(state->maint_ev,api->reconnect_delay) == APR_SUCCESS);
        }
        WARN("connection failure (%s), reconnect delay is %" APR_UINT64_T_FMT " ms",
             zeke_errstr(st),api->reconnect_delay / APR_TIME_C(1000));

        api->last_attempt = now;
        goto io_event_initial_end;
      }
      if(state->zh) {
        apr_pool_t *zpool = zeke_zhandle_context_pool(state->zh);
        assert(zpool != NULL);
        zeke_pool_pre_cleanup_indirect(zpool,state->zh);
        state->started++;
      } else {
        WARN("(re-)connect in progress, remaining attempts before sleep: %d",(int)api->reconnect_attempts);
        reschedule_maintenance_ex(state,io_event_initial,api,api->reconnect_delay);
        api->last_attempt = now;
        goto io_event_initial_end;
      }
    }
    timeout = state->cur_timeout;
    st = zeke_connection_socket_update(conn,&sock,&poll_events,&timeout,NULL);
    LOG("SOCKET UPDATE, new sock=%pp/%pp, poll_events=%u, status=%s",conn->sock,sock,
         (unsigned)poll_events,(st == ZEOK ? "Ok" : zeke_errstr(st)));

    if(st == APR_FROM_ZK_ERROR(ZNOTHING))
      st = ZEOK;
    if(!ZEKE_STATUS_IS_OKAY(st)) {
      if(st == APR_FROM_ZK_ERROR(ZINVALIDSTATE) || conn->sock == NULL ||
               ZEKE_STATUS_CAN_RETRY(st)) {
        /* try reconnecting */
        state->conn = NULL;
        assert(state->maint_ev != NULL);
        zeke_connection_close(conn);
        reschedule_maintenance_ex(state,io_event_initial,api,api->reconnect_delay);
        /* try again in 100 ms */
        if(api->reconnect_delay <= ZEKE_IMMEDIATE)
          api->reconnect_delay = apr_time_from_msec(100);
        else
          api->reconnect_delay += apr_time_from_msec(200);
        api->last_attempt = now;
      } else if(!ZEKE_STATUS_IS_OKAY(st)) {
        zeke_apr_eprintf(st,"Cannot connect to zookeeper: %s", api->zkhosts);
        zeke_safe_shutdown(100);
      }
      goto io_event_initial_end;
    } else {
      assert(sock == state->conn->sock);
      assert(zeke_event_base_get_ex(state->conn->pool,0) != NULL);
      zeke_event_base_set(zeke_event_base_get(state->conn->pool),0,apr_socket_pool_get(sock));
    }
  } else {
    /* try to make a new connection */
    if(zeke_terminating)
      return;
    st = zeke_connection_create(&state->conn,api->zkhosts,NULL,state->pool);
    if(!ZEKE_STATUS_IS_OKAY(st))
      zeke_fatal("zeke_connection_create",st);
    conn = state->conn;
    if(zeke_event_base_exists(state->pool))
      assert(zeke_event_base_get(state->pool) == zeke_event_base_get_ex(conn->pool,0));
    zeke_pool_cleanup_register(conn->pool,zeke_indirect_make(&state->conn,state->pool),
                                  zeke_indirect_deref_wipe);
    reschedule_maintenance(state,io_event_initial);
    goto io_event_initial_end;
  }
  if(conn->sock == NULL) {
    INFO("no socket, rescheduling io_event_initial");
    reschedule_maintenance(state,io_event_initial);
    goto io_event_initial_end;
  }
  conn = state->conn;
  sock = conn->sock;
  if(state->ev == NULL) {
    assert(sock != NULL);
    ev = zeke_socket_event_get(sock);
    if(!ev) {
      st = zeke_socket_event_create_ex(&ev,sock,io_event,0,timeout,conn->pool);
      if(!ZEKE_STATUS_IS_OKAY(st)) {
        WARN("rescheduling io_event_initial, cannot create socket event: %s",zeke_errstr(st));
        state->zh = NULL;
        if(conn->zh)
          close_zhandle_indirect(&conn->zh);
        reschedule_maintenance(state,io_event_initial);
        goto io_event_initial_end;
      }
      assert(zeke_socket_event_set(sock,io_event,state) == APR_SUCCESS);
    }
    assert(ev != NULL);
    state->ev = ev;
    zeke_pool_cleanup_indirect(apr_socket_pool_get(conn->sock),state->ev);
#if 0
    apr_pool_pre_cleanup_register(apr_socket_pool_get(conn->sock),
                                  zeke_indirect_make(&state->ev,state->pool),
                                  zeke_indirect_deref_wipe);
#endif
  }
  event_del(state->ev);
  assert(event_get_base(state->ev) == zeke_event_base_get(state->pool));

  LOG("poll_events = %u, timeout = %" APR_UINT64_T_FMT, poll_events,timeout);
  if(poll_events == 0 && timeout == APR_TIME_C(-1)) {
    st = zeke_connection_socket_maint(conn,&flags,&timeout);
    if(!ZEKE_STATUS_IS_OKAY(st))
      zeke_fatal("zeke_connection_socket_maint",st);

    if((flags == 0 && timeout == APR_TIME_C(-1)) || !state->ev || (sock != conn->sock)) {
      LOG("restarting connection %pp",conn->sock);
      if(conn->sock)
        apr_pool_destroy(apr_socket_pool_get(conn->sock));
      reschedule_maintenance(state,io_event_initial);
      api->last_attempt = now;
      goto io_event_initial_end;
    } else sock = conn->sock;
  } else {
    if(poll_events & APR_POLLIN)
      flags |= APR_READ;
    if(poll_events & APR_POLLOUT)
      flags |= APR_WRITE;
  }
  if(timeout <= APR_TIME_C(0))
    timeout = APR_TIME_C(-1);
  assert(zeke_socket_event_type_set(conn->sock, flags, 1) == APR_SUCCESS);
  state->cur_timeout = timeout;
  assert(zeke_socket_event_add(conn->sock, timeout) == APR_SUCCESS);
  event_del(state->maint_ev);
  assert(zeke_generic_event_callback_set(state->maint_ev,io_event_maint,state) == APR_SUCCESS);
  assert(zeke_generic_event_type_set(state->maint_ev,EV_TIMEOUT|EV_PERSIST) == APR_SUCCESS);
  assert(zeke_generic_event_add(state->maint_ev,APR_TIME_C(250000)) == APR_SUCCESS);

io_event_initial_end:
  in_io_loop--;
}

ZEKE_API(zk_status_t) zeke_io_event_loop_restart(struct io_event_state *state)
{
  zeke_event_api_t *api = NULL;

  if(has_event_loop_api(state,NULL) && (state->conn || state->started > 0)) {
    state->got_break = 0;
    state->got_exit = 0;
    state->exit_code = 0;
    assert(state->maint_ev != NULL);
    event_del(state->maint_ev);
    assert(zeke_generic_event_callback_set(state->maint_ev,io_event_initial,api) == APR_SUCCESS);
    assert(zeke_generic_event_add(state->maint_ev, ZEKE_IMMEDIATE) == APR_SUCCESS);
    if(state->ev)
      event_del(state->ev);
    assert(io_current_event_state_set(state) == APR_SUCCESS);
    return APR_SUCCESS;
  }
  return APR_EBUSY;
}

ZEKE_PRIVATE(void) zeke_io_event_reconnect(struct io_event_state *state)
{
  zeke_event_api_t *api = NULL;

  ZKDEBUG("IO EVENT RECONNECT");
  if(has_event_loop_api(state,&api) && state->conn) {
    if(state->conn->zh && zeke_connection_is_closed(state->conn))
      zeke_fatal("zeke_io_event_reconnect on closed connection",
                 APR_FROM_ZK_ERROR(ZENOSESSION));
    if(state->conn->sock)
      apr_pool_destroy(apr_socket_pool_get(state->conn->sock));
    state->started = 0;
    api->last_attempt = apr_time_now();
    io_event_initial(-1,EV_TIMEOUT,api);
  } else {
    ERR("Cannot reconnect, connection handle has been destroyed.");
    zeke_safe_shutdown(100);
  }
}

ZEKE_API(apr_pool_t*) zeke_io_event_loop_pool_get(zeke_io_event_handle_t h)
{
  zeke_event_api_t *api = NULL;

  ZEKE_MAGIC_CHECK(h, ZEKE_IO_EVENT_STATE_MAGIC);
  if(!h || !has_event_loop_api(h,&api))
    return NULL;

  ZEKE_MAGIC_CHECK(api, ZEKE_LIBEVENT_API_MAGIC);
  return h->pool;
}

ZEKE_API(apr_status_t) zeke_io_event_loop_status_get(zeke_io_event_handle_t h,
                                                     zk_status_t *stp)
{
    if(!h || !stp)
      return APR_EINVAL;

    ZEKE_MAGIC_CHECK(h, ZEKE_IO_EVENT_STATE_MAGIC);

    *stp = h->st;
    return APR_SUCCESS;
}

ZEKE_API(zk_status_t) zeke_io_event_loop_connect(struct io_event_state *state,
                                                 const char *zkhosts,
                                                 zeke_runloop_callback_fn setup,
                                                 apr_uint32_t flags,
                                                 void *app_data)
{
  zk_status_t st;
  zeke_event_api_t *api = NULL;

  if(!zkhosts || !state)
    return APR_EINVAL;
  api = get_event_loop_api(state);

  if(api->state != state || api->setup || api->zkhosts
                         || api->userdata || state->conn)
    return APR_EBUSY;

  ZEKE_MAGIC_CHECK(api, ZEKE_LIBEVENT_API_MAGIC);
  api->setup = setup;
  api->zkhosts = apr_pstrdup(api->pool,zkhosts);
  api->flags = flags;
  api->userdata = app_data;
  state->cur_timeout = APR_TIME_C(-1);
  st = zeke_connection_create(&state->conn,zkhosts,NULL,api->pool);
  zeke_pool_cleanup_register(state->conn->pool,zeke_indirect_make(&state->conn,state->pool),
                                 zeke_indirect_deref_wipe);
  zeke_event_base_set(zeke_event_base_get(api->pool),0,state->conn->pool);
  assert(zeke_event_base_get_ex(state->conn->pool,0) != NULL);

  if(!ZEKE_STATUS_IS_OKAY(st)) {
    api = NULL;
    goto zeke_io_event_loop_connect_error;
  }
  state->api = io_event_api_extern;
  assert(state->conn != NULL);
  if(setup) {
    apr_pool_t *p = zeke_connection_pool_get(state->conn);
    zeke_delayed_startup_t *start = apr_palloc(p,sizeof(zeke_delayed_startup_t));
    start->callback = setup;
    start->registered_name = LIBZEKE_RUNLOOP_DELAYED_STARTUP_KEY;
    start->data = app_data;
    zeke_connection_named_watcher_add(state->conn,start->registered_name,delayed_startup);
    if(!ZEKE_STATUS_IS_OKAY(st)) {
      api = NULL;
      goto zeke_io_event_loop_connect_error;
    }
    assert(apr_pool_userdata_setn(start,LIBZEKE_RUNLOOP_DELAYED_STARTUP_KEY,NULL,p) == APR_SUCCESS);
  }
  st = reschedule_maintenance_ex(state,io_event_initial,api,ZEKE_IMMEDIATE);
  if(!ZEKE_STATUS_IS_OKAY(st))
    api = NULL;

zeke_io_event_loop_connect_error:
  if(api != NULL) {
    if(ZEKE_STATUS_IS_OKAY(st)) {
      assert(io_current_event_state_set(state) == APR_SUCCESS);
    } else {
      zeke_apr_eprintf(st,"cannot connect api=%pp io_event_state=%pp",api,state);
      apr_pool_destroy(api->pool);
    }
  }
  return st;
}

ZEKE_API(zk_status_t) zeke_app_event_loop(const char *zkhosts,
                                          struct event_base *base,
                                          zeke_runloop_callback_fn setup,
                                          apr_uint32_t flags,
                                          void *app_data)
{
  return zeke_app_event_loop_ex(zkhosts,base,APR_TIME_C(-1),NULL,setup,NULL,
                                flags,app_data,NULL);
}

ZEKE_API(zk_status_t) zeke_app_event_loop_ex(const char *zkhosts,
                                             struct event_base *base,
                                             apr_interval_time_t max_run_time,
                                             zeke_runloop_callback_fn timeout,
                                             zeke_runloop_callback_fn startup,
                                             zeke_runloop_callback_fn shutdown,
                                             apr_uint32_t flags,
                                             void *app_data,
                                             apr_pool_t *root_pool)
{
  zk_status_t st = ZEOK;
  static apr_pool_t *loop_pool = NULL;
  unsigned long c = 0;
  apr_interval_time_t run_timer;
  apr_pool_t *pool = NULL;
  zeke_connection_t *conn = NULL;

  if(loop_pool == NULL || loop_pool != root_pool) {
    if(root_pool != NULL)
      loop_pool = root_pool;
    else {
      loop_pool = zeke_root_subpool_create();
      zeke_pool_tag(loop_pool,"zeke_app_run_event_loop root pool");
    }
  }

  run_timer = max_run_time;
  if(run_timer <= APR_TIME_C(0))
    run_timer = APR_TIME_C(-1);
  for(c = 0; ZEKE_STATUS_IS_OKAY(st); c++) {
    if(pool == NULL) {
      st = apr_pool_create(&pool,loop_pool);
      if(st != APR_SUCCESS)
        break;
      zeke_pool_tag(pool,apr_psprintf(pool,"zeke_app_run_event_loop subpool, loop %lu",c));
    }
    if(conn == NULL) {
      st = zeke_connection_create(&conn,zkhosts,NULL,pool);
      if(!ZEKE_STATUS_IS_OKAY(st))
        break;
      if(startup) {
        if(flags & ZEKE_RUNLOOP_DELAY_STARTUP_CALLBACK) {
          apr_pool_t *p = zeke_connection_pool_get(conn);
          zeke_delayed_startup_t *start = apr_palloc(p,sizeof(zeke_delayed_startup_t));
          start->callback = startup;
          start->registered_name = LIBZEKE_RUNLOOP_DELAYED_STARTUP_KEY;
          start->data = app_data;
          zeke_connection_named_watcher_add(conn,start->registered_name,delayed_startup);
          if(!ZEKE_STATUS_IS_OKAY(st))
            break;
          assert(apr_pool_userdata_setn(start,LIBZEKE_RUNLOOP_DELAYED_STARTUP_KEY,NULL,p) == APR_SUCCESS);
        } else
          startup(conn,app_data);
      }
    }

    st = zeke_event_loop(conn,base,(run_timer > APR_TIME_C(-1) ? &run_timer : NULL), NULL);
    if(!ZEKE_STATUS_IS_OKAY(st)) {
      if(APR_STATUS_IS_TIMEUP(st)) {
        if(timeout)
          timeout(conn,app_data);
        else {
          zeke_connection_close(conn);
          break;
        }
        /* if the user didn't perform a full close on the connection, restart */
        if(zeke_connection_is_open(conn)) {
          st = ZEOK;
          if(max_run_time > APR_TIME_C(-1))
            run_timer = max_run_time;
          continue;
        }
      }
    } else if(!zeke_connection_is_active(conn)) break;
  }

  if(conn != NULL && shutdown)
    shutdown(conn,app_data);

  if(pool != NULL)
    apr_pool_destroy(pool);

  if(loop_pool != root_pool)
    apr_pool_clear(loop_pool);

  return st;
}

static apr_status_t cleanup_pool_file_null(void *ign)
{
  return APR_SUCCESS;
}

static apr_status_t cleanup_pool_sock_null(void *ign)
{
  return APR_SUCCESS;
}

static apr_status_t cleanup_pool_event_base(void *b)
{
  if(b)
    event_base_free((struct event_base*)b);
  return APR_SUCCESS;
}

static apr_status_t cleanup_pool_event(void *e)
{
  if(e) {
    event_del((struct event*)e);
    event_free((struct event*)e);
  }
  return APR_SUCCESS;
}

static apr_status_t cleanup_child_pool_event(void *e)
{
  int fd = -1;
  if(e) {
    event_del((struct event*)e);
    fd = event_get_fd((struct event*)e);
    if(fd > -1)
      close(fd);
  }
  return APR_SUCCESS;
}

ZEKE_API(int) zeke_event_base_exists(apr_pool_t *pool)
{
  apr_status_t st;
  struct event_base *base = NULL;

  if(!pool)
    return 0;

  st = apr_pool_userdata_get((void**)&base,LIBZEKE_EVENT_BASE_POOL_KEY,pool);
  return (st == APR_SUCCESS && base != NULL);
}

ZEKE_API(struct event_base*) zeke_event_base_get_ex(apr_pool_t *pool, int create)
{
  apr_status_t st;
  struct event_base *base = NULL;

  if(!pool)
    return NULL;

  st = apr_pool_userdata_get((void**)&base,LIBZEKE_EVENT_BASE_POOL_KEY,pool);
  if((st != APR_SUCCESS || base == NULL) && create) {
    base = event_base_new();
    assert(base != NULL);
    st = zeke_event_base_set(base,1,pool);
    assert(st == APR_SUCCESS);
  }
  return base;
}

ZEKE_API(struct event_base*) zeke_event_base_get(apr_pool_t *pool)
{
  return zeke_event_base_get_ex(pool,1);
}

ZEKE_API(zk_status_t) zeke_event_create_ex(struct event **evp, apr_uint32_t flags,
                                           short persist, event_callback_fn handler,
                                           void *arg, apr_pool_t *pool)
{
  zk_status_t st = APR_SUCCESS;
  struct event_base *base;
  short what = (persist ? EV_PERSIST : 0);

  if(!evp)
    return APR_EINVAL;
  base = zeke_event_base_get(pool);

  if(base == NULL)
    return APR_EINVAL;

  if(flags & APR_READ)
    what |= EV_READ;
  if(flags & APR_WRITE)
    what |= EV_WRITE;

  if(what == 0 || what == EV_PERSIST)
    what |= EV_TIMEOUT;
  *evp = event_new(base,-1,what,handler,arg);
  if(*evp != NULL)
    apr_pool_cleanup_register(pool,*evp,cleanup_pool_event,cleanup_child_pool_event);
  else
    st = APR_EGENERAL;

  return st;
}

ZEKE_API(apr_status_t) zeke_event_base_pool_create(apr_pool_t **cp,
                                                   apr_pool_t *p)
{
  apr_status_t st;
  struct event_base *base = (p ? zeke_event_base_get_ex(p,0) : NULL);

  st = apr_pool_create(cp,p);
  if(st == APR_SUCCESS && base != NULL)
    st = zeke_event_base_set(base,0,*cp);
  return st;
}

ZEKE_API(zk_status_t) zeke_event_create(struct event **evp, apr_pool_t *pool)
{
  zk_status_t st = APR_SUCCESS;
  struct event_base *base;

  if(!evp)
    return APR_EINVAL;
  base = zeke_event_base_get(pool);

  if(base == NULL)
    return APR_EINVAL;

  *evp = event_new(base, -1, EV_TIMEOUT, NULL, NULL);
  if(*evp != NULL)
    apr_pool_cleanup_register(pool,*evp,cleanup_pool_event,cleanup_child_pool_event);
  else
    st = APR_EGENERAL;

  return st;
}

static apr_status_t cleanup_pool_file_event(void *fp)
{
  if(fp)
    apr_file_close((apr_file_t*)fp);
  return APR_SUCCESS;
}

static apr_status_t cleanup_pool_sock_event(void *sp)
{
  if(sp)
    apr_socket_close((apr_socket_t*)sp);
  return APR_SUCCESS;
}

static apr_status_t cleanup_child_pool_file_event(void *ep)
{
  struct event *e = (struct event*)ep;
  evutil_socket_t fd = -1;
  if(e)
    fd = event_get_fd(e);

  if(fd > -1)
    close(fd);
  return APR_SUCCESS;
}

static apr_status_t cleanup_child_pool_sock_event(void *ep)
{
  struct event *e = (struct event*)ep;
  evutil_socket_t fd = -1;

  if(e)
    fd = event_get_fd(e);

  if(fd > -1)
    close(fd);
  return APR_SUCCESS;
}

static apr_status_t  cleanup_generic_event(void *evp)
{
  if(evp) {
    event_del((struct event*)evp);
  }
  return APR_SUCCESS;
}

ZEKE_API(struct event*) zeke_file_event_get(apr_file_t *f)
{
  struct event *ev;
  assert(f != NULL);
  if(apr_file_data_get((void*)&ev,ZEKE_EVENT_KEY(file),f) != APR_SUCCESS)
    ev = NULL;
  return ev;
}

ZEKE_API(struct event*) zeke_socket_event_get(apr_socket_t *s)
{
  struct event *ev = NULL;
  assert(s != NULL);

  if(apr_socket_data_get((void*)&ev,ZEKE_EVENT_KEY(socket),s) != APR_SUCCESS)
    ev = NULL;
  return ev;
}

ZEKE_API(apr_status_t) zeke_socket_event_del(apr_socket_t *s)
{
  struct event *ev;
  if(!s) return APR_EINVAL;
  ev = zeke_socket_event_get(s);
  if(!ev)
    return APR_EINVAL;
  event_del(ev);
  return APR_SUCCESS;
}

ZEKE_API(apr_status_t) zeke_file_event_del(apr_file_t *f)
{
  struct event *ev;
  if(!f) return APR_EINVAL;
  ev = zeke_file_event_get(f);
  if(!ev)
    return APR_EINVAL;

  event_del(ev);
  return APR_SUCCESS;
}

ZEKE_PRIVATE(int)
zeke_generic_event_pending(const struct event *ev, short what,
                           apr_interval_time_t *expires)
{
  int rc;

  if(expires || (what & EV_TIMEOUT)) {
    struct timeval tv = {0,0};
    rc = event_pending(ev,(what|EV_TIMEOUT),&tv);
    if(expires)
      *expires = apr_time_make(tv.tv_sec,tv.tv_usec);
  } else
    rc = event_pending(ev,(what&~EV_TIMEOUT),NULL);
  return rc;
}

ZEKE_PRIVATE(int)
zeke_generic_event_info_get(struct event *ev,
                            apr_os_sock_t *fd,
                            apr_uint32_t *flags,
                            short *persist,
                            short *signal,
                            apr_interval_time_t *remaining)
{
  evutil_socket_t sfd;
  short events;
  int pending;

  if(remaining)
    *remaining = APR_TIME_C(-1);

  pending = zeke_generic_event_pending(ev,EV_TIMEOUT|EV_READ|EV_WRITE|EV_SIGNAL,remaining);

  event_get_assignment(ev,NULL,&sfd,&events,NULL,NULL);
  if(fd)
    *fd = sfd;
  if(persist)
    *persist = (events & EV_PERSIST);
  if(signal)
    *signal = (events & EV_SIGNAL);
  if(flags) {
    *flags = 0;
    if(events & EV_READ)
      *flags |= APR_READ;
    if(events & EV_WRITE)
      *flags |= APR_WRITE;
  }

  return pending;
}

ZEKE_PRIVATE(apr_status_t)
zeke_generic_event_type_set(struct event *ev, short new_events)
{
  int pending;
  struct event_base *base;
  evutil_socket_t fd;
  event_callback_fn fn;
  void *arg;

  apr_interval_time_t remaining = APR_TIME_C(-1);

  pending = zeke_generic_event_pending(ev,EV_TIMEOUT|EV_READ|EV_WRITE|EV_SIGNAL,
                                       &remaining);
  if(pending)
    event_del(ev);

  pending |= (((EV_READ|EV_WRITE) & new_events) & pending);
  event_get_assignment(ev,&base,&fd,NULL,&fn,&arg);
  if(event_assign(ev,base,fd,new_events,fn,arg) == -1)
    return get_libevent_error();
  if(pending) {
    if(remaining >= APR_TIME_C(0)) {
      struct timeval tv;
      tv.tv_sec = apr_time_sec(remaining);
      tv.tv_usec = apr_time_usec(remaining);
      if(event_add(ev,&tv) == -1)
        return get_libevent_error();
    } else if(event_add(ev,NULL) == -1)
        return get_libevent_error();
  }

  return APR_SUCCESS;
}

ZEKE_PRIVATE(apr_status_t)
zeke_generic_event_callback_set(struct event *ev,
                 event_callback_fn fn, void *arg)
{
  int pending;
  struct event_base *base;
  evutil_socket_t fd;
  short what;
  event_callback_fn old_fn = NULL;
  apr_interval_time_t remaining = APR_TIME_C(-1);

  pending = zeke_generic_event_pending(ev,EV_TIMEOUT|EV_READ|EV_WRITE|EV_SIGNAL,
                                       &remaining);
  if(pending)
    event_del(ev);
  event_get_assignment(ev,&base,&fd,&what,&old_fn,NULL);
  if(event_assign(ev,base,fd,what,(fn ? fn : old_fn),arg) == -1)
    return get_libevent_error();
  if(pending) {
    if(remaining >= APR_TIME_C(0)) {
      struct timeval tv;
      tv.tv_sec = apr_time_sec(remaining);
      tv.tv_usec = apr_time_usec(remaining);
      if(event_add(ev,&tv) == -1)
        return get_libevent_error();
    } else if(event_add(ev,NULL) == -1)
        return get_libevent_error();
  }
  return APR_SUCCESS;
}

ZEKE_PRIVATE(apr_status_t)
zeke_generic_event_add(struct event *ev, apr_interval_time_t timeout)
{
  struct timeval tv;
  int rc;

  if(!ev || !event_initialized(ev))
    return APR_EINVAL;

  if(event_pending(ev,EV_TIMEOUT|EV_SIGNAL|EV_READ|EV_WRITE,NULL))
    event_del(ev);

  if(timeout > APR_TIME_C(-1)) {
    tv.tv_sec = apr_time_sec(timeout);
    tv.tv_usec = apr_time_usec(timeout);
    rc = event_add(ev,&tv);
  } else
    rc = event_add(ev,NULL);

  return (rc == -1 ? get_libevent_error() : APR_SUCCESS);
}

ZEKE_API(int) zeke_socket_event_pending(apr_socket_t *s,
                                        apr_interval_time_t *expires)
{
  struct event *ev;

  if(s) {
    ev = zeke_socket_event_get(s);
    if(ev)
      return zeke_generic_event_pending(ev,EV_READ|EV_WRITE,expires);
  }
  return 0;
}

ZEKE_API(int) zeke_file_event_pending(apr_file_t *f,
                                      apr_interval_time_t *expires)
{
  struct event *ev;

  if(f) {
    ev = zeke_file_event_get(f);
    if(ev)
      return zeke_generic_event_pending(ev,EV_READ|EV_WRITE,expires);
  }
  return 0;
}

ZEKE_API(apr_status_t) zeke_socket_event_add(apr_socket_t *s,
                                             apr_interval_time_t timeout)
{
  struct event *ev;
  struct timeval tv;
  int rc;

  if(!s) return APR_EINVAL;
  ev = zeke_socket_event_get(s);
  if(!ev)
    return APR_EINVAL;

  if(event_pending(ev,EV_READ|EV_WRITE,NULL))
    return APR_EBUSY;

  if(timeout > APR_TIME_C(-1)) {
    tv.tv_sec = apr_time_sec(timeout);
    tv.tv_usec = apr_time_usec(timeout);
    rc = event_add(ev,&tv);
  } else
    rc = event_add(ev,NULL);

  return (rc == -1 ? get_libevent_error() : APR_SUCCESS);
}

ZEKE_API(apr_status_t) zeke_file_event_add(apr_file_t *f, apr_interval_time_t timeout)
{
  struct event *ev;
  struct timeval tv;
  int rc;

  if(!f) return APR_EINVAL;
  ev = zeke_file_event_get(f);
  if(!ev)
    return APR_EINVAL;

  if(event_pending(ev,EV_READ|EV_WRITE,NULL))
    return APR_EBUSY;

  if(timeout > APR_TIME_C(-1)) {
    tv.tv_sec = apr_time_sec(timeout);
    tv.tv_usec = apr_time_usec(timeout);
    rc = event_add(ev,&tv);
  } else
    rc = event_add(ev,NULL);

  return (rc == -1 ? get_libevent_error() : APR_SUCCESS);
}

ZEKE_API(zk_status_t)
zeke_socket_event_set(apr_socket_t *s, event_callback_fn fn, void *arg)
{
  struct event *ev;
  if(!s) return APR_EINVAL;
  ev = zeke_socket_event_get(s);
  if(!ev) return APR_EINVAL;

  return zeke_generic_event_callback_set(ev,fn,arg);
}

ZEKE_API(zk_status_t)
zeke_file_event_set(apr_file_t *f, event_callback_fn fn, void *arg)
{
  struct event *ev;
  if(!f) return APR_EINVAL;
  ev = zeke_file_event_get(f);
  if(!ev) return APR_EINVAL;

  return zeke_generic_event_callback_set(ev,fn,arg);
}

ZEKE_API(zk_status_t)
zeke_socket_event_type_set(apr_socket_t *s, apr_uint32_t flags, short persist)
{
  short new_events = 0;
  struct event *ev;

  if(!s) return APR_EINVAL;
  ev = zeke_socket_event_get(s);
  if(!ev) return APR_EINVAL;

  if(persist)
    new_events |= EV_PERSIST;
  if(flags & APR_READ)
    new_events |= EV_READ;
  if(flags & APR_WRITE)
    new_events |= EV_WRITE;

  return zeke_generic_event_type_set(ev, new_events);
}

ZEKE_API(zk_status_t)
zeke_socket_event_type_get(apr_socket_t *s, apr_uint32_t *flags, short *persist)
{
  struct event *ev;

  if(!s) return APR_EINVAL;
  ev = zeke_socket_event_get(s);
  if(!ev) return APR_EINVAL;

  (void)zeke_generic_event_info_get(ev,NULL,flags,persist,NULL,NULL);
  return APR_SUCCESS;
}

ZEKE_API(zk_status_t)
zeke_file_event_type_get(apr_file_t *f, apr_uint32_t *flags, short *persist)
{
  struct event *ev;

  if(!f) return APR_EINVAL;
  ev = zeke_file_event_get(f);
  if(!ev) return APR_EINVAL;

  (void)zeke_generic_event_info_get(ev,NULL,flags,persist,NULL,NULL);
  return APR_SUCCESS;
}

ZEKE_API(zk_status_t)
zeke_file_event_type_set(apr_file_t *f, apr_uint32_t flags, short persist)
{
  short new_events = 0;
  struct event *ev;
  if(!f) return APR_EINVAL;
  ev = zeke_file_event_get(f);
  if(!ev) return APR_EINVAL;

  if(persist)
    new_events |= EV_PERSIST;
  if(flags & APR_READ)
    new_events |= EV_READ;
  if(flags & APR_WRITE)
    new_events |= EV_WRITE;

  return zeke_generic_event_type_set(ev, new_events);
}

ZEKE_API(void*)
zeke_file_event_data_get(apr_file_t *f)
{
  struct event *ev;
  void *arg = NULL;
  if(!f) return arg;
  ev = zeke_file_event_get(f);

  if(ev)
    event_get_assignment(ev,NULL,NULL,NULL,NULL,&arg);
  return arg;
}

ZEKE_API(void*)
zeke_socket_event_data_get(apr_socket_t *s)
{
  struct event *ev;
  void *arg = NULL;
  if(!s) return arg;
  ev = zeke_socket_event_get(s);

  if(ev)
    event_get_assignment(ev,NULL,NULL,NULL,NULL,&arg);
  return arg;
}

ZEKE_API(zk_status_t)
zeke_socket_event_create_ex(struct event **evp, apr_socket_t *s,
                                    event_callback_fn handler,
                                    apr_uint32_t flags,
                                    apr_interval_time_t timeout,
                                    apr_pool_t *base_pool)
{
  apr_status_t st = APR_SUCCESS;
  apr_os_sock_t fd = -1;
  struct event_base *base;
  short events = 0;
  apr_pool_t *p;
  int add = 0;
  if(!evp || !s)
    return APR_EINVAL;

  *evp = zeke_socket_event_get(s);
  if(*evp)
    event_del(*evp);
  
  add = ((flags & (APR_READ|APR_WRITE)) != 0);
  if(flags & APR_READ)
    events |= EV_READ;
  if(flags & APR_WRITE)
    events |= EV_WRITE;

  p = apr_socket_pool_get(s);
  if(base_pool == NULL)
    base_pool = p;
  base = zeke_event_base_get(base_pool);
  if(base == NULL || p == NULL)
    return APR_EINVAL;
  st = apr_socket_opt_set(s, APR_SO_NONBLOCK,1);
  if(st != APR_SUCCESS)
    return st;
  st = apr_os_sock_get(&fd,s);
  if(st != APR_SUCCESS)
    return st;

  if(*evp) {
    if(event_assign(*evp, base, fd, events, handler, s) == -1)
      st = get_libevent_error();
  } else {
    *evp = event_new(base, fd, events, handler, s);
    if(evp != NULL) {
      assert(apr_socket_data_set(s,*evp,ZEKE_EVENT_KEY(socket),cleanup_generic_event) == APR_SUCCESS);
      apr_socket_inherit_set(s);
      apr_pool_cleanup_register(p,s,cleanup_pool_sock_event,apr_pool_cleanup_null);
      apr_pool_cleanup_register(p,*evp,cleanup_pool_sock_null,cleanup_child_pool_sock_event);
    } else
      st = get_libevent_error();
  }

  if(st == APR_SUCCESS && *evp && add) {
    struct timeval tv = {0,0};
    if(timeout > APR_TIME_C(0)) {
      tv.tv_sec = apr_time_sec(timeout);
      tv.tv_usec = apr_time_usec(timeout);
    }
    if(event_add(*evp, (timeout > APR_TIME_C(-1) ? &tv : NULL)) == -1)
     st = get_libevent_error();
  } else if(st == APR_SUCCESS && !*evp)
    st = APR_EGENERAL;
  return st;
}

ZEKE_API(zk_status_t)
zeke_file_event_create_ex(struct event **evp, apr_file_t *f,
                                    event_callback_fn handler,
                                    int add, apr_interval_time_t timeout,
                                    apr_pool_t *base_pool)
{
  apr_status_t st = APR_SUCCESS;
  apr_os_file_t fd = -1;
  struct event_base *base;
  short events = 0;
  apr_uint32_t flags;
  apr_pool_t *p;

  if(!evp || !f)
    return APR_EINVAL;

  *evp = zeke_file_event_get(f);
  if(*evp)
    event_del(*evp);

  flags = apr_file_flags_get(f);
  if((flags & (APR_WRITE|APR_READ)) != 0) {
    if(flags & APR_WRITE)
      events |= EV_WRITE;
    if(flags & APR_READ)
      events |= EV_READ;
  } else {
    flags = 0;
    add = 0;
  }
  p = apr_file_pool_get(f);
  if(base_pool == NULL)
    base_pool = p;
  base = zeke_event_base_get(base_pool);
  if(base == NULL || p == NULL)
    return APR_EINVAL;
  st = apr_file_pipe_timeout_set(f,APR_TIME_C(0));
  if(st != APR_SUCCESS)
    return st;
  st = apr_os_file_get(&fd,f);
  if(st != APR_SUCCESS)
    return st;
  if(*evp) {
    if(event_assign(*evp, base, fd, events, handler, f) == -1)
      st = get_libevent_error();
  } else {
    *evp = event_new(base, fd, events, handler, f);
    if(evp != NULL) {
      assert(apr_file_data_set(f,*evp,ZEKE_EVENT_KEY(file),cleanup_generic_event) == APR_SUCCESS);
      apr_file_inherit_set(f);
      apr_pool_cleanup_register(p,f,cleanup_pool_file_event,apr_pool_cleanup_null);
      apr_pool_cleanup_register(p,*evp,cleanup_pool_file_null,cleanup_child_pool_file_event);
    } else
      st = get_libevent_error();
  }

  if(st == APR_SUCCESS && *evp && add) {
    struct timeval tv = {0,0};
    if(timeout > APR_TIME_C(0)) {
      tv.tv_sec = apr_time_sec(timeout);
      tv.tv_usec = apr_time_usec(timeout);
    }
    if(event_add(*evp, (timeout > APR_TIME_C(-1) ? &tv : NULL)) == -1)
     st = get_libevent_error();
  } else if(st == APR_SUCCESS && !*evp)
    st = APR_EGENERAL;
  return st;
}

ZEKE_API(zk_status_t) zeke_event_base_set(struct event_base *base, int own,
                                          apr_pool_t *pool)
{
  if(!pool)
    return APR_EINVAL;

  return apr_pool_userdata_setn(base,LIBZEKE_EVENT_BASE_POOL_KEY,
                               (own ? cleanup_pool_event_base : NULL),
                               pool);
}
#endif /* libevent api */
