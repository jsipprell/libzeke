#ifndef _ZEKE_IO_H
#define _ZEKE_IO_H

#include "internal.h"
#include "libzeke_event_loop.h"

#define ZEKE_CLIENT_ID_KEY "zeke:zk_client_id"
#define ZEKE_ZHANDLE_KEY "zeke:zk_zhandle"
#define ZEKE_CONNECTION_KEY "zeke:connection"

#ifdef LIBZEKE_USE_LIBEVENT

enum io_event_api {
  io_event_api_extern = 0,
#define ZEKE_EVENT_API_EXTERN io_event_api_extern
  io_event_api_intern = 1,
#define ZEKE_EVENT_API_INTERN io_event_api_intern
};

struct io_event_state {
  apr_pool_t *pool;
  apr_uint32_t magic;
#define ZEKE_IO_EVENT_STATE_MAGIC 0x99feba17
  enum io_event_api api;
  apr_time_t start;
  apr_interval_time_t elapsed,*timeout;
  unsigned *reconnect_attempts;
  zeke_connection_t *conn;
  ZEKE_MAY_ALIAS(zhandle_t) *zh,*wzh;
  zk_status_t st;
  ZEKE_MAY_ALIAS(clientid_t) *client;
  int started;
  struct event *ev, *maint_ev;
  apr_interval_time_t cur_timeout;
  int got_break, got_exit, exit_code;
};

ZEKE_EXTERN struct io_event_state *io_current_event_state_get(void);
ZEKE_EXTERN apr_status_t io_current_event_state_set(struct io_event_state*);
ZEKE_EXTERN apr_status_t io_cleanup_event_state(void*);
/* NB: event_base below is optional */
ZEKE_EXTERN struct io_event_state *io_current_event_state_create(apr_pool_t*,
                                                                 struct event_base*);
ZEKE_EXTERN struct event_base *io_current_event_state_base(void);
ZEKE_EXTERN void io_event_maint(evutil_socket_t, short, void*);
ZEKE_EXTERN void io_event(evutil_socket_t, short, void*);
ZEKE_EXTERN void io_event_initial(evutil_socket_t, short, void*);

#endif /* LIBZEKE_USE_LIBEVENT */

ZEKE_EXTERN apr_pool_t *global_io_pool;

ZEKE_EXTERN apr_pool_t *zeke_zhandle_pool(zhandle_t*);
ZEKE_EXTERN void zeke_io_reset_state(apr_pool_t*);

ZEKE_EXTERN zk_status_t close_zhandle(void*);
ZEKE_EXTERN zk_status_t close_zhandle_indirect(void*);
ZEKE_EXTERN zk_status_t zeke_io_set_state(const zeke_connection_t*,
                              const clientid_t*,const zhandle_t*);

#endif /* _ZEKE_IO_H */
