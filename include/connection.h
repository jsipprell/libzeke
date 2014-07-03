#ifndef _ZEKE_CONNECTION_H
#define _ZEKE_CONNECTION_H

#include "internal.h"
#include "libzeke_connection_hooks.h"

#ifndef ZK_TIMEOUT
#define ZK_TIMEOUT 10000
#endif

#define ZEKE_INIT_CONTEXT_KEY "_zeke_private_:init_context_key"

enum zeke_connection_flag_enum {
  zeke_flag_connection_closed = (1 << 0),
#define ZEKE_FLAG_CONNECTION_CLOSED zeke_flag_connection_closed
  zeke_flag_connection_starting = (1 << 1),
#define ZEKE_FLAG_CONNECTION_STARTING zeke_flag_connection_starting
  zeke_flag_connection_hooks_sorted = (1 << 2),
#define ZEKE_FLAG_CONNECTION_HOOKS_SORTED zeke_flag_connection_hooks_sorted
};

/* to register for the session_started hook:
     zeke_hook_connection_session_started(conn,fp,name,char*pre[],char*succ[],order);
   fp prototype:
     void fp(const zeke_connection_t*, apr_pool_t *session_pool);
   notes:
     always called AFTER all global watchers registered with a connection have been called.
     All connection hooks are sorted right before this hook is called meaning that it is
     safe to add hooks inside global watchers.
*/
ZEKE_EXTERN_HOOK_GET(connection,session_started);
ZEKE_DECLARE_HOOK(connection,void,session_started);
/* to register for the closed hook:
     zeke_hook_connection_closed(conn,fp,name,char*pre[],char*succ[],order);
   fp prototype:
     void fp(const zeke_connection_t*);
   notes:
     called immediately before the connection is terminated. This may happen if the
     connection is abnormally terminated so their is no gaurauntee that the connection
     is valid when this hook is called.
*/
ZEKE_EXTERN_HOOK_GET(connection,closed);
ZEKE_DECLARE_HOOK(connection,void,closed);
/* to register for the starting hook (connection start/restart):
     zeke_hook_connection_starting(conn,fp,name,char*pre[],char*succ[],order);
   fp prototype:
     void fp(const zeke_connection_t*, apr_socket_t*);
   notes:
     called whenever a new socket is associated with a connection. The connection will
     always be in startup mode when this happens.
*/
ZEKE_EXTERN_HOOK_GET(connection,starting);
ZEKE_DECLARE_HOOK(connection,void,starting);

struct zeke_connection {
  apr_pool_t *pool;
  zhandle_t *zh;
  const char *zhosts;
  apr_hash_t *watchers;
  apr_socket_t *sock;
  apr_uint32_t flags;
  ZEKE_HOOK_STRUCT(connection,
    ZEKE_HOOK_LINK(connection,session_started);
    ZEKE_HOOK_LINK(connection,closed);
    ZEKE_HOOK_LINK(connection,starting);
  );
};

typedef struct init_context_state context_state_t;

typedef struct {
  /* Top-level(ish) pool that may be long lived. */
  apr_pool_t *pool;
  /* Session pool never lives past the end of zk session */
  apr_pool_t *session_pool;
  /* associated connection */
  zeke_connection_t *connection;
  /* opaque state */
  context_state_t *state;
} init_context_s;


ZEKE_EXTERN zk_status_t zeke_connection_maintenance(const zeke_connection_t*);

/* Atomically update the connection state counter by one, also marking
   the last change as *right now* */
ZEKE_EXTERN void zeke_connection_state_update(const zeke_connection_t*);

/* Non-destructive/non-atomic peek at the connection state internals, returns
   the number of changes since last read and the timestamp of the last change
   if `last_change` is not NULL. */
ZEKE_EXTERN apr_uint64_t zeke_connection_state_peek(const zeke_connection_t*,
                                                    apr_time_t *last_change);

/* Atomically check the connection state internal counters. Return 0 if
   the counters are not identical (meaning updates have occurred since last read.
   Additional restrictions can be added by passing non-NULL arguments for
   `older_than` and `newer_than`. ALL non-NULL time restrictions must be true
   for !0 to be returned. older_than means the last_change must have occurred
   before the `older_than` time and `newer_than` means the last change must
   have occurred after _or at_ the `newer_than` time. */
ZEKE_EXTERN int zeke_connection_state_ensure_current(const zeke_connection_t*,
                                                     const apr_time_t *older_than,
                                                     const apr_time_t *newer_than);


/* Set one or more bit flags from zeke_connection_flag_enum */
ZEKE_EXTERN void zeke_connection_flags_set(zeke_connection_t*, apr_uint32_t flags);
ZEKE_EXTERN void zeke_connection_flags_clear(zeke_connection_t*, apr_uint32_t flags);

/* sort all hooks for a connection if they haven't been sorted */
ZEKE_EXTERN void zeke_connection_sort_all_hooks(zeke_connection_t*);

#endif /* _ZEKE_CONNECTION_H */
