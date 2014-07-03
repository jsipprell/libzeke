#ifndef _LIBZEKE_CONNECTION_HOOKS_H
#define _LIBZEKE_CONNECTION_HOOKS_H

#include <zeke/libzeke_hooks.h>

#ifdef __cplusplus
extern "C"
{
#endif

/* Hook API for zeke_connection_t objects. See libzeke_hooks.h for implementation
   details. */

/* to register for the session_started hook:
     zeke_hook_connection_session_started(conn,fp,name,char*pre[],char*succ[],order);
   fp prototype:
     void fp(const zeke_connection_t*, apr_pool_t *session_pool);
   notes:
     always called AFTER all global watchers registered with a connection have been called.
     All connection hooks are sorted right before this hook is called meaning that it is
     safe to add hooks inside global watchers.
*/
ZEKE_EXPORT_HOOK_SELF(connection,void,session_started,apr_pool_t*);
ZEKE_EXPORT_HOOK_RUN(connection,void,session_started,apr_pool_t*);

/* to register for the closed hook:
     zeke_hook_connection_closed(conn,fp,name,char*pre[],char*succ[],order);
   fp prototype:
     void fp(const zeke_connection_t*);
   notes:
     called immediately before the connection is terminated. This may happen if the
     connection is abnormally terminated so their is no gaurauntee that the connection
     is valid when this hook is called.
*/
ZEKE_EXPORT_HOOK_SELF(connection,void,closed);
ZEKE_EXPORT_HOOK_RUN(connection,void,closed);
/* to register for the starting hook (connection start/restart):
     zeke_hook_connection_starting(conn,fp,name,char*pre[],char*succ[],order);
   fp prototype:
     void fp(const zeke_connection_t*, apr_socket_t*);
   notes:
     called whenever a new socket is associated with a connection. The connection will
     always be in startup mode when this happens.
*/
ZEKE_EXPORT_HOOK_SELF(connection,void,starting,apr_socket_t*);
ZEKE_EXPORT_HOOK_RUN(connection,void,starting,apr_socket_t*);

#ifdef __cplusplus
}
#endif

#endif /* _LIBZEKE_CONNECTION_HOOKS_H */
