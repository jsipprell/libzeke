#ifndef _ZEKE_HOOKS_H
#define _ZEKE_HOOKS_H

#include "internal.h"
#include "libzeke_hooks.h"

#define ZEKE_SORT_HOOK(module,name,...) zeke_##module##_sort_##name##_hook( __VA_ARGS__ )
#define ZEKE_SORT_CONNECTION_HOOK(name,c,...) zeke_connection_sort_##name##_hook(c, ## __VA_ARGS__ )

#define ZEKE_EXTERN_DECLARE(t) ZEKE_EXTERN t
ZEKE_DECL_GLOBAL_HOOK(ZEKE_EXTERN,void,new_connection,(zeke_connection_t*));
ZEKE_DECL_GLOBAL_HOOK(ZEKE_EXTERN,int,shutdown,(int*));

#ifdef LIBZEKE_USE_LIBEVENT
ZEKE_DECL_GLOBAL_HOOK(ZEKE_EXTERN,void,event_loop_begin,(struct event_base*,apr_pool_t*));
ZEKE_DECL_GLOBAL_HOOK(ZEKE_EXTERN,void,event_loop_end,(struct event_base*,apr_pool_t*));
#endif

#endif /* _ZEKE_HOOKS_H */
