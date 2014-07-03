#ifndef _LIBZEKE_HOOKS_H
#define _LIBZEKE_HOOKS_H

/* These are based on apr-util's hooks, but extended to include object association. Each
   set of hooks is private to a specific object. */

#include <apr_hooks.h>

#ifdef __cplusplus
extern "C"
{
#endif

enum {
  zeke_hook_result_okay = 0,
#define ZEKE_HOOK_OKAY zeke_hook_result_okay
#define ZEKE_HOOK_OK zeke_hook_result_okay
  zeke_hook_result_decline,
#define ZEKE_HOOK_DECLINE zeke_hook_result_decline
  zeke_hook_result_abort,
#define ZEKE_HOOK_ABORT zeke_hook_result_abort
};

/* Rather like apr-util's hooks but intended to be encapsulated with in a parent structure
   so that hooks each call requires passing a pointer to the parent. */

#define ZEKE_TYPE(sym) zeke_##sym##_t
#define ZEKE_HOOK_TYPE_VAR(prefix,sym,name) prefix##_##sym##_##name
#define ZEKE_HOOK_LINK_VAR(sym,name) ZEKE_HOOK_TYPE_VAR(link,sym,name)

#define ZEKE_HOOK_TYPE(sym,name) zeke_HOOK_##sym##_##name##_t
#define ZEKE_HOOK_LINK_TYPE(sym,name) zeke_LINK_##sym##_##name##_t
#define ZEKE_HOOK_SYMBOL(ns,sym,name) ns##_##sym##_##name
#define ZEKE_HOOK_FUNC(op,sym,name) zeke_##sym##_##op##_##name##_hook

#define ZEKE_HOOK_ARGS_DECL(sym, ...) (const ZEKE_TYPE(sym) *self, ## __VA_ARGS__)
#define ZEKE_HOOK_ARGS_USE(sym, ...) (self, ## __VA_ARGS__)


/* These declare hook prototypes */
#define ZEKE_IMPLEMENT_HOOK_GET_PROTO(sym,name) \
apr_array_header_t *ZEKE_HOOK_FUNC(get,sym,name)(const ZEKE_TYPE(sym) *self)

#define ZEKE_IMPLEMENT_HOOK_GET_PROTO_VIS(vis,sym,name) \
vis(apr_array_header_t*) ZEKE_HOOK_FUNC(get,sym,name)(const ZEKE_TYPE(sym) *self)

/* These export hook functions */

/* Export the hook running function, for a given type 'foo' with a hook named 'bar', this
   will export as: zeke_foo_run_bar_hook(const zeke_foo_t *,...) */
#define ZEKE_EXPORT_HOOK_RUN(sym,ret,name, ...) \
ZEKE_EXPORT ret zeke_##sym##_run_##name##_hook (const ZEKE_TYPE(sym)*, ## __VA_ARGS__)

/* Export the hook retrieval function, for a given type 'foo' with a hook named 'bar', this
   will export as: zeke_foo_get_bar_book(const zeke_foo_t*); */
#define ZEKE_EXPORT_HOOK_GET(sym,name) \
ZEKE_EXPORT ZEKE_IMPLEMENT_HOOK_GET_PROTO(sym,name)
#define ZEKE_EXTERN_HOOK_GET(sym,name) \
ZEKE_EXTERN ZEKE_IMPLEMENT_HOOK_GET_PROTO(sym,name)

/* This exports the main hook function, for a given type 'foo' with a hook named 'bar', this
   will export as: zeke_hook_foo_bar(const zeke_foo_t*,...) */
#define ZEKE_EXPORT_HOOK(sym,ret,name, ...) \
typedef ret ZEKE_HOOK_TYPE(sym,name) (__VA_ARGS__); \
ZEKE_EXTERN void zeke_##sym##_sort_##name##_hook(const ZEKE_TYPE(sym)*); \
ZEKE_EXPORT void ZEKE_HOOK_SYMBOL(zeke_hook,sym,name)(ZEKE_TYPE(sym) *self, \
                                                       ZEKE_HOOK_TYPE(sym,name) *pf, \
                                                       const char * namespace, \
                                                       const char * const *aszPre, \
                                                       const char * const *aszSucc, \
                                                       int nOrder)
#define ZEKE_EXPORT_HOOK_SELF(sym,ret,name, ...) \
        ZEKE_EXPORT_HOOK(sym,ret,name,const ZEKE_TYPE(sym)*, ## __VA_ARGS__)

/* Export all hook prototypes. These are not all required, however, technically only
   ZEKE_EXPORT_HOOK is required. */
#define ZEKE_EXPORT_HOOK_PROTOS(sym,ret,name, ...) \
ZEKE_EXPORT_HOOK_RUN(sym,ret,name, ## __VA_ARGS__) \
ZEKE_EXPORT ZEKE_IMPLEMENT_HOOK_GET_PROTO(sym,name); \
ZEKE_EXPORT_HOOK(sym,ret,name, ## __VA_ARGS__)

/* Declare a new hook. This does not normally need to be a publically exposed type. */
#define ZEKE_DECLARE_HOOK(sym,ret,name, ...) \
typedef struct ZEKE_HOOK_LINK_TYPE(sym,name) \
{ \
  ZEKE_HOOK_TYPE(sym,name) *pFunc; \
  const char *szName; \
  const char * const *aszPredecessors; \
  const char * const *aszSuccessors; \
  int nOrder; \
} ZEKE_HOOK_LINK_TYPE(sym,name)

#define ZEKE_HOOK_LINK(sym,name) apr_array_header_t *ZEKE_HOOK_TYPE_VAR(link,sym,name)

/* Optionally declare typedef a hook structure */
#define ZEKE_HOOK_STRUCT_TYPE(sym) zeke_hookset_##sym##_t
#define ZEKE_HOOK_STRUCT_REF(sym) ZEKE_HOOK_STRUCT_TYPE(sym) _hooks
#define ZEKE_HOOK_DECL_STRUCT(sym,members) typedef struct { members } ZEKE_HOOK_STRUCT_TYPE(sym)

/* Embed a hook structure in its parent, example:

   typedef struct zeke_foo_t {
      apr_pool_t *pool;
      ZEKE_HOOK_STRUCT(foo,
        ZEKE_HOOK_LINK(foo,a_hookable_function);
        ZEKE_HOOK_LINK(foo,another_function);
      );
      ...
   }
*/
#define ZEKE_HOOK_STRUCT(sym,members) struct { members } _hooks

/* IMPLEMENTATION MACROS */
#define ZEKE_IMPLEMENT_EXTERNAL_HOOK_BASE(vis,sym,name) \
ZEKE_PRIVATE(void) zeke_##sym##_sort_##name##_hook(const ZEKE_TYPE(sym) *self) \
{ \
  zeke_hook_sort(zeke_##sym##_pool_get(self),self,APR_STRINGIFY(name)); \
} \
\
vis(void) ZEKE_HOOK_SYMBOL(zeke_hook,sym,name)(ZEKE_TYPE(sym) *self, \
                                      ZEKE_HOOK_TYPE(sym,name) *pf, \
                                      const char * namespace, \
                                      const char * const *aszPre, \
                                      const char * const *aszSucc, \
                                      int nOrder) \
{ \
  ZEKE_HOOK_LINK_TYPE(sym,name) *pHook; \
  if(!self->_hooks.ZEKE_HOOK_LINK_VAR(sym,name)) { \
    self->_hooks.ZEKE_HOOK_LINK_VAR(sym,name) = \
                apr_array_make(zeke_##sym##_pool_get(self),1, \
                               sizeof(ZEKE_HOOK_LINK_TYPE(sym,name))); \
    zeke_hook_sort_register(zeke_##sym##_pool_get(self), self, \
                            APR_STRINGIFY(name), \
                            &self->_hooks.ZEKE_HOOK_LINK_VAR(sym,name)); \
  } \
  pHook = apr_array_push(self->_hooks.ZEKE_HOOK_LINK_VAR(sym,name)); \
  pHook->pFunc = pf; \
  pHook->aszPredecessors = aszPre; \
  pHook->aszSuccessors = aszSucc; \
  pHook->nOrder = nOrder; \
  pHook->szName = namespace ? namespace : APR_STRINGIFY(name); \
  if(apr_hook_debug_enabled) \
    apr_hook_debug_show(APR_STRINGIFY(name),aszPre,aszSucc); \
}

#define ZEKE_IMPLEMENT_HOOK_GET(vis,sym,name) \
ZEKE_IMPLEMENT_HOOK_GET_PROTO_VIS(vis,sym,name) { \
  return self->_hooks.ZEKE_HOOK_TYPE_VAR(link,sym,name); \
}
#define ZEKE_IMPLEMENT_HOOK_STATIC_GET(sym,name) \
static ZEKE_IMPLEMENT_HOOK_GET_PROTO(sym,name) { \
  return self->_hooks.ZEKE_HOOK_TYPE_VAR(link,sym,name); \
}
#define ZEKE_IMPLEMENT_HOOK_VOID(vis,sym,name,args_decl,args_use) \
ZEKE_IMPLEMENT_EXTERNAL_HOOK_BASE(vis,sym,name) \
vis(void) zeke_##sym##_run_##name##_hook args_decl \
{ \
  ZEKE_HOOK_LINK_TYPE(sym,name) *pHook; \
  int n; \
\
  if(!self->_hooks.ZEKE_HOOK_LINK_VAR(sym,name)) \
    return; \
\
  pHook = (ZEKE_HOOK_LINK_TYPE(sym,name) *)self->_hooks.ZEKE_HOOK_LINK_VAR(sym,name)->elts; \
  for(n = 0; n < self->_hooks.ZEKE_HOOK_LINK_VAR(sym,name)->nelts; n++) \
    pHook[n].pFunc args_use; \
}

#define ZEKE_IMPLEMENT_PUBLIC_HOOK_VOID(sym,name,args_decl,args_use) \
        ZEKE_IMPLEMENT_HOOK_VOID(ZEKE_API,sym,name,args_decl,args_use)
#define ZEKE_IMPLEMENT_PRIVATE_HOOK_VOID(sym,name,args_decl,args_use) \
        ZEKE_IMPLEMENT_HOOK_VOID(ZEKE_PRIVATE,sym,name,args_decl,args_use)

#define ZEKE_IMPLEMENT_HOOK_RUN_ALL(vis,ret,sym,name,args_decl,args_use,ok,decline,def) \
ZEKE_IMPLEMENT_EXTERNAL_HOOK_BASE(vis,sym,name) \
vis(ret) zeke_##sym##_run_##name##_hook args_decl \
{ \
  ZEKE_HOOK_LINK_TYPE(sym,name) *pHook; \
  int n; \
  ret rv; \
\
  if(!self->_hooks.ZEKE_HOOK_LINK_VAR(sym,name)) \
    return (def); \
\
  pHook = (ZEKE_HOOK_LINK_TYPE(sym,name) *)self->_hooks.ZEKE_HOOK_LINK_VAR(sym,name)->elts; \
  for(n = 0; n < self->_hooks.ZEKE_HOOK_LINK_VAR(sym,name)->nelts; n++) { \
    rv = pHook[n].pFunc args_use; \
    if(rv != (ok) && rv != (decline)) \
      return rv; \
  } \
  return def; \
}

#define ZEKE_IMPLEMENT_PUBLIC_HOOK_RUN_ALL(ret,sym,name,args_decl,args_use,ok,decline,def) \
        ZEKE_IMPLEMENT_HOOK_RUN_ALL(ZEKE_API,ret,sym,name,args_decl,args_use,ok,decline,def)
#define ZEKE_IMPLEMENT_PRIVATE_HOOK_RUN_ALL(ret,sym,name,args_decl,args_use,ok,decline,def) \
        ZEKE_IMPLEMENT_HOOK_RUN_ALL(ZEKE_PRIVATE,ret,sym,name,args_decl,args_use,ok,decline,def)

#define ZEKE_IMPLEMENT_HOOK_RUN_FIRST(vis,ret,sym,name,args_decl,args_use,decline,def) \
ZEKE_IMPLEMENT_EXTERNAL_HOOK_BASE(vis,sym,name) \
vis(ret) zeke_##sym##_run_##name##_##hook args_decl \
{ \
  ZEKE_HOOK_LINK_TYPE(sym,name) *pHook; \
  int n; \
  ret rv; \
\
  if(!self->_hooks.ZEKE_HOOK_LINK_VAR(sym,name)) \
    return (def); \
\
  pHook = (ZEKE_HOOK_LINK_TYPE(sym,name) *)self->_hooks.ZEKE_HOOK_LINK_VAR(sym,name)->elts; \
  for(n = 0; n < self->_hooks.ZEKE_HOOK_LINK_VAR(sym,name)->nelts; n++) { \
    rv = pHook[n].pFunc args_use; \
    if(rv != (decline)) \
      return rv; \
  } \
  return (def); \
}

#define ZEKE_IMPLEMENT_PUBLIC_HOOK_RUN_FIRST(ret,sym,name,args_decl,args_use,decline,def) \
        ZEKE_IMPLEMENT_HOOK_RUN_FIRST(ZEKE_API,ret,sym,name,args_decl,args_use,decline,def)
#define ZEKE_IMPLEMENT_PRIVATE_HOOK_RUN_FIRST(ret,sym,name,args_decl,args_use,decline,def) \
        ZEKE_IMPLEMENT_HOOK_RUN_FIRST(ZEKE_PRIVATE,ret,sym,name,args_decl,args_use,decline,def)

/* Convenience mirroring of apr hook orderings */
#define ZEKE_HOOK_REALLY_FIRST APR_HOOK_REALLY_FIRST
#define ZEKE_HOOK_FIRST APR_HOOK_FIRST
#define ZEKE_HOOK_MIDDLE APR_HOOK_MIDDLE
#define ZEKE_HOOK_LAST APR_HOOK_LAST
#define ZEKE_HOOK_REALLY_LAST APR_HOOK_REALLY_LAST
#define ZEKE_HOOK_DEFAULT_ORDER APR_HOOK_MIDDLE

/**********************************************************************************************
 * global hooks (standard APR hooks)
 **********************************************************************************************/

#define ZEKE_GLOBAL_HOOK_DECLARE(type) type
#define ZEKE_IMPLEMENT_GLOBAL_HOOK_GET_PROTO(name) \
  APR_IMPLEMENT_HOOK_GET_PROTO(zeke,ZEKE_PRIVATE,name)

#define ZEKE_DECL_GLOBAL_HOOK(link,ret,name,args) link##_DECLARE(ret) zeke_run_##name args

#define ZEKE_EXPORT_GLOBAL_HOOK(ret,name,args) \
typedef ret zeke_HOOK_##name##_t args; \
ZEKE_EXPORT void zeke_hook_##name(zeke_HOOK_##name##_t *pf, \
                                  const char * const *aszPre, \
                                  const char * const *aszSucc, int nOrder); \
ZEKE_EXPORT APR_IMPLEMENT_HOOK_GET_PROTO(zeke,ZEKE_GLOBAL_HOOK,name); \
typedef struct zeke_LINK_##name##_t { \
  zeke_HOOK_##name##_t *pFunc; \
  const char *szName; \
  const char * const *aszPredecessors; \
  const char * const *aszSuccessors;; \
  int nOrder; \
} zeke_LINK_##name##_t

#define ZEKE_IMPLEMENT_EXTERNAL_GLOBAL_HOOK_BASE(name) \
  APR_IMPLEMENT_EXTERNAL_HOOK_BASE(zeke,ZEKE_API,name)
#define ZEKE_IMPLEMENT_EXTERNAL_GLOBAL_HOOK_VOID(name,args_decl,args_use) \
  APR_IMPLEMENT_EXTERNAL_HOOK_VOID(zeke,ZEKE_API,name,args_decl,args_use)
#define ZEKE_IMPLEMENT_EXTERNAL_GLOBAL_HOOK_RUN_ALL(ret,name,args_decl,args_use,ok,decline) \
  APR_IMPLEMENT_EXTERNAL_HOOK_RUN_ALL(zeke,ZEKE_API,ret,name,args_decl,args_use,ok,decline)
#define ZEKE_IMPLEMENT_EXTERNAL_GLOBAL_HOOK_RUN_FIRST(ret,name,args_decl,args_use,decline) \
  APR_IMPLEMENT_EXTERNAL_HOOK_RUN_FIRST(zeke,ZEKE_API,ret,name,args_decl,args_use,decline)

/* Prototypes */

/* Hooks are registered via a (pool,key) tuple. The pool must be at least as long lived
   as the hook struct containing object ("self"). Key can be any unique pointer, but it is
   usually "self". Note that key is NOT dereferenced -- its integer value alone is used as
   the key.
*/
ZEKE_EXPORT void zeke_hook_sort_register(apr_pool_t *pool,
                                         const void *key,
                                         const char *hookName,
                                         apr_array_header_t **aHooks);
ZEKE_EXPORT void zeke_hook_sort(apr_pool_t *pool, const void *key, const char *hookName);
ZEKE_EXPORT void zeke_hook_sort_all(apr_pool_t *pool, const void *key);
ZEKE_EXPORT void zeke_hook_deregister(apr_pool_t *pool, const void *key, const char *name);

#define ZEKE_HOOK_KEY(module) APR_STRINGIFY(module)
#define ZEKE_HOOK_EX(module,name,obj,fn,...) \
  zeke_hook_##module##_##name (obj,fn,ZEKE_HOOK_KEY(module), ## __VA_ARGS__)
#define ZEKE_HOOK(module,name,obj,fn) \
  zeke_hook_##module##_##name (obj,fn,ZEKE_HOOK_KEY(module),NULL,NULL,ZEKE_HOOK_DEFAULT_ORDER)

#define ZEKE_HOOK_CONNECTION(name,obj,fn) ZEKE_HOOK(connection,name,obj,fn)
#define ZEKE_HOOK_CONNECTION_EX(name,obj,fn,...) ZEKE_HOOK_EX(connection,name,obj,fn, ## __VA_ARGS__)

/* Global hooks: maintenance */
/* Always call zeke_global_hooks_are_modifiable() before calling any hook function below,
 * only perform modifications if this function returns !0.
 */
ZEKE_EXPORT int zeke_global_hooks_are_modifiable(void);
/* Sort or re-sort global hooks. If hooks don't need restorting and resort == 0, this call
 * is a noop
 */
ZEKE_EXPORT void zeke_global_hooks_sort(int resort);
/* Mark all global hooks as requiring a re-sort */
ZEKE_EXPORT void zeke_global_hooks_modified(void);

/* Global hooks: actual hooks */
ZEKE_EXPORT_GLOBAL_HOOK(void,new_connection,(zeke_connection_t*));
ZEKE_EXPORT_GLOBAL_HOOK(int,shutdown,(int *exit_code));

#ifdef LIBZEKE_USE_LIBEVENT
#include <libzeke_event.h>
ZEKE_EXPORT_GLOBAL_HOOK(void,event_loop_begin,(struct event_base*,apr_pool_t *tmp));
ZEKE_EXPORT_GLOBAL_HOOK(void,event_loop_end,(struct event_base*,apr_pool_t *tmp));
#endif /* LIBZEKE_USE_LIBEVENT */

#ifdef __cplusplus
}
#endif

#endif /* _LIBZEKE_HOOKS_H */
