/* internal zeke prototypes and macros */
#ifndef _ZEKE_INTERNAL_H
#define _ZEKE_INTERNAL_H

#include "libzeke.h"

#define ZEKE_API(type) type
#define ZEKE_PRIVATE(type) ZEKE_HIDDEN type

#define ZEKE_POOL_IMPLEMENT_ACCESSOR(type) \
  ZEKE_API(apr_pool_t*) zeke_##type##_pool_get(const zeke_##type##_t* type##_object) \
    { return type##_object->pool; }

#define ZEKE_IMPLEMENT_GENERIC_ACCESSOR(type,rtype,field,cls) \
  ZEKE_API(cls zeke_##rtype##_t *) zeke_##type##_##rtype##_get(const zeke_##type##_t* type##_object) \
    { return type##_object->field; }

#define ZEKE_IMPLEMENT_ACCESSOR(type,rtype,field) ZEKE_IMPLEMENT_GENERIC_ACCESSOR(type,rtype,field,)
#define ZEKE_IMPLEMENT_CONST_ACCESSOR(type,rtype,field) ZEKE_IMPLEMENT_GENERIC_ACCESSOR(type,rtype,field,const)

#define zk_printf(format, ...) apr_file_printf(zstdout, format, ## __VA_ARGS__)
#define zk_eprintf(format, ...) apr_file_printf(zstderr, format, ## __VA_ARGS__)

ZEKE_HIDDEN void zeke_init_indirects(apr_pool_t*);

#include "compat.h"

#include "libzeke_errno.h"
#include "libzeke_event_loop.h"
#include "libzeke_util.h"

#ifdef DEBUGGING
ZEKE_EXPORT void zeke_indirect_debug();
ZEKE_EXPORT void zeke_indirect_debug_banner(const char*);
#endif

#endif /* _ZEKE_INTERNAL_H */
