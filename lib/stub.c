/* empty stub for darwin which doesn't support empty static libs */
#include "internal.h"

#define LIBZEKE_MAGIC 0xf3bcdeaa049eac3b

ZEKE_API(apr_uint64_t) libzeke_version_magic(void) {
  return APR_UINT64_C(LIBZEKE_MAGIC) ^ ((((apr_uint64_t)LIBZEKE_MAJOR_VERSION) << 32) | 
                         ((apr_uint64_t)LIBZEKE_MINOR_VERSION & 0xffff0000LL));
}
