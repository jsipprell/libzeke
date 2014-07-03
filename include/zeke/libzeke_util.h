#ifndef _LIBZEKE_UTIL_H
#define _LIBZEKE_UTIL_H

#include <zeke/libzeke.h>

#ifdef __cpluscplus
extern "C"
{
#endif

typedef struct zeke_regex_t zeke_regex_t;

/* Misc assorted libzeke api prototypes that don't really fit anywhere else */

/* Sort a table by key returning a new sorted table allocated
from the provided pool. If no compare function is passed comparison
is strictly standard strcmp() (ie case sensitive). Keys that are NULL
are discarded. */
ZEKE_EXPORT apr_table_t *zeke_table_sort(const apr_table_t *src,
                                  zeke_sort_compare_fn compare,
                                  apr_pool_t*,
                                  zeke_context_t*);


/* Sort an array of char* in-place. The actual data in each string is not
  modified. If either a compare function nor a regular expression are provided
  a generic expression will be used to sort on sequence numbers at the end
  of each string in the format: _00000000 */
ZEKE_EXPORT apr_status_t zeke_array_sort(apr_array_header_t*,
                                  zeke_sort_compare_fn compare,
                                  zeke_regex_t*,
                                  apr_pool_t*,
                                  zeke_context_t*);
#define zeke_array_sort_seq(a,p) zeke_array_sort((a),NULL,NULL,(p),NULL)

/* Return the index of a table as an array allocated from the given pool.
If sort is !0 the keys will be sorted in alpha case-sensitive order.

NOTE: This __always__ produces a shallow index!! */
ZEKE_EXPORT apr_array_header_t *zeke_table_index(const apr_table_t *t, int sort,
                                                 apr_pool_t*);


/* Extended version of the above, uses a custom compare function (which is called as
compare(key1,key2,context);) to perform any desired sorting. If shallow != 0 the
keys will not be duplicated into the memory pool. */
ZEKE_EXPORT apr_array_header_t *zeke_table_index_ex(const apr_table_t *t,
                                                  int sort, int shallow,
                                                  zeke_sort_compare_fn compare,
                                                  apr_pool_t*,
                                                  zeke_context_t*);

/* apr pool cleanup function that will set a pointer to a pointer NULL but check
   to make sure the pointer is !NULL first */
ZEKE_EXPORT zk_status_t zeke_indirect_wipe(void*);
#define zk_indirect_wipe zeke_indirect_wipe

/* identical to apr_pool_cleanup_register except that a null handle is always
 * used for the child pre-exec cleanup. Useful for the common case which is
 * where one doesn't care about cleanup pre-exec
 */
ZEKE_EXPORT void zeke_pool_cleanup_register(apr_pool_t*,
                                            const void*,
                                            apr_status_t (*cleanup)(void*));
#define zeke_pool_cleanup_indirect(p,vp) zeke_pool_cleanup_register((p),&(vp),\
                                                          zeke_indirect_wipe)
#define zeke_pool_pre_cleanup_indirect(p,vp) apr_pool_pre_cleanup_register((p),&(vp),\
                                                          zeke_indirect_wipe)
#define zeke_pool_cleanup_indirect_kill(p,vp) apr_pool_cleanup_kill((p),&(vp),\
                                                          zeke_indirect_wipe)
/* create a new zookeeper nodepath, the root node path is always "/" unless it has
   been changed via a call to zeke_nodepath_chroot(). Each parent in the hierarchy
   is demarcated by a "/". Literal '/' characters are forbidden but except at
   the beginning or end of node names (in which case they are silently removed).

   !! Always pass a NULL as the final argument to terminate the nodepath. */
ZEKE_EXPORT_SENTINEL const char *zeke_nodepath_join(apr_pool_t *pool, ...);
ZEKE_EXPORT const char *zeke_nodepath_joinv(apr_pool_t *pool, va_list);

/* Set chrooted nodepath */
ZEKE_EXPORT apr_status_t zeke_nodepath_chroot_set(const char*);
/* Return chrooted nodepath (if any) */
ZEKE_EXPORT const char *zeke_nodepath_chroot_get(void);

/* util to get somea idea of the best/current fqdn */
ZEKE_EXPORT const char *zeke_get_fqdn(apr_pool_t*);

/* Formatter for zookeeper log output */
ZEKE_EXPORT const char *zeke_format_log_message(const char *fmt, ...)
                 ZEKE_PRINTF(1,2);

/* Wrappers around apr_pool_tag */
ZEKE_EXPORT void zeke_pool_tag_ex(apr_pool_t*, const char *fmt, ...)
                 ZEKE_PRINTF(2,3);

ZEKE_EXPORT void zeke_pool_tag(apr_pool_t*, const char *tag);
ZEKE_EXPORT const char *zeke_pool_tag_get(apr_pool_t*);

#ifdef __cplusplus
}
#endif

#endif /* _LIBZEKE_UTIL_H */
