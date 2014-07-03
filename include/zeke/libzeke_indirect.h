#ifndef _LIBZEKE_INDIRECT_H
#define _LIBZEKE_INDIRECT_H

#include <zeke/libzeke.h>

#ifdef __cplusplus
extern "C"
{
#endif

/* Indirects are "context proxies"; object which reside in a separate isolated
 * apr_pool_t and point to a "real" context. After creation they can be used
 * *exactly* once at which time they are considered abandoned. At regular
 * intervals, during consumption, the underlying apr pool will be cleared as to
 * release memory as long as no outstanding indirects exist.
*/
typedef struct zeke_indirect_t zeke_indirect_t;

/* This creates a new indirect proxying to a context of some arbirary kind. The
 * indirect is "bound" to a pool which should be a pool from which it was
 * allocated. When said pool is cleared or destroyed the indirect will be
 * considered a zombie; meaning that it will remain valid and in existence
 * (until) consumed but will return NULL when zeke_indirect_consume() is called
 * on it.
*/
ZEKE_EXPORT zeke_indirect_t *zeke_indirect_make(zeke_context_t *context, apr_pool_t *binding);

/* This consumes an indirect and returns whatever the indirect proxies to. !!
 * ONCE THIS IS CALLED THE INDIRECT CAN NEVER BE USED AGAIN !!
*
* Note that all callers MUST assume this may return NULL meaning that the
* memory which provided the original context has been released and thus said
* context no longer exists.
*/
ZEKE_EXPORT zeke_context_t *zeke_indirect_consume(zeke_indirect_t*);

/* Peek into an indirect and return the context it points to if the pool it is
 * bound to has not been destroyed or cleared.
 */
ZEKE_EXPORT zeke_context_t *zeke_indirect_peek(const zeke_indirect_t*);

/* Return an existing unconsumed indirect for a given *real* context and
 * binding pool. The indirect __cannot__ have been consumed yet although the
 * caller may consume it. If the indirect has already been consumed
 * (dereferenced), NULL will be returned.
 */
ZEKE_EXPORT zeke_indirect_t *zeke_indirect_find(zeke_context_t *context, apr_pool_t *binding);

/* Release an indirect for a given context and binding if one exists and has
 * not been consumed.  The return pointer is to the original object passed to
 * zeke_indirect_make(). This call atomically consumes/derefs the indirect and
 * wipes it out. Be careful not to use this if something else is holding a
 * pointer to the indirect itself (such as via apr_pool_cleanup_register()).
 */
ZEKE_EXPORT zeke_context_t *zeke_indirect_release(zeke_context_t *context, apr_pool_t *binding);

/* Indirection util for use with apr_pool_cleanup_register,
 * apr_pool_pre_cleanup_register, or zeke_pool_cleanup_register.
 *
 * When using this cleanup handler always register it with the data arg of a
 * zeke_indirect_t created via zeke_indirect_make(). The context (first arg)
 * should be a pointer to a pointer. When the APR calls
 * zeke_indirect_deref_wipe() as a normal part of calling the cleanup chain
 * prior to pool clear or destruction, libzeke will dereference the indirect
 * (via a single clal to zeke_indirect_consume).  If it discovers the deref is
 * still valid it will set the pointer originally pointed at to null. This
 * allows for the creation of a simple design pattern: storing a pointer in one
 * struct in another struct where each struct is allocated from unrelated
 * pools.  When the pointed-to struct's pool is released the pointer in the
 * pointed-from struct will become NULL, *but without the standard problem of
 * needing to worry about the pointed-to stucture still being valid*.
 *
 * Example usage:
 *
 * struct struct_a;
 * struct struct_b;
 *
 * struct struct_a {
 *    apr_pool_t *pool;
 *    struct struct_b *b;
 * };
 *
 * struct struct_b {
 *    apr_pool_t *pool;
 *    int some_data;
 * };
 *
 * // assumption: the relationship between poola and poolb is unknown.
 * struct struct_a *obj_a = apr_palloc(poola,sizeof(struct struct_a));
 * struct struct_b *obj_b = apr_palloc(poolb,sizeof(struct struct_b));
 * obj_a->pool = poola;
 * obj_a->b = objb;
 * obj_b->pool = poolb;
 * obj_b->some_data = 42;
 *
 * // Magic happens here:
 * zeke_indirect_t *ref = zeke_indirect_make(&obj_a->b,obj_a->pool);
 * // Note the use of obj_a's pool. This is critical to ensure
 * // the indirect doesn't return an invalid pointer if A's pool
 * // is released.
 * apr_pool_pre_cleanup_register(poolb,ref,zeke_indirect_deref_wipe);
 *
 * // Iff poola is destroyed before poolb, zeke_indirect_deref_wipe will do
 * // nothing.
 * // Iff poolb is destroyed before poola, zeke_indirect_deref_wipe will set
 * // obj_a->b = NULL;
 */
ZEKE_EXPORT apr_status_t zeke_indirect_deref_wipe(void*);

#ifdef __cplusplus
}
#endif

#endif /* _LIBZEKE_INDIRECT_H */
