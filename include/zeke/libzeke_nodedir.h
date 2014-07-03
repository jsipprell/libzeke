#ifndef _LIBZEKE_NODEDIR_H_
#define _LIBZEKE_NODEDIR_H_

#include <zeke/libzeke.h>
#include <zeke/libzeke_session.h>

#ifdef __cplusplus
extern "C"
{
#endif

/* high-level interface which will asyncronously fill in a table with the names
   of a childen nodes as keys and their contents as values. It is not a tree, but
   a representation of a single node's children */
typedef struct zeke_nodedir_t zeke_nodedir_t;

ZEKE_POOL_EXPORT_ACCESSOR(nodedir);

/* If these return !0, the nodedir is presumed to be still valid and operational (perhaps
having been restarted) */
typedef int (*zeke_nodedir_complete_fn)(const zeke_nodedir_t*,apr_table_t*,zeke_context_t*);
typedef int (*zeke_nodedir_timeout_fn)(const zeke_nodedir_t*,apr_interval_time_t,zeke_context_t*);

ZEKE_EXPORT zk_status_t zeke_nodedir_create(zeke_nodedir_t**,const char *root,
                                            apr_interval_time_t timeout,
                                            zeke_nodedir_complete_fn completed,
                                            zeke_nodedir_timeout_fn timedout,
                                            zeke_context_t *context,
                                            const zeke_connection_t *conn);

ZEKE_EXPORT zk_status_t zeke_nodedir_cancel(const zeke_nodedir_t*);
ZEKE_EXPORT zk_status_t zeke_nodedir_restart(const zeke_nodedir_t*);

/* Destroy a nodedir, releasing all objects associated with its memory pool */
ZEKE_EXPORT zk_status_t zeke_nodedir_destroy(zeke_nodedir_t*);

/* Return the total request time in usec. If called on a nodedir request still being processed, 
returns total time at that moment (i.e. will stop incrementing once operations are completed) */
ZEKE_EXPORT apr_interval_time_t zeke_nodedir_request_time_get(const zeke_nodedir_t*);

/* ZooKeeper nodes can have a special "null" content set, meaning no data and no length.
Nodedir displays this as "(null)" in the apr_table_t, but this function can be used to test
if a particular entry is really null. */
ZEKE_EXPORT int zeke_nodedir_node_is_null(const apr_table_t*, const char *name);

/* Return the status of a _completed_ nodedir operation. If called prior to completion, results
are indeterminate. */
ZEKE_EXPORT zk_status_t zeke_nodedir_status(const zeke_nodedir_t*);

/* Returns TRUE if the nodedir operations are all complete, otherwise FALSE. Unless otherwise
noted, it is not safe to call any other nodedir API functions if this does not return TRUE. */
ZEKE_EXPORT int zeke_nodedir_is_complete(const zeke_nodedir_t*);

/* Construct a new hash with node names as keys and node contents as values. For ZK nodes with no internal data/content,
their value in the hash will be set to the null_value argument (which can be passed a sentinel to represent NULL). */
ZEKE_EXPORT apr_hash_t *zeke_nodedir_hash_make(const zeke_nodedir_t*, const char *null_value, apr_pool_t *pool);

/* Returns the context which the nodedir was created with. This is the same opaque context which is passed to
completed and timedout callbacks. */
ZEKE_EXPORT zeke_context_t *zeke_nodedir_context(const zeke_nodedir_t*);

/* Change the opaque context pointer which is passed back to completed and timedout callbacks. */
ZEKE_EXPORT void zeke_nodedir_context_set(zeke_nodedir_t*, zeke_context_t*);

#ifdef __cplusplus
}
#endif

#endif /* _LIBZEKE_NODEDIR_H_ */
