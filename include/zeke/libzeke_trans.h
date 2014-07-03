#ifndef _LIBZEKE_TRANSACTION_H
#define _LIBZEKE_TRANSACTION_H

/* Public high-level zookeeper transaction API. */

#ifdef __cplusplus
extern "C"
{
#endif

/* Opaque transaction handle */
typedef struct zeke_transaction zeke_transaction_t;

ZEKE_POOL_EXPORT_ACCESSOR(transaction);

/* Convenience macro for use in place of APR_ARRAY_IDX on transaction results */
#define ZEKE_TRANSACTION_IDX(arr,i) \
 (const zoo_op_result_t*)(&APR_ARRAY_IDX((arr),(i),const zoo_op_result_t))

/* Create a new transaction */
ZEKE_EXPORT zk_status_t zeke_transaction_create_ex(zeke_transaction_t **trans,
                                                   apr_size_t size_hint,
                                                   apr_pool_t *pool,
                                                   int use_subpool);

/* Slightly simpler version */
ZEKE_EXPORT zk_status_t zeke_transaction_create(zeke_transaction_t **trans,
                                                apr_size_t size_hint,
                                                apr_pool_t *pool);

/* Explicitly destroy a transaction. It is not absolutely required that this be called,
   the apr memory pool system will take care of it as long a pools are being managed
   correctly */
ZEKE_EXPORT zk_status_t zeke_transaction_destroy(zeke_transaction_t *trans);


/* Transaction callback contexts are ALWAYS zeke_transaction_t*, but an addtional context
   can be set and retrived by the following two calls: */
ZEKE_EXPORT void zeke_transaction_context_set(zeke_transaction_t*,const zeke_context_t*);
ZEKE_EXPORT zeke_context_t *zeke_transaction_context(const zeke_transaction_t*);

/* Override the default zookeeper multioperation buffer size */
ZEKE_EXPORT apr_size_t zeke_transaction_buflen_get(const zeke_transaction_t*);
ZEKE_EXPORT void zeke_transaction_buflen_set(zeke_transaction_t*,apr_size_t);

/* Add a "transaction requirement", right now this just means that the data version of
   a node MUST be equal to the indicated version after all the operations have been
   sent and are pending "commit". If this is fails all operations in the transaction rollback
   and the transaction callback will be called with an apporiatel results set containing
   any errors. If the version requirement succeeds then all operations have been
   *attempted*, although there may still be individual errors per-operation */
ZEKE_EXPORT void zeke_transaction_require_addn(zeke_transaction_t*,
                                                      const char *path,
                                                      int required_version);
/* like zeke_transaction_require_add() but makes a copy of that path */
ZEKE_EXPORT void zeke_transaction_require_add(zeke_transaction_t*,
                                                     const char *path,
                                                     int required_version);

/* Add a node creation operation to the transaction */
ZEKE_EXPORT zk_status_t zeke_trans_op_create_add(zeke_transaction_t*,
                                                 const char *path,
                                                 const char *value,
                                                 apr_ssize_t valuelen,
                                                 const struct ACL_vector *acl,
                                                 int flags,
                                                 int copy);
#define zeke_trans_op_create_addn(t,p,v,vl,acl,fl) zeke_trans_op_create_add( (t), (p), (v), (vl), (acl), (fl), 0 )
ZEKE_EXPORT zk_status_t zeke_trans_op_delete_add(zeke_transaction_t*,
                                                 const char *path,
                                                 int version,
                                                 int copy);

/* Add a node deletion operation to the transaction */
#define zeke_trans_op_delete_addn(t,p,v) zeke_trans_op_delete_add( (t), (p), (v), 0 )
#define zeke_trans_op_delete_any_addn(t,p) zeke_trans_op_delete_add( (t), (p), -1, 0 )
ZEKE_EXPORT zk_status_t zeke_trans_op_set_add(zeke_transaction_t*,
                                              const char *path,
                                              const char *value,
                                              apr_ssize_t valuelen,
                                              int version,
                                              int copy);

/* Add a node contents set operation to the transaction */
#define zeke_trans_op_set_addn(t,p,v,vl,ve) zeke_trans_op_set_add( (t), (p), (v), (vl), (ve), 0 )
#define zeke_trans_op_set_any_addn(t,p,v,vl) zeke_trans_op_set_add( (t), (p), (v), (vl), -1, 0 )

/* This initiates the transaction. NO FURTHER OPERATIONS OR REQUIREMENTS CAN BE ADDED
   AFTER THIS IS CALLED. The transaction executes asyncronously and once completed the
   a standard zeke callback is called. The callback data argument's context will point
   to the related transaction, but callers can associate an additional private context
   via zeke_transaction_context_set() and fetch it via zeke_transaction_context_get().

   Note that there are two very different types of failures that may occur with a transaction:

   1. The transaction itself fails, in which case the status member of the callback data
      structure will indicate this as per normal.

   2. The transaction may be successful but one or more transaction operations failed
      (only required ops for a valid trans are "required versions"). In which case, it
      will be necessary to iterate throught the transaction results and examine each
      result code
*/
ZEKE_EXPORT zk_status_t zeke_transaction_run(zeke_transaction_t*,
                                             const zeke_connection_t*,
                                             zeke_callback_fn callback);

/* How many operations were performed or attempted */
ZEKE_EXPORT apr_size_t zeke_transaction_num_results(const zeke_transaction_t*);
/* How many operations (including require "versions") are wrapped inside the
   transaction. */
ZEKE_EXPORT apr_size_t zeke_transaction_num_ops(const zeke_transaction_t*);

/* Return completed transaction results as an array. Each array elemtent is 
   a zoo_op_result_t structure (NOT a pointer to said structure) */
ZEKE_EXPORT const apr_array_header_t *zeke_transaction_results(const zeke_transaction_t*);

#ifdef __cplusplus
}
#endif

#endif /* _LIBZEKE_TRANSACTION_H */

