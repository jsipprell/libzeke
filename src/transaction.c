#include "internal.h"
#include "transaction.h"
#include "dispatch.h"

/* high-level zookeeper transaction api */
struct zeke_transaction {
  apr_pool_t *pool;
  apr_uint16_t self_owned_pool;
  apr_time_t start,end;
  apr_size_t nops,nreqs;
  apr_array_header_t *ops;
  apr_array_header_t *results;
  apr_size_t buflen;
  zeke_callback_fn callback;
  const zeke_context_t *context;
};

ZEKE_POOL_IMPLEMENT_ACCESSOR(transaction)

#define ZEKE_TRANS_PUSH(ary,type) ((type)apr_array_push(ary))

ZEKE_API(zk_status_t) zeke_transaction_create(zeke_transaction_t **trans,
                                              apr_size_t hint,
                                              apr_pool_t *pool)
{
  return zeke_transaction_create_ex(trans,hint,pool,(pool ? 0 : 1));
}

ZEKE_API(zk_status_t) zeke_transaction_create_ex(zeke_transaction_t **trans,
                                                 apr_size_t hint,
                                                 apr_pool_t *pool,
                                                 int use_subpool)
{
  zk_status_t st = APR_SUCCESS;

  if(pool == NULL) {
    assert((pool = zeke_root_subpool_create()));
    use_subpool++;
  } else if(use_subpool) {
    apr_pool_t *subpool;
    use_subpool = 0;
    if((st = apr_pool_create(&subpool,pool)) != APR_SUCCESS)
      goto trans_create_exit;
    use_subpool++;
  }

  if(hint <= 0)
    hint = 1;

  assert(trans != NULL);
  *trans = apr_pcalloc(pool,sizeof(zeke_transaction_t));
  assert(*trans != NULL);
  (*trans)->pool = pool;
  (*trans)->self_owned_pool = use_subpool;
  (*trans)->ops = apr_array_make(pool,hint+1,sizeof(zoo_op_t));
  assert((*trans)->ops != NULL);
  (*trans)->buflen = DEFAULT_BUFLEN;

trans_create_exit:
  if(st != APR_SUCCESS) {
    if(use_subpool)
      apr_pool_destroy(pool);
  }
  return st;
}

ZEKE_API(void) zeke_transaction_context_set(zeke_transaction_t *trans,
                                                       const zeke_context_t *ctx)
{
  trans->context = ctx;
}

ZEKE_API(zeke_context_t*) zeke_transaction_context(const zeke_transaction_t *trans)
{
  return (zeke_context_t*)trans->context;
}

ZEKE_API(zk_status_t) zeke_transaction_destroy(zeke_transaction_t *trans)
{
  zk_status_t st = APR_SUCCESS;
  if(trans->self_owned_pool) {
    apr_pool_t *pool = trans->pool;
    trans->pool = NULL;
    apr_pool_destroy(pool);
  } else {
    if(trans->ops)
      apr_array_clear(trans->ops);
    if(trans->results)
      apr_array_clear(trans->results);
    trans->nops = 0;
    trans->nreqs = 0;
  }

  return st;
}

ZEKE_API(apr_size_t) zeke_transaction_buflen_get(const zeke_transaction_t *trans)
{
  return trans->buflen;
}

ZEKE_API(void) zeke_transaction_buflen_set(zeke_transaction_t *trans,
                                           apr_size_t buflen)
{
  assert(buflen > 0);
  trans->buflen = buflen;
}

ZEKE_API(void) zeke_transaction_require_addn(zeke_transaction_t *trans,
                                             const char *path,
                                             int required_version)
{
  zoo_op_t *op;
  assert(trans != NULL);

  assert(trans->ops != NULL);
  op = ZEKE_TRANS_PUSH(trans->ops,zoo_op_t*);
  assert(op != NULL);
  zoo_check_op_init(op,path,required_version);
  trans->nreqs++;
}

ZEKE_API(void) zeke_transaction_require_add(zeke_transaction_t *trans,
                                            const char *path,
                                            int required_version)
{
  zeke_transaction_require_addn(trans,apr_pstrdup(trans->pool,path),
                                required_version);
}

ZEKE_API(zk_status_t) zeke_trans_op_create_add(zeke_transaction_t *trans,
                                              const char *path,
                                              const char *value,
                                              apr_ssize_t valuelen,
                                              const struct ACL_vector *acl,
                                              int flags,
                                              int copy)
{
  char *path_buffer;
  zoo_op_t *op;
  zk_status_t st = ZEOK;
  assert(trans != NULL);
  
  if(copy)
    path = apr_pstrdup(trans->pool,path);
  
  path_buffer = apr_palloc(trans->pool,trans->buflen);
  assert(path_buffer != NULL);
  if(value == NULL)
    valuelen = -1;
  if(acl == NULL)
    acl = &ZOO_OPEN_ACL_UNSAFE;
  op = ZEKE_TRANS_PUSH(trans->ops,zoo_op_t*);
  assert(op != NULL);
  zoo_create_op_init(op,path,value,valuelen,acl,flags,path_buffer,trans->buflen);
  trans->nops++;
  return st;
}

ZEKE_API(zk_status_t) zeke_trans_op_delete_add(zeke_transaction_t *trans,
                                               const char *path,
                                               int version,
                                               int copy)
{
  zoo_op_t *op;
  zk_status_t st = ZEOK;
  assert(trans != NULL);

  if(copy)
    path = apr_pstrdup(trans->pool,path);

  op = ZEKE_TRANS_PUSH(trans->ops,zoo_op_t*);
  assert(op != NULL);
  zoo_delete_op_init(op,path,version);
  trans->nops++;
  return st;
}

ZEKE_API(zk_status_t) zeke_trans_op_set_add(zeke_transaction_t *trans,
                                            const char *path,
                                            const char *value,
                                            apr_ssize_t valuelen,
                                            int version,
                                            int copy)
{
  zoo_op_t *op;
  zk_status_t st = ZEOK;
  struct Stat *stat;

  assert(trans != NULL);

  if(copy)
    path = apr_pstrdup(trans->pool,path);
  if(value == NULL)
    valuelen = -1;
  stat = apr_pcalloc(trans->pool,sizeof(struct Stat));
  assert(stat != NULL);
  op = ZEKE_TRANS_PUSH(trans->ops,zoo_op_t*);
  assert(op != NULL);
  zoo_set_op_init(op,path,value,valuelen,version,stat);
  trans->nops++;
  return st;
}

static zeke_cb_t trans_callback(const zeke_callback_data_t *cbd)
{
  zeke_transaction_t *trans = (zeke_transaction_t*)cbd->ctx;

  trans->end = apr_time_now();
  if(cbd->status == ZEOK) {
    int i;
    if(trans->results == NULL)
      trans->results = apr_array_make(cbd->pool,trans->ops->nelts,
                                        sizeof(zoo_op_result_t));
    else
      apr_array_clear(trans->results);
    assert(trans->results != NULL);
    for(i = 0; i < trans->ops->nelts; i++) {
      memcpy(ZEKE_TRANS_PUSH(trans->results,zoo_op_result_t*),
            (cbd->vectors.results+i),sizeof(zoo_op_result_t));
    }
  }
  
  ((zeke_callback_data_t*)cbd)->ctx = trans;
  if(trans->callback)
    return trans->callback(cbd);

  return ZEKE_CALLBACK_DESTROY;
}

ZEKE_API(apr_size_t) zeke_transaction_num_results(const zeke_transaction_t *trans)
{
  return (trans->results ? trans->results->nelts : 0);
}

ZEKE_API(apr_size_t) zeke_transaction_num_ops(const zeke_transaction_t *trans)
{
  return trans->ops->nelts;
}

ZEKE_API(const apr_array_header_t*) zeke_transaction_results(const zeke_transaction_t *trans)
{
  return trans->results;
}

ZEKE_API(zk_status_t) zeke_transaction_run(zeke_transaction_t *trans,
                                           const zeke_connection_t *conn,
                                           zeke_callback_fn callback)
{
  zk_status_t st = ZEOK;
  zeke_callback_data_t *cbd;

  if(trans->nops == 0) {
    zk_eprintf("no operations added to transaction %lx\n",
              (unsigned long)trans);
    abort();
  }
  if(!trans->results) {
    trans->results = apr_array_make(trans->pool,trans->nops+trans->nreqs,
                                    sizeof(zoo_op_result_t));
    assert(trans->results != NULL);
  }

  cbd = zeke_callback_data_create_ex(conn,NULL,trans->pool,
                                    !trans->self_owned_pool);
  assert(cbd != NULL);
  trans->callback = callback;
  trans->start = apr_time_now();
  st = zeke_amulti(&cbd,conn,trans->ops,trans_callback,trans);
  if(st != ZEOK)
    zeke_callback_data_destroy(cbd);
  return st;
}
