#ifndef _ZEKE_DISPATCH_H
#define _ZEKE_DISPATCH_H

#include "internal.h"

ZEKE_EXTERN void zeke_dispatch_init(apr_pool_t*);
ZEKE_EXTERN void zeke_dispatch_release(int);
ZEKE_EXTERN apr_pool_t *zeke_dispatch_subpool_create(void);

ZEKE_EXTERN zeke_watcher_cb_data_t *zeke_watcher_cb_data_create_ex(const zeke_connection_t*,
                                                                   const zeke_context_t*,
                                                                   apr_pool_t *pool,
                                                                   apr_pool_t *parent_pool);
ZEKE_EXTERN zeke_watcher_cb_data_t *zeke_watcher_cb_data_create(const zeke_connection_t*,
                                                               const zeke_context_t*);
ZEKE_EXTERN void zeke_watcher_cb_data_context_set(zeke_watcher_cb_data_t*,const zeke_context_t*);
ZEKE_EXTERN zk_status_t zeke_watcher_cb_data_destroy(zeke_watcher_cb_data_t*);


#endif /* _ZEKE_DISPATCH_H */
