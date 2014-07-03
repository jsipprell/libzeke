#ifndef _LIBZEKE_TREE
#define _LIBZEKE_TREE

#include <zeke/libzeke.h>
#include <zeke/libzeke_session.h>
#include <apr_ring.h>

#ifdef __cplusplus
extern "C"
{
#endif

#define ZKTREE_ENTRY APR_RING_ENTRY(zeke_tree_node)
#define ZKTREE_HEAD_TYPE(name) zeke_tree_##name
#define ZKTREE_HEAD_DECL(name) APR_RING_HEAD(ZKTREE_HEAD_TYPE(name),zeke_tree_node)
#define ZKTREE_HEAD_VAR(name) struct ZKTREE_HEAD_TYPE(name)
#define ZKTREE_SENTINEL(hp) APR_RING_SENTINEL((hp),zeke_tree_node,l)
#define ZKTREE_FIRST(hp) APR_RING_FIRST(hp)
#define ZKTREE_LAST(hp) APR_RING_LAST(hp)
#define ZKTREE_NEXT(ep) APR_RING_NEXT((ep),l)
#define ZKTREE_PREV(ep) APR_RING_PREV((ep),l)
#define ZKTREE_INIT(hp) APR_RING_INIT((hp),zeke_tree_node,l)
#define ZKTREE_EMPTY(hp) APR_RING_EMPTY((hp),zeke_tree_node,l)
#define ZKTREE_INSERT_BEFORE(lep,nep) APR_RING_INSERT_BEFORE((lep),(nep),l)
#define ZKTREE_INSERT_AFTER(lep,nep) APR_RING_INSERT_AFTER((lep),(nep),l)
#define ZKTREE_INSERT_HEAD(hp,ep) APR_RING_INSERT_HEAD((hp),(ep),zeke_tree_node,l)
#define ZKTREE_INSERT_TAIL(hp,ep) APR_RING_INSERT_TAIL((hp),(ep),zeke_tree_node,l)
#define ZKTREE_REMOVE(ep) APR_RING_REMOVE((ep),l)
#define ZKTREE_FOREACH(ep,hp) APR_RING_FOREACH((ep),(hp),zeke_tree_node,l)
#define ZKTREE_FOREACH_SAFE(ep,state,hp) APR_RING_FOREACH_SAFE((ep),(state),(hp),zeke_tree_node,l)
#define ZKTREE_CHECK_CONSISTENCY(hp) zeke_tree_check_consistency(hp)
#define ZKTREE_ELEM_IS_ORPHAN(ep) (APR_RING_NEXT((ep),l) == (ep) && \
                                   APR_RING_PREV((ep),l) == (ep))

enum {
  zeke_tree_filter_discard = 0,
#define ZKTFILTER_DISCARD zeke_tree_filter_discard
  zeke_tree_filter_keep = 0x0100,
#define ZKTFILTER_KEEP zeke_tree_filter_keep
  zeke_tree_filter_repl_name = 0x0001,
#define ZKTFILTER_REPLACE_NAME zeke_tree_filter_repl_name
  zeke_tree_filter_repl_absname = 0x0002,
#define ZKTFILTER_REPLACE_ABSNAME zeke_tree_filter_repl_absname
  zeke_tree_filter_stop_descent = 0x0300,
#define ZKTFILTER_STOP_DESCENT zeke_tree_filter_stop_descent
};

typedef enum {
  zeke_tree_iter_top,
  zeke_tree_iter_bottom
} zeke_tree_iter_type_e;

typedef struct zeke_tree_iter zeke_tree_iter_t;
typedef struct zeke_tree_ring zeke_tree_ring_t;
typedef struct zeke_tree_ring zeke_tree_head_t;
typedef struct zeke_tree zeke_tree_t;
typedef struct zeke_tree_node {
  zeke_magic_t magic;
#define ZEKE_TREE_NODE_MAGIC 0x4bb823a7
  ZKTREE_ENTRY l;
  struct zeke_tree_data *priv;
} zeke_tree_node_t;

typedef int (*zeke_tree_complete_fn)(zeke_tree_t*,zeke_context_t*);
typedef int (*zeke_tree_timeout_fn)(zeke_tree_t*,apr_interval_time_t,zeke_context_t*);
typedef apr_uint32_t (*zeke_tree_filter_fn)(const zeke_tree_node_t *parent,
                                            const char **name,
                                            const char **absname,
                                            zeke_context_t*);


ZKTREE_HEAD_DECL(ring);


/* provide zeke_tree_pool_get() */
ZEKE_POOL_EXPORT_ACCESSOR(tree);
/* provide zeke_tree_context_get() */
ZEKE_EXPORT_ACCESSOR(tree,context);


/* prototypes */
ZEKE_EXPORT zk_status_t zeke_tree_create(zeke_tree_t**, const char *root,
                                         zeke_tree_complete_fn complete_cb,
                                         zeke_tree_timeout_fn timeout_cb,
                                         zeke_context_t *userdata,
                                         const zeke_connection_t*);
ZEKE_EXPORT zk_status_t zeke_tree_build_filter_set(zeke_tree_t*,
                                                   zeke_tree_filter_fn);
ZEKE_EXPORT int zeke_tree_check_consistency(zeke_tree_ring_t*);
ZEKE_EXPORT int zeke_tree_is_started(const zeke_tree_t*);
ZEKE_EXPORT int zeke_tree_is_complete(const zeke_tree_t*);
ZEKE_EXPORT int zeke_tree_is_error(const zeke_tree_t*, zk_status_t*);
ZEKE_EXPORT int zeke_tree_is_cancelled(const zeke_tree_t*);
ZEKE_EXPORT int zeke_tree_is_held(const zeke_tree_t*);
ZEKE_EXPORT int zeke_tree_hold_set(zeke_tree_t*, int on);
ZEKE_EXPORT zk_status_t zeke_tree_start(zeke_tree_t*);
ZEKE_EXPORT zk_status_t zeke_tree_status(const zeke_tree_t*);
ZEKE_EXPORT zk_status_t zeke_tree_destroy(zeke_tree_t*);
ZEKE_EXPORT zk_status_t zeke_tree_cancel(zeke_tree_t*);
ZEKE_EXPORT zk_status_t zeke_tree_request_time_get(apr_interval_time_t *reqtime,
                                                   const zeke_tree_t*);
ZEKE_EXPORT void zeke_tree_context_set(zeke_tree_t*, zeke_context_t*);
ZEKE_EXPORT zeke_context_t *zeke_tree_context(const zeke_tree_t*);

ZEKE_EXPORT apr_status_t zeke_tree_node_userdata_get(void **data, const char *key,
                                                     zeke_tree_node_t*);
ZEKE_EXPORT void zeke_tree_node_userdata_set(const void *data, const char *key,
                                             zeke_tree_node_t*);
ZEKE_EXPORT void zeke_tree_node_info_get(const zeke_tree_node_t*,
                                         const char **name,
                                         const char **absname,
                                         apr_interval_time_t *last);
ZEKE_EXPORT zk_status_t zeke_tree_head_get(const zeke_tree_t*,
                                           const char **root,
                                           const zeke_tree_head_t **head);
ZEKE_EXPORT int zeke_tree_node_count(const zeke_tree_t*);
ZEKE_EXPORT const zeke_tree_node_t *zeke_tree_node_parent_get(const zeke_tree_node_t*);
ZEKE_EXPORT const zeke_tree_head_t *zeke_tree_node_children_get(const zeke_tree_node_t*);

/* iteration */
ZEKE_EXPORT zeke_tree_iter_t *zeke_tree_iter_first(const zeke_tree_t*,
                                                   zeke_tree_iter_type_e type,
                                                   void *user_data,
                                                   apr_pool_t *pool);
ZEKE_EXPORT zeke_tree_node_t *zeke_tree_iter_this(zeke_tree_iter_t*, void **userdata);
ZEKE_EXPORT zeke_tree_iter_t *zeke_tree_iter_next(zeke_tree_iter_t*, void **userdata);
ZEKE_EXPORT void zeke_tree_iter_destroy(zeke_tree_iter_t*);

#ifdef __cplusplus
}
#endif

#endif /* _LIBZEKE_TREE */
