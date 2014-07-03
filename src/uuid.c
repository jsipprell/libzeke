#include "internal.h"
#include "connection.h"
#include "dispatch.h"

#include "libzeke_nodedir.h"
#include "libzeke_uuid.h"
#include "libzeke_indirect.h"

struct zeke_uuid_registry_t {
  apr_pool_t *pool;
  const zeke_connection_t *connection;
  zk_status_t status;
  zeke_nodedir_t *nodedir;
  zeke_context_t *user_context;
  apr_interval_time_t timeout;
  zeke_uuid_complete_fn completed;
  zeke_uuid_timeout_fn timedout;
  apr_uuid_t *uuid;
  apr_table_t *entries;
  apr_hash_t *hash;
  apr_time_t start_time;
  apr_time_t end_time;
  int flags;
  int finished;
  const char *name,*node,*root;
};

ZEKE_POOL_IMPLEMENT_ACCESSOR(uuid_registry)

#define ZEKE_UUID_REGISTRY_DEFAULT_ROOT "uuids"

#ifndef DEFAULT_UUID_ZOO_FLAGS
# ifndef DEFAULT_UUID_NON_EPHEMERAL
# define DEFAULT_UUID_ZOO_FLAGS ZOO_EPHEMERAL
# else
# define DEFAULT_UUID_ZOO_FLAGS 0
# endif /* DEFAULT_UUID_NON_EPHEMERAL */
#endif /* DEFAULT_UUID_ZOO_FLAGS */

#define UUID_ZOO_MASK(flags) ( (flags) & ~(ZOO_SEQUENCE) )

/* forward decls */
static int uuid_registry_complete(const zeke_nodedir_t*,apr_table_t*,
                                   zeke_context_t*);
static apr_status_t remove_uuid_registry_nodedir_cleanup(void*);
static const char *get_default_name(apr_pool_t*);
static zeke_cb_t uuid_registry_complete2(const zeke_callback_data_t*);

/* Various accessor api calls */
ZEKE_API(int) zeke_uuid_registry_is_complete(const zeke_uuid_registry_t *registry)
{
  return registry->finished > 0;
}

ZEKE_API(zk_status_t) zeke_uuid_registry_status(const zeke_uuid_registry_t *registry)
{
  return registry->status;
}

ZEKE_API(const apr_array_header_t*) zeke_uuid_registry_index_get(const zeke_uuid_registry_t *r,
                                                                 int sorted,
                                                                 apr_pool_t *pool)
{
  if(r->finished <= 0 || !r->entries)
    return NULL;

  return zeke_table_index(r->entries,sorted,(pool ? pool : r->pool));
}

#if 0
ZEKE_API(apr_table_t*) zeke_uuid_registry_table_get(zeke_uuid_registry_t *r,
                                                   apr_pool_t *pool)
{
  if(r->finished <= 0 || !r->entries)
    return NULL;

  if(pool != NULL)
    return apr_table_clone(pool,r->entries);
  return r->entries;
}
#endif

ZEKE_API(apr_interval_time_t) zeke_uuid_registry_request_time_get(const zeke_uuid_registry_t *r)
{
  if(!r->end_time)
    return apr_time_now() - r->start_time;
  return r->end_time - r->start_time;
}


ZEKE_API(const char*) zeke_uuid_registry_name_get(const zeke_uuid_registry_t *r, apr_pool_t *pool)
{
  if(pool != NULL)
    return apr_pstrdup(pool,r->name);
  return r->name;
}

ZEKE_API(zk_status_t) zeke_uuid_registry_uuid_get(const zeke_uuid_registry_t *r, apr_uuid_t *uuid)
{
  apr_uuid_t *h_uuid = NULL;

  if(r->finished <= 0)
    return APR_EINPROGRESS;

  assert(uuid != NULL);
  if(r->hash) {
    apr_uuid_t *h_uuid = apr_hash_get(r->hash,r->name,APR_HASH_KEY_STRING);
    ZEKE_ASSERTV(h_uuid != NULL,"no uuid found for '%s'",r->name);
  } else {
    const char *val = apr_table_get(r->entries,r->name);
    ZEKE_ASSERTV(val != NULL,"no uuid found for '%s'",r->name);
    return apr_uuid_parse(uuid,val);
  }
  
  memcpy(uuid,h_uuid,sizeof(apr_uuid_t));
  return ZEOK;
}

ZEKE_API(const char*) zeke_uuid_registry_uuid_format(const zeke_uuid_registry_t *r, apr_pool_t *pool)
{
  char uuid_buf[APR_UUID_FORMATTED_LENGTH+1];
  apr_uuid_t uuid;
  zk_status_t st = zeke_uuid_registry_uuid_get(r,&uuid);

  if(!ZEKE_STATUS_IS_OKAY(st)) {
    zeke_apr_error(apr_psprintf(pool ? pool : r->pool,"%s/%s", r->name, r->node),st);
    return NULL;
  }

  apr_uuid_format(uuid_buf,&uuid);
  return apr_pstrdup((pool ? pool : r->pool), uuid_buf);
}

ZEKE_API(zeke_context_t*) zeke_uuid_registry_context(const zeke_uuid_registry_t *registry)
{
  return registry->user_context;
}

ZEKE_API(void) zeke_uuid_registry_context_set(zeke_uuid_registry_t *registry,
                                              zeke_context_t *context)
{
  registry->user_context = context;
}

static int set_hash(void *hash, const char *key, const char *value) {
  apr_hash_t *h = (apr_hash_t*)hash;
  apr_uuid_t *uuid;
  apr_status_t st;

  uuid = apr_palloc(apr_hash_pool_get(h),sizeof(apr_uuid_t));
  assert(uuid != NULL);
  if((st = apr_uuid_parse(uuid,value)) != APR_SUCCESS) {
    zeke_apr_error(apr_psprintf(apr_hash_pool_get(h),
                   "apr_uuid_parse(\"%s\")",value),st);
    return 0;
  }
  apr_hash_set(h,key,APR_HASH_KEY_STRING,uuid);
  return 1;
}

ZEKE_API(apr_hash_t*) zeke_uuid_registry_hash_get(const zeke_uuid_registry_t *r,
                                                  apr_pool_t *pool)
{
  apr_hash_t *h = r->hash;

  if(pool != NULL || h == NULL) {
    h = apr_hash_make(pool ? pool : r->pool);
    apr_table_do(set_hash,h,r->entries,NULL);
    if(pool == r->pool)
      ((zeke_uuid_registry_t*)r)->hash = h;
  }

  return h;
}

static inline int uuid_zoo_get_flags(int flags) {
  int zoo_flags = DEFAULT_UUID_ZOO_FLAGS;

  if(flags & ZEKE_UUID_REGISTER_PERMANENT)
    zoo_flags &= ~ZOO_EPHEMERAL;

  return UUID_ZOO_MASK(zoo_flags);
}

static void registry_failure(zeke_uuid_registry_t *r,zk_status_t status)
{
  zeke_uuid_complete_fn completed = r->completed;

  r->finished = 1;
  r->status = status;
  r->completed = NULL;
  r->timedout = NULL;
  r->end_time = apr_time_now();
  if(completed)
    completed(r,r->user_context);
}
#define registry_completed(r) registry_failure((r),ZEOK)

static int uuid_registry_timeout(const zeke_nodedir_t *nodedir,
                                  apr_interval_time_t elapsed,
                                  zeke_context_t *ctx)
{
  zeke_uuid_registry_t *r = (zeke_uuid_registry_t*)ctx;
  zeke_uuid_timeout_fn timedout = r->timedout;

  r->finished = 1;
  r->end_time = apr_time_now();
  r->timedout = NULL;
  r->completed = NULL;
  if(timedout)
    timedout(r,elapsed,r->user_context);
  return 0;
}

static zeke_cb_t uuid_registry_retry(const zeke_callback_data_t *cbd)
{
  zk_status_t st = cbd->status;
  zeke_uuid_registry_t *r = (zeke_uuid_registry_t*)cbd->ctx;

  if(r->finished > 0)
    return ZEKE_CALLBACK_DESTROY;

  if(!ZEKE_STATUS_IS_OKAY(st)) {
    registry_failure(r,st);
    zeke_callback_status_set(cbd,ZEOK);
    return ZEKE_CALLBACK_DESTROY;
  }

  if(r->nodedir) {
    zeke_nodedir_t *nodedir = r->nodedir;
    remove_uuid_registry_nodedir_cleanup(r);
    zeke_nodedir_destroy(nodedir);
  }

  st = zeke_nodedir_create(&r->nodedir,r->root,r->timeout,
                           uuid_registry_complete,
                           (r->timedout ? uuid_registry_timeout : NULL),
                           r,cbd->connection);
  if(!ZEKE_STATUS_IS_OKAY(st)) {
    registry_failure(r,st);
    zeke_nodedir_destroy(r->nodedir);
    r->nodedir = NULL;
  } else
    apr_pool_cleanup_register(zeke_nodedir_pool_get(r->nodedir),&r->nodedir,
                              zk_indirect_wipe,apr_pool_cleanup_null);

  return ZEKE_CALLBACK_DESTROY;
}

ZEKE_PRIVATE(apr_table_t*) zeke_uuid_registry_table(const zeke_uuid_registry_t *r)
{
  return r->entries;
}

static zeke_cb_t uuid_registry_complete2(const zeke_callback_data_t *cbd)
{
  zeke_uuid_registry_t *r = (zeke_uuid_registry_t*)cbd->ctx;
  zk_status_t st = cbd->status;

  if(ZEKE_STATUS_IS_OKAY(st)) {
    if(r->node)
      apr_table_setn(r->entries,r->name,r->node);
    registry_completed(r);
  } else registry_failure(r,st);

  return ZEKE_CALLBACK_DESTROY;
}

static zeke_cb_t uuid_registry_complete_to_sync(const zeke_callback_data_t *cbd)
{
  zeke_uuid_registry_t *r = (zeke_uuid_registry_t*)cbd->ctx;
  zk_status_t st = cbd->status;

  if(ZEKE_STATUS_IS_OKAY(st)) {
    const char *path = zeke_nodepath_join(r->pool,r->root,r->name,NULL);

    st = zeke_async(NULL,r->connection,path,uuid_registry_complete2,r);
    if(ZEKE_STATUS_IS_OKAY(st))
      return ZEKE_CALLBACK_DESTROY;
  }

  registry_failure(r,st);
  return ZEKE_CALLBACK_DESTROY;
}

static int get_uuid(void *data, const char *key, const char *val)
{
  zeke_uuid_registry_t *r = (zeke_uuid_registry_t*)data;
  apr_uuid_t uuid;
  apr_status_t st;

  ZEKE_ASSERTV(strcmp(key,r->name) == 0,
        "node name mismatch for %s (!= %s)",key,r->name);
  if(r->node) {
    ZEKE_ASSERTV(strcasecmp(val,r->node) == 0,
          "node uuid mismatch for '%s' (%s != %s)",key,r->node,val);
  }

  if((st = apr_uuid_parse(&uuid,val)) != APR_SUCCESS) {
    zeke_apr_error(val,st);
    return 0;
  }

  if(!r->uuid) {
    r->uuid = apr_palloc(r->pool,sizeof(apr_uuid_t));
    assert(r->uuid != NULL);
  } else {
    ZEKE_ASSERTV(memcmp(r->uuid,&uuid,sizeof(apr_uuid_t)) == 0,
          "node uuid mismatch for '%s' (%s/%s)",key,r->node,val);
  }

  memcpy(r->uuid,&uuid,sizeof(apr_uuid_t));
  return 1;
}


static int uuid_registry_complete(const zeke_nodedir_t *nodedir,
                                   apr_table_t *table,
                                   zeke_context_t *ctx)
{
  zeke_uuid_registry_t *r = (zeke_uuid_registry_t*)ctx;
  zk_status_t st = zeke_nodedir_status(nodedir);
  apr_uuid_t *force_uuid = r->uuid;

  if(r->finished > 0)
    return 0;

  if(!ZEKE_STATUS_IS_OKAY(st)) {
    if(!r->finished && APR_TO_ZK_ERROR(st) == ZNONODE) {
      st = zeke_acreate(NULL,r->connection,r->root,NULL,-1,NULL,0,
                       uuid_registry_retry,r);
      if(ZEKE_STATUS_IS_OKAY(st))
        return 0;
    }
    registry_failure(r,st);
    return 0;
  }
  
  r->entries = apr_table_clone(r->pool,table);
  r->uuid = NULL;
  if(apr_table_get(r->entries,r->name) &&
     apr_table_do(get_uuid,r,r->entries,r->name,NULL)) {
    assert(r->uuid != NULL);
    if(force_uuid && (r->flags & ZEKE_UUID_REGISTER_ACCEPT) == 0
                  && memcmp(force_uuid,r->uuid,sizeof(apr_uuid_t)) != 0) {
      const char *path = zeke_nodepath_join(r->pool,r->root,r->name,NULL);
      r->uuid = force_uuid;

      if(r->node == NULL) {
        char uuid_buf[APR_UUID_FORMATTED_LENGTH+1];
        apr_uuid_format(uuid_buf,force_uuid);
        r->node = apr_pstrdup(r->pool,uuid_buf);
        assert(r->node != NULL);
      }
      
      st = zeke_aset(NULL,r->connection,path,r->node,strlen(r->node)+1,
                     -1,uuid_registry_complete_to_sync,r);
      if(!ZEKE_STATUS_IS_OKAY(st))
        registry_failure(r,st);
    } else
      registry_completed(r);
  } else {
    /* need to create our node as it does not yet exist */
    const char *path = zeke_nodepath_join(r->pool,r->root,r->name,NULL);

    if(force_uuid)
      r->uuid = force_uuid;

    if(r->node == NULL) {
      char uuid_buf[APR_UUID_FORMATTED_LENGTH+1];
      if(r->uuid == NULL) { 
        r->uuid = apr_palloc(r->pool,sizeof(apr_uuid_t));
        assert(r->uuid != NULL);
        apr_uuid_get(r->uuid);
      }
      apr_uuid_format(uuid_buf,r->uuid);
      r->node = apr_pstrdup(r->pool,uuid_buf);
      assert(r->node != NULL);
    }
    st = zeke_acreate(NULL,r->connection,path,r->node,strlen(r->node)+1,
                      ZEKE_DEFAULT_ACL,uuid_zoo_get_flags(r->flags),
                      uuid_registry_complete_to_sync,r);
    if(!ZEKE_STATUS_IS_OKAY(st))
      registry_failure(r,st);
  }

  return 0;
}

ZEKE_API(zk_status_t) zeke_uuid_registry_destroy(zeke_uuid_registry_t *registry)
{
  apr_pool_t *pool = registry->pool;

  if(pool != NULL) {
    zeke_nodedir_t *nodedir = registry->nodedir;
    apr_pool_cleanup_run(pool,registry,remove_uuid_registry_nodedir_cleanup);
    if(nodedir)
      zeke_nodedir_destroy(nodedir);
    registry->pool = NULL;
    apr_pool_destroy(pool);
  }
  return ZEOK;
}

static const char *get_default_name(apr_pool_t *pool)
{
  return zeke_get_fqdn(pool);
}

static apr_status_t remove_uuid_registry_nodedir_cleanup(void *data)
{
  zeke_uuid_registry_t *r = (zeke_uuid_registry_t*)data;

  if(r->nodedir && zeke_nodedir_pool_get(r->nodedir) != NULL)
    apr_pool_cleanup_kill(zeke_nodedir_pool_get(r->nodedir),
                          &r->nodedir,zk_indirect_wipe);
  
  r->nodedir = NULL;

  return APR_SUCCESS;
}

ZEKE_API(zk_status_t) zeke_uuid_registry_create(zeke_uuid_registry_t **registry,
                                                const char *name,
                                                zeke_uuid_complete_fn completed,
                                                zeke_context_t *context,
                                                const zeke_connection_t *conn)
{
  return zeke_uuid_registry_create_ex(registry,NULL,name,NULL,-1,
                                      completed,NULL,
                                      context,0,conn);
}

ZEKE_API(zk_status_t) zeke_uuid_registry_create_uuid(zeke_uuid_registry_t **registry,
                                                     const char *root,
                                                     const char *name,
                                                     apr_uuid_t *uuid,
                                                     zeke_uuid_complete_fn completed,
                                                     zeke_context_t *context,
                                                     const zeke_connection_t *conn)
{
  return zeke_uuid_registry_create_ex(registry,root,name,uuid,-1,
                                      completed,NULL,
                                      context,0,conn);
}

ZEKE_API(zk_status_t) zeke_uuid_registry_create_ex(zeke_uuid_registry_t **registry,
                                                  const char *root,
                                                  const char *name,
                                                  apr_uuid_t *uuid,
                                                  apr_interval_time_t timeout,
                                                  zeke_uuid_complete_fn completed,
                                                  zeke_uuid_timeout_fn timedout,
                                                  zeke_context_t *context,
                                                  int flags,
                                                  const zeke_connection_t *conn)
{
  zeke_uuid_registry_t *r;
  zk_status_t st = ZEOK;
  apr_pool_t *pool = NULL;

  assert(apr_pool_create(&pool,conn->pool) == APR_SUCCESS);
  assert(pool != NULL);
  assert(registry != NULL);
  *registry = r = apr_pcalloc(pool,sizeof(zeke_uuid_registry_t));
  assert(*registry != NULL);

  r->pool = pool;
  r->root = zeke_nodepath_join(pool,(root ? root : ZEKE_UUID_REGISTRY_DEFAULT_ROOT),NULL);
  if(name)
    r->name = apr_pstrdup(pool,name);
  else
    r->name = get_default_name(pool);

  if(uuid) {
    r->uuid = apr_palloc(pool,sizeof(apr_uuid_t));
    assert(r->uuid != NULL);
    memcpy(r->uuid,uuid,sizeof(apr_uuid_t));
  }
  if(timeout > APR_TIME_C(0))
    r->timeout = timeout;
  else
    r->timeout = -1;
  r->connection = conn;
  r->completed = completed;
  r->timedout = timedout;
  r->user_context = context;
  r->flags = flags;
  r->start_time = apr_time_now();

  st = zeke_nodedir_create(&r->nodedir,r->root,r->timeout,
                           uuid_registry_complete,
                           (r->timedout ? uuid_registry_timeout : NULL),
                           r,conn);
  apr_pool_cleanup_register(zeke_nodedir_pool_get(r->nodedir),&r->nodedir,
                            zk_indirect_wipe,apr_pool_cleanup_null);
  apr_pool_cleanup_register(pool,r,remove_uuid_registry_nodedir_cleanup,
                            apr_pool_cleanup_null);

  return st;
}
