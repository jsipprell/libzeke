/* zeked/libzeke initialization */
#include "libzeke.h"
#include "libzeke_hooks.h"
#include "dispatch.h"

extern void zeke_global_hooks_init(void);

apr_file_t *zstdout = NULL;
apr_file_t *zstderr = NULL;
apr_file_t *zstdin = NULL;

static int initialized = 0;
static apr_pool_t *root_pool = NULL;

struct zeke_atexit {
  void (*func)(void*);
  void *arg;
};

ZEKE_API(void) zeke_terminate(void)
{
  if(--initialized >= 0)
    apr_terminate();
}

extern void zeke_indirect_debug(void);

static
apr_status_t finalize_root_pool(void *ignore)
{
  return APR_SUCCESS;
}

ZEKE_API(zk_status_t) zeke_init_cli(int *argc,
                          char const *const **argv,
                          char const *const **env)
{
  apr_status_t rc = APR_SUCCESS;

  assert(!initialized);
  if((rc = apr_app_initialize(argc,argv,env)) == APR_SUCCESS) {
    initialized++;
    atexit(zeke_terminate);
  }

  if(!root_pool && rc == APR_SUCCESS) {
    rc = apr_pool_create(&root_pool,NULL);
    apr_pool_tag(root_pool,"ZEKE ROOT POOL (cli)");
    assert(rc == APR_SUCCESS);
    zeke_init_indirects(root_pool);
    rc = apr_pool_create(&root_pool,root_pool);
    assert(rc == APR_SUCCESS);
    apr_pool_tag(root_pool,"ZEKE PSUEDO-ROOT POOL (cli)");
    zeke_pool_cleanup_register(root_pool,NULL,finalize_root_pool);
  }

  if(rc == APR_SUCCESS)
    rc = zeke_init(root_pool);

  return rc;
}

ZEKE_API(zk_status_t) zeke_init(apr_pool_t *pool)
{
  apr_status_t rc = APR_SUCCESS;

  if(!initialized) {
    if((rc = apr_initialize()) == APR_SUCCESS) {
      initialized++;
      atexit(zeke_terminate);
    }

    if(!root_pool && rc == APR_SUCCESS) {
      rc = apr_pool_create(&root_pool,pool);
      apr_pool_tag(root_pool,"ZEKE ROOT POOL");
      zeke_init_indirects(root_pool);
    }
    if(pool == NULL) {
      rc = apr_pool_create(&root_pool,root_pool);
      assert(rc == APR_SUCCESS);
      apr_pool_tag(root_pool,"ZEKE PSUEDO-ROOT POOL");
    }
  }

  if(!pool)
    pool = root_pool;
  
  if(rc == APR_SUCCESS && zstdout == NULL)
    rc = apr_file_open_stdout(&zstdout,pool);
  if(rc == APR_SUCCESS && zstderr == NULL)
    rc = apr_file_open_stderr(&zstderr,pool);
  if(rc == APR_SUCCESS && zstdin == NULL)
    rc = apr_file_open_stdin(&zstdin,pool);

  if(rc == APR_SUCCESS) {
    zeke_error_init(pool);
    zeke_dispatch_init(pool);
    zeke_global_hooks_init();
  }

  return rc;
}

ZEKE_API(apr_pool_t*) zeke_root_subpool_create(void)
{
  apr_pool_t *pool = NULL;
  apr_status_t rc = apr_pool_create(&pool,root_pool);

  apr_pool_tag(pool,"unlabeled root pool");
  if(rc != APR_SUCCESS)
    zeke_fatal("apr_pool_create",rc);
  return pool;
}

static apr_status_t zeke_atexit_call(void *data)
{
  assert(data != NULL);
  if(((struct zeke_atexit*)data)->func)
    ((struct zeke_atexit*)data)->func(((struct zeke_atexit*)data)->arg);
  return APR_SUCCESS;
}

ZEKE_API(void) zeke_atexit(void (*func)(void*), void *arg)
{
  struct zeke_atexit *info;
  static apr_pool_t *atexit_pool = NULL;

  if(atexit_pool == NULL)
    assert(apr_pool_create_unmanaged(&atexit_pool) == APR_SUCCESS);
  
  assert(atexit_pool != NULL);
  info = apr_palloc(atexit_pool,sizeof(*info));
  assert(info != NULL);
  info->func = func;
  info->arg = arg;
  apr_pool_cleanup_register(root_pool,info,zeke_atexit_call,apr_pool_cleanup_null);
}
