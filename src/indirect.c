#include "internal.h"
#include "libzeke_indirect.h"

#ifdef ZEKE_SINGLE_THREADED
# ifdef ZEKE_USE_THREADS
# undef ZEKE_USE_THREADS
# endif
#elif defined(APR_HAS_THREADS)
# ifndef ZEKE_USE_THREADS
# define ZEKE_USE_THREADS
# endif
#endif

#if !defined(AGGRESSIVELY_COLLECT_INDIRECTS) && defined(DEBUGGING)
#define AGGRESSIVELY_COLLECT_INDIRECTS
#endif

#ifndef MIN_RELEASES_BEFORE_POOL_CLEAR
#define MIN_RELEASES_BEFORE_POOL_CLEAR 10
#endif

#ifndef RECYCLE_INDIRECTS_LOW_WATER_MARK
#define RECYCLE_INDIRECTS_LOW_WATER_MARK MIN_RELEASES_BEFORE_POOL_CLEAR + \
                                         (MIN_RELEASES_BEFORE_POOL_CLEAR/2)
#endif

struct zeke_indirect_t {
  zeke_context_t *ctx;
  apr_pool_t *binding;
#ifdef ZEKE_USE_THREADS
  apr_thread_mutex_t *mutex;
#else
  int reuse;
#endif
  zeke_magic_t magic;
#define ZEKE_INDIRECT_MAGIC 0x3bbcc731
#define ZEKE_DEFUNCT_INDIRECT_MAGIC 0x998faa16
#ifdef DEBUGGING
  char tag[65];
#endif
};
static apr_status_t unbind_indirect(void*);

static apr_pool_t *indirect_pool = NULL;
static apr_array_header_t *indirects = NULL;
static apr_size_t nreleased = 0;
static int finalize = 0;
static apr_size_t compact_ceiling = RECYCLE_INDIRECTS_LOW_WATER_MARK;
#ifdef ZEKE_USE_THREADS
static apr_pool_t *perm_mutex_pool = NULL;
static apr_thread_mutex_t *global_indirect_mutex = NULL;
#endif

static void collect_indirects(int force);
void zeke_indirect_debug(void);

static apr_status_t finalize_indirect_pool(void *ign)
{
  finalize++;
#ifdef DEBUGGING
  puts("FINALIZED INDIRECT POOL");
  zeke_indirect_debug();
#endif
  if(indirect_pool) {
    collect_indirects(1);
  }
  indirect_pool = NULL;
  indirects = NULL;
#ifdef DEBUGGING
  puts("FINALIZATION COMPLETE");
#endif
  return APR_SUCCESS;
}

#ifdef ZEKE_USE_THREADS
static apr_status_t release_global_indirect_mutex(void *vmp)
{
  apr_thread_mutex_t **mp = (apr_thread_mutex_t**)vmp;

  assert(finalize > 0);
  if(mp) {
    assert(apr_thread_mutex_unlock(*mp) == APR_SUCCESS);
    *mp = NULL;
  }
  perm_mutex_pool = NULL;
  return APR_SUCCESS;
}
#endif

static void init_indirects(apr_pool_t *rp)
{
  if(indirect_pool == NULL) {
    assert(rp != NULL);
    /* indirect_pool = zeke_root_subpool_create(); */
    apr_pool_create(&indirect_pool,rp);
    assert(indirect_pool != NULL);
    apr_pool_tag(indirect_pool,"GLOBAL Indirect Unmanaged Pool");
    apr_pool_cleanup_register(indirect_pool,rp,finalize_indirect_pool,apr_pool_cleanup_null);
  }

  nreleased = 0;
  compact_ceiling = RECYCLE_INDIRECTS_LOW_WATER_MARK;

  indirects = apr_array_make(indirect_pool,
                             RECYCLE_INDIRECTS_LOW_WATER_MARK,
                             sizeof(zeke_indirect_t));
  assert(indirects != NULL);
#ifdef ZEKE_USE_THREADS
  if(perm_mutex_pool == NULL) {
    apr_pool_create_unmanaged(&perm_mutex_pool);
    assert(perm_mutex_pool != NULL);
    apr_pool_tag(perm_mutex_pool,"really really really permanent mutex pool");
  }
  if(global_indirect_mutex == NULL) {
    assert(apr_thread_mutex_create(&global_indirect_mutex,APR_THREAD_MUTEX_UNNESTED,
           perm_mutex_pool) == APR_SUCCESS);
    apr_pool_pre_cleanup_register(perm_mutex_pool,&global_indirect_mutex,
                                  release_global_indirect_mutex);
  }
#endif
}

ZEKE_PRIVATE(void) zeke_init_indirects(apr_pool_t *root_pool)
{
  init_indirects(root_pool);
}

void zeke_indirect_debug(void)
{
  int i;

#ifdef ZEKE_USE_THREADS
  if(perm_mutex_pool)
    printf("IND: have global mutex pool\n");
#endif
  if(indirect_pool) {
    char prefix[128] = "";

    printf("IND: %u released/recycled indirects, compaction/recycle ceiling of %u\n",
           (unsigned)nreleased,(unsigned)compact_ceiling);
    if(indirects) {
      printf("IND: have %d total redirects\n",indirects->nelts);

      for(i = 0; i < indirects->nelts; i++) {
        zeke_indirect_t *ind = ((zeke_indirect_t*)indirects->elts)+i;

        if(ind->mutex) {
          snprintf(prefix,sizeof(prefix)-1,"IND: (magic:%llx)",
                   (unsigned long long)ind->magic);
          printf("%s #%d has a mutex\n",prefix,i);
          if(ind->ctx)
            printf("---> #%d has a context\n",i);
          if(ind->binding) {
#ifdef DEBUGGING
            if(ind->tag[0])
              printf("---> #%d has a binding (%s)\n",i,ind->tag);
            else
#endif
              printf("---> #%d has a binding\n",i);
          }
        }
      }
    }
  }
}

ZEKE_API(void) zeke_indirect_debug_banner(const char *msg)
{
  if(msg)
    printf("===========================================================\n%s\n"
           "===========================================================\n",
           msg);
  else
    printf("===========================================================\n");

  zeke_indirect_debug();
  printf("===========================================================\n");
}

  /* note: always call with global mutex locked */
static zeke_indirect_t *reuse_indirect(void)
{
  int i;
  zeke_indirect_t *ind = NULL;

  if(!indirects || !nreleased)
    return ind;
  ind = (zeke_indirect_t*)indirects->elts;

  for(i = 0; i < indirects->nelts; i++, ind++)
    if(ind->magic == ZEKE_DEFUNCT_INDIRECT_MAGIC) {
#ifdef ZEKE_USE_THREADS
      if(!ind->mutex)
#else
      if(ind->reuse)
#endif
      {
        assert(ind->ctx == NULL);
        assert(ind->binding == NULL);
#ifndef ZEKE_USE_THREADS
        ind->reuse = 0;
#endif
        ind->magic = ZEKE_INDIRECT_MAGIC;
        assert(nreleased > 0);
        nreleased--;
        return ind;
      }
    }
  return NULL;
}

static void compact_indirects(void)
{
  int i;
  apr_size_t nrel = 0,nvalid = 0;
  zeke_indirect_t *ind;
  apr_status_t st = APR_SUCCESS;

  if(!indirects)
    return;

#ifdef ZEKE_USE_THREADS
  st = apr_thread_mutex_lock(global_indirect_mutex);
  if(st != APR_SUCCESS)
    zeke_fatal("apr_thread_mutex_lock(compact_indirects)",st);
#endif

  ind = (zeke_indirect_t*)indirects->elts;

  for(i = 0; i < indirects->nelts; i++, ind++) {
    if(ind->ctx == NULL && ind->binding == NULL)
      nrel++;
    if(ind->magic == ZEKE_DEFUNCT_INDIRECT_MAGIC) {
      assert(ind->ctx == NULL);
      assert(ind->binding == NULL);
#ifdef ZEKE_USE_THREADS
      if(ind->mutex) {
        apr_thread_mutex_t *mtx = ind->mutex;
        assert(apr_thread_mutex_lock(mtx) == APR_SUCCESS);
        ind->mutex = NULL;
        assert(apr_thread_mutex_unlock(mtx) == APR_SUCCESS);
        apr_thread_mutex_destroy(mtx);
      }
#else
      ind->reuse++;
#endif
    } else if(ind->mutex && (ind->ctx || ind->binding))
      nvalid++;
  }

  nreleased = nrel;
  while(nvalid > (compact_ceiling/2))
    compact_ceiling += (MIN_RELEASES_BEFORE_POOL_CLEAR/2)-1;

  if(compact_ceiling < indirects->nelts)
    compact_ceiling = indirects->nelts;
  if(compact_ceiling < RECYCLE_INDIRECTS_LOW_WATER_MARK)
    compact_ceiling = RECYCLE_INDIRECTS_LOW_WATER_MARK;

#ifdef ZEKE_USE_THREADS
  st = apr_thread_mutex_unlock(global_indirect_mutex);
  if(st != APR_SUCCESS)
    zeke_fatal("apr_thread_mutex_unlock(compact_indirects)",st);
#endif
}

static void collect_indirects(int force)
{
  apr_size_t nrel = nreleased;

#ifdef ZEKE_USE_THREADS
  apr_status_t st = apr_thread_mutex_lock(global_indirect_mutex);
  if(st != APR_SUCCESS)
    zeke_fatal("apr_thread_mutex_lock(collect_indirects)",st);
#endif

  if(force)
    nrel = indirects->nelts;

  if(nrel == indirects->nelts && (nrel >= MIN_RELEASES_BEFORE_POOL_CLEAR
                               || force)) {
    apr_size_t i;
    zeke_indirect_t *ind = (zeke_indirect_t*)indirects->elts;
    for(i = 0; i < indirects->nelts; i++, ind++) {
      assert(ind->magic == ZEKE_INDIRECT_MAGIC || ind->magic == ZEKE_DEFUNCT_INDIRECT_MAGIC);
#ifdef ZEKE_USE_THREADS
      if(ind->mutex) {
#endif
      if(ind->ctx == NULL && (force || ind->binding == NULL)) {
#ifdef ZEKE_USE_THREADS
        apr_thread_mutex_t *mutex = NULL;
        assert(apr_thread_mutex_lock(ind->mutex) == APR_SUCCESS);
        if(ind->ctx == NULL && (force || ind->binding == NULL)) {
          mutex = ind->mutex;
          ind->mutex = NULL;
#endif
          if(ind->binding != NULL)
            apr_pool_cleanup_kill(ind->binding,ind,unbind_indirect);
          ind->binding = NULL;
          ind->ctx = NULL;
#ifdef ZEKE_USE_THREADS
        }
        if(mutex) {
          assert(apr_thread_mutex_unlock(mutex) == APR_SUCCESS);
          assert(apr_thread_mutex_destroy(mutex) == APR_SUCCESS);
        }
#endif
      }
#ifdef ZEKE_USE_THREADS
      } else if(!ind->ctx && !ind->binding && ind->mutex) {
        apr_thread_mutex_t *mutex = ind->mutex;
        assert(apr_thread_mutex_lock(ind->mutex) == APR_SUCCESS);
        ind->mutex = NULL;
        assert(apr_thread_mutex_unlock(mutex) == APR_SUCCESS);
        assert(apr_thread_mutex_destroy(mutex) == APR_SUCCESS);
      }
#endif

      if(st != APR_SUCCESS)
        goto collect_indirects_exit;
    }

    if(!finalize) {
      apr_pool_clear(indirect_pool);
#ifdef ZEKE_USE_THREADS
      apr_pool_clear(perm_mutex_pool);
      assert(global_indirect_mutex == NULL);
#endif
      init_indirects(NULL);
    }
    return;
  }

collect_indirects_exit:
#ifdef ZEKE_USE_THREADS
  assert(apr_thread_mutex_unlock(global_indirect_mutex) == APR_SUCCESS);
#endif
  if(st != APR_SUCCESS)
    zeke_fatal("collect_indirects",st);
}

static apr_status_t unbind_indirect(void *data)
{
  zeke_indirect_t *i = data;
  int perform_gc = 0;

  assert(i->magic == ZEKE_INDIRECT_MAGIC || i->magic == ZEKE_DEFUNCT_INDIRECT_MAGIC);
#ifdef ZEKE_USE_THREADS
  apr_status_t st = apr_thread_mutex_lock(i->mutex);
  /* guard against race from collect_indirects */
  if(!i->mutex)
    return APR_SUCCESS;
  else
    assert(st == APR_SUCCESS);
#endif
  if(i->binding != NULL) {
    i->binding = NULL;
    if(i->ctx == NULL) {
      nreleased++;
      i->magic = ZEKE_DEFUNCT_INDIRECT_MAGIC;
      if(nreleased >= MIN_RELEASES_BEFORE_POOL_CLEAR && nreleased == indirects->nelts)
        perform_gc++;
    }
  }
#ifdef ZEKE_USE_THREADS
  assert(apr_thread_mutex_unlock(i->mutex) == APR_SUCCESS);
#endif
  if(perform_gc)
    collect_indirects(0);
  else if(indirects->nelts >= compact_ceiling)
    compact_indirects();
  return APR_SUCCESS;
}


ZEKE_API(zeke_context_t*) zeke_indirect_peek(const zeke_indirect_t *i)
{
  zeke_context_t *ctx;

  assert(i->magic == ZEKE_INDIRECT_MAGIC);
#ifdef ZEKE_USE_THREADS
  assert(i->mutex != NULL);
  assert(apr_thread_mutex_lock(i->mutex) == APR_SUCCESS);
#endif

  if(i->ctx == NULL || i->binding == NULL)
    ctx = NULL;
  else
    ctx = i->ctx;

#ifdef ZEKE_USE_THREADS
  assert(apr_thread_mutex_unlock(i->mutex) == APR_SUCCESS);
#endif
  return ctx;
}

ZEKE_API(zeke_context_t*) zeke_indirect_consume(zeke_indirect_t *i)
{
  zeke_context_t *ctx;
  int perform_gc = 0;

  assert(i->magic == ZEKE_INDIRECT_MAGIC || i->magic == ZEKE_DEFUNCT_INDIRECT_MAGIC);
  if(i->magic == ZEKE_DEFUNCT_INDIRECT_MAGIC) {
    i->ctx = NULL;
    i->binding = NULL;
    return NULL;
  }

#ifdef ZEKE_USE_THREADS
  assert(i->mutex != NULL);
  assert(apr_thread_mutex_lock(i->mutex) == APR_SUCCESS);
#endif

  ctx = i->ctx;
  if(ctx != NULL) {
    i->ctx = NULL;
    if(i->binding == NULL) {
      ctx = NULL;  /* it's no longer valid, bound pool is gone */
      i->magic = ZEKE_DEFUNCT_INDIRECT_MAGIC;
      nreleased++;
      perform_gc++;
    }
#ifdef AGGRESSIVELY_COLLECT_INDIRECTS
    else {
      apr_pool_cleanup_kill(i->binding,i,unbind_indirect);
      i->magic = ZEKE_DEFUNCT_INDIRECT_MAGIC;
      i->binding = NULL;
      nreleased++;
      perform_gc++;
    }
#endif
  }
#ifdef ZEKE_USE_THREADS
  assert(apr_thread_mutex_unlock(i->mutex) == APR_SUCCESS);
#endif
  if(perform_gc && (nreleased < MIN_RELEASES_BEFORE_POOL_CLEAR ||
                    nreleased != indirects->nelts))
    perform_gc = 0;

  if(perform_gc)
    collect_indirects(0);
  else if(indirects->nelts >= compact_ceiling)
    compact_indirects();
  return ctx;
}

ZEKE_API(zeke_indirect_t*) zeke_indirect_find(zeke_context_t *ctx,
                                              apr_pool_t *binding)
{
  apr_status_t st = APR_SUCCESS;
  zeke_indirect_t *i, *ind = NULL;
  apr_size_t n;

#ifdef ZEKE_USE_THREADS
  if(global_indirect_mutex == NULL)
    init_indirects(NULL);

  st = apr_thread_mutex_lock(global_indirect_mutex);
  if(st != APR_SUCCESS)
    zeke_fatal("apr_thread_mutex_lock",st);
#else
  if(indirect_pool == NULL)
    init_indirects(NULL);
#endif
  for(n = 0, i = (zeke_indirect_t*)indirects->elts; n < indirects->nelts; n++, i++) {
    if(i->binding == binding && i->ctx == ctx) {
      ind = i;
      break;
    }
  }

#ifdef ZEKE_USE_THREADS
  assert(apr_thread_mutex_unlock(global_indirect_mutex) == APR_SUCCESS);
#endif
  return ind;
}

ZEKE_API(zeke_context_t*) zeke_indirect_release(zeke_context_t *ctx,
                                                apr_pool_t *binding)
{
  zeke_indirect_t *i = zeke_indirect_find(ctx,binding);
  return (i ? zeke_indirect_consume(i) : NULL);
}

ZEKE_API(zeke_indirect_t*) zeke_indirect_make(zeke_context_t *ctx,
                                              apr_pool_t *binding)
{
  apr_status_t st = APR_SUCCESS;
  ZEKE_MAY_ALIAS(zeke_indirect_t) *i = NULL;

  assert(binding != NULL);
  assert(ctx != NULL);

#ifdef ZEKE_USE_THREADS
  if(global_indirect_mutex == NULL)
    init_indirects(NULL);

  st = apr_thread_mutex_lock(global_indirect_mutex);
  if(st != APR_SUCCESS)
    zeke_fatal("apr_thread_mutex_lock",st);
#else
  if(indirect_pool == NULL)
    init_indirects(NULL);
#endif
  if(nreleased >= RECYCLE_INDIRECTS_LOW_WATER_MARK)
    i = reuse_indirect();
  if(!i) {
    i = (zeke_indirect_t*)&APR_ARRAY_PUSH(indirects,zeke_indirect_t);
    i->magic = ZEKE_INDIRECT_MAGIC;
  }
  i->ctx = ctx;
  i->binding = binding;
#ifdef DEBUGGING
  do {
    const char *t = zeke_pool_tag_get(binding);
    if(t)
      apr_cpystrn(i->tag,t,sizeof(i->tag));
    else
      i->tag[0] = '\0';
  } while(0);
#endif /* DEBUGGING */
#ifdef ZEKE_USE_THREADS
  st = apr_thread_mutex_create(&i->mutex,APR_THREAD_MUTEX_UNNESTED,perm_mutex_pool);
  if(st != APR_SUCCESS)
    goto zeke_indirect_make_exit;
#else
  i->reuse = 0;
#endif
  apr_pool_cleanup_register(binding,i,unbind_indirect,apr_pool_cleanup_null);

#ifdef ZEKE_USE_THREADS
zeke_indirect_make_exit:
  assert(apr_thread_mutex_unlock(global_indirect_mutex) == APR_SUCCESS);
#endif
  if(st != APR_SUCCESS)
    zeke_fatal("zeke_indirect_make",st);

  return i;
}

ZEKE_API(apr_status_t) zeke_indirect_deref_wipe(void *data)
{
  zeke_indirect_t *i = (zeke_indirect_t*)data;
  void **indp = NULL;

  if(i) {
    assert(i->magic == ZEKE_INDIRECT_MAGIC || i->magic == ZEKE_DEFUNCT_INDIRECT_MAGIC);
    indp = (void**)zeke_indirect_consume(i);
  }

  if(indp)
    *indp = NULL;
  return APR_SUCCESS;
}
