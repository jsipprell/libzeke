#include "internal.h"
#include <apr_thread_mutex.h>

#if APR_HAS_THREADS
static apr_pool_t *qsort_pool = NULL;
static apr_thread_mutex_t *qsort_mutex = NULL;
#endif

static void *qsort_context = NULL;
static int (*qsort_compar)(const void*, const void*, void*);

#if APR_HAS_THREADS
static apr_status_t cleanup_qsort_mutex(void *data)
{
  qsort_mutex = NULL;

  if(data != NULL && data == (void*)qsort_pool)
    qsort_pool = NULL;
  return APR_SUCCESS;
}
#endif /* APR_HAS_THREADS */

static int compar_redirect(const void *a1, const void *a2)
{
  return qsort_compar(a1,a2,qsort_context);
}

ZEKE_API(void) qsort_r(void *base, size_t nmemb, size_t size,
                       void *arg,
                       int (*compar)(void*, const void*, const void*)
{
  void *save_context;
  int (*save_compar)(void*, const void*, const void*);
#if APR_HAS_THREADS
  apr_pool_t *pool = NULL;
#endif
  static volatile int spinlock = 0;

#if APR_HAS_THREADS
  while(qsort_pool == NULL || qsort_mutex == NULL) {
    spinlock++;
    while(spinlock > 1) ;

    if(qsort_pool == NULL || qsort_mutex == NULL) {
      if (qsort_pool == NULL) {
        apr_status_t st = apr_pool_create(&pool,NULL);
        qsort_pool = pool;
        assert(st == APR_SUCCESS);
      }

      if (qsort_mutex == NULL) {
        apr_status_t st = apr_thread_mutex_create(&qsort_mutex,
                                                  APR_THREAD_MUTEX_UNNESTED,
                                                  qsort_pool);

        assert(st == APR_SUCCESS && qsort_mutex != NULL);
        apr_pool_cleanup_register(qsort_pool,pool,cleanup_qsort_mutex,
                                                  apr_pool_cleanup_null);
      }
    }
    spinlock--;
  }
  assert(apr_thread_mutex_lock(qsort_mutex) == APR_SUCCESS);
#else /* !APR_HAS_THREADS */
  spinlock++;
  while(spinlock > 1) ;
#endif
  save_context = qsort_context;
  save_compar = qsort_compar;
  qsort_context = arg;
  qsort_compar = compar;

  qsort(base,nmemb,size,compar_redirect);

  qsort_compar = save_compar;
  qsort_context = save_context;
#if APR_HAS_THREADS
  if (qsort_mutex) {
    assert(apr_thread_mutex_unlock(qsort_mutex) == APR_SUCCESS);
  }
#else /* !APR_HAS_THREADS */
  spinlock--;
#endif
}
