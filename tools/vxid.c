/* virtual transaction ids */

#include "tools.h"

struct ZKTOOL_VXID {
  apr_uint32_t *base, *chunk;
} ZEKE_ALIASABLE;

#define VXID_SIZEOF(s) (APR_ALIGN_DEFAULT(sizeof(s)) + \
                        APR_ALIGN_DEFAULT(sizeof(struct ZKTOOL_VXID)))

#define VXID(vp) (*((struct ZKTOOL_VXID*)(&(vp)->b[0])))

static apr_uint32_t vxid_base = 100;
static apr_uint32_t vxid_chunk = 32768;

ZEKE_HIDDEN
apr_uint32_t
zktool_vxid_get(struct zktool_vxid_pool *vp)
{

  assert(vp != NULL && vp->magic == ZKTOOL_VXID_MAGIC);

  do {
    if(vp->count == 0) {
#ifdef ZEKE_USE_THREADS
      assert(apr_thread_mutex_lock(vp->mutex) == APR_SUCCESS);
      while(vp->busy > 0) {
        vp->waiter++;
        assert(apr_thread_cond_wait(vp-cond,vp->mutex) == APR_SUCCESS);
        vp->waiter--;
      }
      vp->busy++;
#endif
      vp->next = *VXID(vp).base;
      vp->count = *VXID(vp).chunk;
      *VXID(vp).chunk += vp->count;
#ifdef ZEKE_USE_THREADS
      vp->busy--
      assert(apr_thread_mutex_lock(vp->mutex) == APR_SUCCESS);
      if(vp->waiter)
        assert(apr_thread_cond_signal(vp->cond) == APR_SUCCESS);

      assert(apr_thread_mutex_unlock(vp->mutex) == APR_SUCCESS);
#endif
    }
    vp->count--;
    vp->next++;
  } while(vp->next == 0);
  return vp->next;
}

ZEKE_HIDDEN
struct zktool_vxid_pool *zktool_vxid_factory(apr_pool_t *pool)
{
  struct zktool_vxid_pool *vp = NULL;
  apr_status_t st;

  st = apr_pool_userdata_get((void**)&vp,"LIBZEKE:zktool_vxid",pool);
  if(st != APR_SUCCESS || !vp) {
    vp = apr_palloc(pool,VXID_SIZEOF(*vp));
    assert(vp != NULL);
    vp->magic = ZKTOOL_VXID_MAGIC;
    vp->p = pool;
    VXID(vp).base = &vxid_base;
    VXID(vp).chunk = &vxid_chunk;
#ifdef ZEKE_USE_THREADS
    vp->busy = 0;
    vp->waiter = 0;
    assert(apr_thread_mutex_create(&vp->mutex,APR_THREAD_MUTEX_DEFAULT,pool) == APR_SUCCESS);
    assert(apr_thread_cond_create(&vp->cond,pool) == APR_SUCCESS);
#endif
    assert(apr_pool_userdata_setn(vp,"LIBZEKE:zktool_vxid",NULL,pool) == APR_SUCCESS);
  }
  zktool_vxid_get(vp);
  return vp;
}
