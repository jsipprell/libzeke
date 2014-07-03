/* shared memory logging for libzeke */
#include "internal.h"

#include "libzeke_log.h"

#if 0
#if defined(HAVE_FOPENCOOKIE) && defined(HAVE_LIBIO_H)
#include <stdio.h>
#include <libio.h>
#endif
#endif

#include <apr_shm.h>
#ifdef ZEKE_USE_THREADS
#include <apr_thread_rwlock.h>
#endif

#ifndef DEFAULT_BUFLEN
#define DEFAULT_BUFLEN 4096
#endif

#ifndef ZEKE_LOG_SIZE
#define ZEKE_LOG_SIZE 200
#endif

#ifndef ZEKE_LOGDIR
#define ZEKE_LOGDIR "/var/log"
#endif

#ifndef ZEKE_SHMLOG_SUFFIX
#define ZEKE_SHMLOG_SUFFIX "-log.shm"
#endif

#ifndef ZEKE_SHMLOCK_SUFFIX
#define ZEKE_SHMLOCK_SUFFIX "-log.lock"
#endif

#ifdef HAVE_FUNOPEN
typedef int zk_size_t;
typedef int zk_ssize_t;
#elif defined(HAVE_FOPENCOOKIE)
typedef size_t zk_size_t;
typedef ssize_t zk_ssize_t;
#endif


struct zeke_shmlog_header {
  apr_uint32_t magic;
#define ZEKE_LOG_HEADER_MAGIC 0x9834babb
  char lock[32];
  apr_uint32_t nelts;
  apr_int32_t first,next;
  apr_uint16_t read_locks,write_locks;
};

struct zeke_shmlock {
  apr_uint32_t magic;
#define ZEKE_SHMLOCK_MAGIC 0x64ba3c12
  apr_pool_t *p;
#ifdef ZEKE_USE_THREADS
  apr_thread_rwlock_t *tlock;
#endif
  apr_file_t *f;
  int r_type,w_type;
  apr_byte_t rlocked,wlocked;
};

struct zeke_shmlog {
  apr_uint32_t magic;
#define ZEKE_SHMLOG_MAGIC 0xbbab4398
  apr_uint64_t index;
  char ref[DEFAULT_BUFLEN];
};

typedef struct zeke_shmlog_stream {
  apr_uint32_t magic;
#define ZEKE_SHMLOG_STREAM_MAGIC 0x45be0cff
  struct zeke_shmlog_header *hdr;
  char rbuf[ZEKE_LOG_ENTRY_SIZE];
  char wbuf[DEFAULT_BUFLEN];
  char *rcp,*wcp;
  apr_int32_t idx;
} zslogf_t;

#define SHMLOG_ASSERT(h) assert((h) != NULL && (h)->magic == ZEKE_LOG_HEADER_MAGIC)
#define SHMLOG_REF_ASSERT(r) assert((r) != NULL && (r)->magic == ZEKE_SHMLOG_MAGIC)
#define SHMLOG_STREAM_ASSERT(f) assert((f) != NULL && (f)->magic == ZEKE_SHMLOG_STREAM_MAGIC)
#define SHMLOCK_ASSERT(l) assert((l) != NULL && (l)->magic == ZEKE_SHMLOCK_MAGIC)

static char *shmlog_filename = NULL;
static char *shmlog_lockname = NULL;
static apr_shm_t *shmlog = NULL;
static apr_pool_t *shmpool = NULL;
static const char *shmlog_ident = "libzeke";
static const char *shmlog_base = NULL;

static struct {
  int shmlog_ref;
  int shmlog;
} detach = { 1, 1 };

static inline zeke_log_entry_t *shmlog_idx(struct zeke_shmlog_header *hdr,
                                           apr_uint32_t i)
{
  zeke_log_entry_t *ents = (zeke_log_entry_t*)((char*)hdr + APR_ALIGN_DEFAULT(sizeof(*hdr)));

  return ents+i;
}

static apr_status_t shmref_cleanup(void *s)
{
  apr_shm_t **shm = (apr_shm_t**)s;
  if(shm && *shm && detach.shmlog_ref)
    return apr_shm_detach(*shm);
  return APR_SUCCESS;
}

static const char *get_shmlog(apr_pool_t *p, unsigned increment)
{
  apr_status_t st;
  static apr_shm_t *shm = NULL;
  struct zeke_shmlog *logref;
  char fname[DEFAULT_BUFLEN];

  if(!shm) {
    if(!shmpool) {
      assert(apr_pool_create(&shmpool,p) == APR_SUCCESS);
    }
    if(!shmlog_base)
      shmlog_base = shmlog_ident;
    if(!shmlog_filename) {
      char *cp = apr_cpystrn(fname,shmlog_base,sizeof(fname));
      if(cp - fname < sizeof(fname)-1)
        apr_cpystrn(cp,ZEKE_SHMLOG_SUFFIX,sizeof(fname) - ((cp - fname)+1));

      assert(apr_filepath_merge(&shmlog_filename,ZEKE_LOGDIR,
                                fname,
                                APR_FILEPATH_TRUENAME|APR_FILEPATH_NATIVE|
                                APR_FILEPATH_NOTRELATIVE,shmpool) == APR_SUCCESS);
    }
    if(!shmlog_lockname) {
      char *cp = apr_cpystrn(fname,shmlog_base,sizeof(fname));
      if(cp - fname < sizeof(fname)-1)
        apr_cpystrn(cp,ZEKE_SHMLOCK_SUFFIX,sizeof(fname) - ((cp - fname)+1));

      assert(apr_filepath_merge(&shmlog_lockname,ZEKE_LOGDIR,
                                fname,
                                APR_FILEPATH_TRUENAME|APR_FILEPATH_NATIVE|
                                APR_FILEPATH_NOTRELATIVE,shmpool) == APR_SUCCESS);
    }
    st = apr_shm_attach(&shm,shmlog_filename,shmpool);
    if(st != APR_SUCCESS) {
      int i;
      /* Fun fun fun, on linux at least merely attaching can create the file */
      if(st == APR_ENOENT)
        apr_shm_remove(shmlog_filename,shmpool);
      for(i = 0; st != APR_SUCCESS && i < 2; i++) {
        st = apr_shm_create(&shm,sizeof(*logref),shmlog_filename,shmpool);
        if(st != APR_SUCCESS) {
          apr_sleep(APR_TIME_C(100) * APR_TIME_C(1000));
          apr_shm_remove(shmlog_filename,shmpool);
        }
      }
      if(st != APR_SUCCESS) {
#ifdef DEBUGGING
        zeke_apr_eprintf(st,"shm ref error: %s",shmlog_filename);
        abort();
#else
        zeke_fatal(shmlog_filename,st);
#endif
      }
      detach.shmlog_ref = 0;
      logref = apr_shm_baseaddr_get(shm);
      assert(logref != NULL);
      logref->magic = ZEKE_SHMLOG_MAGIC;
      logref->index = (apr_uint64_t)increment;
      increment = 0;
      apr_snprintf(logref->ref,sizeof(logref->ref),"%s.%016" APR_UINT64_T_HEX_FMT,
                                                                shmlog_filename,
                                                                logref->index);
    } else
      logref = apr_shm_baseaddr_get(shm);
    zeke_pool_cleanup_register(shmpool,&shm,shmref_cleanup);
  } else
    logref = apr_shm_baseaddr_get(shm);

  SHMLOG_REF_ASSERT(logref);
  if(increment) {
    logref->index += (apr_uint64_t)increment;
    apr_snprintf(logref->ref,sizeof(logref->ref),"%s.%016" APR_UINT64_T_HEX_FMT,
                                                           shmlog_filename,
                                                           logref->index);
  }
  return logref->ref;
}

static apr_status_t zeke_shmlog_cleanup(void *hp)
{
  struct zeke_shmlog_header *hdr = (struct zeke_shmlog_header*)hp;

  if(hdr && hdr->magic == ZEKE_SHMLOG_MAGIC) {
    if(hdr->write_locks || hdr->read_locks)
      zeke_shmlog_unlock(hdr);
  }
  shmlog_filename = NULL;
  shmlog_lockname = NULL;
  shmpool = NULL;
  if(shmlog) {
    if(detach.shmlog)
      apr_shm_detach(shmlog);
    shmlog = NULL;
  }
  return APR_SUCCESS;
}

static apr_status_t shmlock_cleanup(void *lp)
{
  zeke_shmlock_t *l = (zeke_shmlock_t*)lp;

  if(l->magic == 1) return APR_SUCCESS;

  SHMLOCK_ASSERT(l);
#ifdef ZEKE_USE_THREADS
  if((l->rlocked || l->wlocked) && l->tlock)
    apr_thread_rwlock_unlock(l->tlock);
  if(l->tlock)
    apr_thread_rwlock_destroy(l->tlock);
  l->tlock = NULL;
#endif

  if(l->f) {
    if(l->wlocked || l->rlocked)
      apr_file_unlock(l->f);
    apr_file_close(l->f);
    l->f = NULL;
  }
  l->magic = 1;
  return APR_SUCCESS;
}

ZEKE_API(apr_status_t) zeke_shmlock_create(zeke_shmlock_t **lockp,
                                           const char *fname,
                                           apr_lockmech_e mech,
                                           apr_pool_t *p)
{
  apr_status_t st = APR_SUCCESS;
  zeke_shmlock_t *lock;

  if(!p || !lockp)
    return APR_EINVAL;

  lock = apr_palloc(p,sizeof(*lock));
  if(!lock)
    return APR_ENOMEM;

  lock->magic = ZEKE_SHMLOCK_MAGIC;
  lock->p = p;
  lock->f = NULL;
  lock->rlocked = 0;
  lock->wlocked = 0;
#ifdef ZEKE_USE_THREADS
  lock->tlock = NULL;

  st = apr_thread_rwlock_create(&lock->tlock,p);
#endif
  if(st != APR_SUCCESS)
    goto zeke_shmlock_create_exit;
  st = apr_file_open(&lock->f,fname,APR_READ|APR_WRITE|APR_CREATE|
                                   APR_SHARELOCK,APR_OS_DEFAULT,p);
  if(st == APR_SUCCESS)
    apr_file_inherit_unset(lock->f);

  if(st == APR_EACCES) {
    st = apr_file_open(&lock->f,fname,APR_READ,APR_OS_DEFAULT,p);
    lock->r_type = 0;
    lock->w_type = 0;
  } else {
    lock->r_type = APR_FLOCK_SHARED;
    lock->w_type = APR_FLOCK_EXCLUSIVE;
  }

zeke_shmlock_create_exit:
  if(st != APR_SUCCESS) {
#ifdef ZEKE_USE_THREADS
    if(lock->tlock)
      apr_thread_rwlock_destroy(lock->tlock);
#endif
    if(lock->f)
      apr_file_close(lock->f);
  } else {
    *lockp = lock;
    apr_pool_cleanup_register(p,lock,shmlock_cleanup,shmlock_cleanup);
  }
  return st;
}

ZEKE_API(apr_status_t) zeke_shmlock_rdlock(zeke_shmlock_t *l, int block)
{
  apr_status_t st = APR_SUCCESS;
  SHMLOCK_ASSERT(l);

#ifdef ZEKE_USE_THREADS
  st = apr_thread_rwlock_rdlock(l->tlock);
  if(st != APR_SUCCESS)
    return st;
#endif

  if(!block)
    block = APR_FLOCK_NONBLOCK;
  else
    block = 0;
  st = apr_file_lock(l->f, l->r_type|block);
#ifdef ZEKE_USE_THREADS
  if(st != APR_SUCCESS)
    apr_thread_rwlock_unlock(l->tlock);
#endif

  if(st == APR_SUCCESS)
    l->rlocked++;
  return st;
}

ZEKE_API(apr_status_t) zeke_shmlock_wrlock(zeke_shmlock_t *l, int block)
{
  apr_status_t st = APR_SUCCESS;
  SHMLOCK_ASSERT(l);

#ifdef ZEKE_USE_THREADS
  st = apr_thread_rwlock_wrlock(l->tlock);
  if(st != APR_SUCCESS)
    return st;
#endif

  if(!block)
    block = APR_FLOCK_NONBLOCK;
  else
    block = 0;
  st = apr_file_lock(l->f, l->w_type|block);
#ifdef ZEKE_USE_THREADS
  if(st != APR_SUCCESS)
    apr_thread_rwlock_unlock(l->tlock);
#endif
  if(st == APR_SUCCESS)
    l->wlocked++;
  return st;
}

ZEKE_API(apr_status_t) zeke_shmlock_unlock(zeke_shmlock_t *l)
{
  apr_status_t st1 = APR_SUCCESS, st2;
  SHMLOCK_ASSERT(l);

#ifdef ZEKE_USE_THREADS
  st1 = apr_thread_rwlock_unlock(l->tlock);
#endif
  st2 = apr_file_unlock(l->f);
  if(st1 == APR_SUCCESS)
    st1 = st2;
  if(st1 == APR_SUCCESS) {
    l->rlocked = 0;
    l->wlocked = 0;
  }
  return st1;
}

ZEKE_API(struct zeke_shmlog_header*) zeke_shmlog_open(apr_pool_t *p, int nrecs)
{
  apr_status_t st;
  struct zeke_shmlog_header *hdr = NULL;
  zeke_shmlock_t *lock = NULL;
  int needs_init = 0;
  const char *shmname;

  if(nrecs <= 0)
    nrecs = ZEKE_LOG_SIZE;
  if(!shmlog) {
    shmname = get_shmlog(p,0);
    assert(shmname != NULL);
    assert(shmpool != NULL);

    st = apr_shm_attach(&shmlog,shmname,shmpool);
    if(st == APR_SUCCESS) {
      hdr = apr_shm_baseaddr_get(shmlog);
      SHMLOG_ASSERT(hdr);
      if(hdr->nelts != nrecs) {
        const char *newname = get_shmlog(p,1);
        apr_shm_destroy(shmlog);
        shmlog = NULL;
        shmname = newname;
      }
    } else {
      if(st == APR_ENOENT)
        apr_shm_remove(shmname,shmpool);
      shmlog = NULL;
    }
    if(shmlog == NULL) {
      apr_size_t sz = (nrecs * APR_ALIGN_DEFAULT(sizeof(zeke_log_entry_t)) +
                       APR_ALIGN_DEFAULT(sizeof(struct zeke_shmlog_header)));
      st = apr_shm_create(&shmlog,sz,shmname,shmpool);
      if(st != APR_SUCCESS) {
        int i;
        if(st == APR_ENOENT)
          apr_shm_remove(shmname,shmpool);
        for(i = 0; st != APR_SUCCESS && i < 2; i++) {
          st = apr_shm_create(&shmlog,sz,shmname,shmpool);
          if(st != APR_SUCCESS) {
            apr_sleep(APR_TIME_C(100) * APR_TIME_C(1000));
            apr_shm_remove(shmname,shmpool);
          }
        }
        if(st != APR_SUCCESS) {
#ifdef DEBUGGING
          zeke_apr_eprintf(st,"shm error: %s",shmname);
          abort();
#else
          zeke_fatal(shmname,st);
#endif
        }
      }
      assert(shmlog != NULL);
      detach.shmlog = 0;
      needs_init++;
    }
  }
  hdr = apr_shm_baseaddr_get(shmlog);
  if(needs_init)
    apr_cpystrn(hdr->lock,shmlog_lockname,sizeof(hdr->lock));

  if(apr_pool_userdata_get((void**)&lock,"lock",shmpool) != APR_SUCCESS || !lock) {
    st = zeke_shmlock_create(&lock,hdr->lock,APR_LOCK_DEFAULT,shmpool);
    if(st != APR_SUCCESS)
      zeke_fatal(hdr->lock,st);
    apr_pool_userdata_set(lock,"lock",shmlock_cleanup,shmpool);
  }
  if(needs_init) {
    hdr->magic = ZEKE_LOG_HEADER_MAGIC;
    hdr->nelts = nrecs;
    hdr->first = 0;
    hdr->next = 0;
    hdr->read_locks = 0;
    hdr->write_locks = 0;
    apr_pool_cleanup_register(shmpool,hdr,zeke_shmlog_cleanup,zeke_shmlog_cleanup);
  } else SHMLOG_ASSERT(hdr);
  return hdr;
}

ZEKE_API(apr_status_t) zeke_shmlog_lock(struct zeke_shmlog_header *hdr)
{
  apr_status_t st;
  zeke_shmlock_t *l;

  SHMLOG_ASSERT(hdr);
  assert(shmpool != NULL);
  st = apr_pool_userdata_get((void**)&l,"lock",shmpool);
  if(st != APR_SUCCESS) {
    zeke_apr_eprintf(st,"unable to lock shmlog");
    return st;
  }
  SHMLOCK_ASSERT(l);
  st = zeke_shmlock_wrlock(l,0);
  if(st == APR_SUCCESS) {
    hdr->write_locks++;
#ifdef DEBUGGING
    LOG("%s LOCKED (count=%d)",hdr->lock, (int)hdr->write_locks);
#endif
  }
  return st;
}

ZEKE_API(apr_status_t) zeke_shmlog_unlock(struct zeke_shmlog_header *hdr)
{
  apr_status_t st;
  zeke_shmlock_t *l;

  SHMLOG_ASSERT(hdr);
  assert(shmpool != NULL);
  st = apr_pool_userdata_get((void**)&l,"lock",shmpool);
  if(st != APR_SUCCESS)
    return st;
  st = zeke_shmlock_unlock(l);
  if(st == APR_SUCCESS) {
    hdr->write_locks--;
#ifdef DEBUGGING
    LOG("%s UNLOCKED (count=%d)",hdr->lock,(int)hdr->write_locks);
#endif
  }
  return st;
}

ZEKE_API(apr_status_t) zeke_shmlog_size_get(struct zeke_shmlog_header *hdr,
                                            apr_uint32_t *szp,
                                            apr_uint32_t *fp,
                                            apr_uint32_t *np)
{
  int locked = 0;

  if(!hdr)
    return APR_EINVAL;
  SHMLOG_ASSERT(hdr);
  if(szp && zeke_shmlog_lock(hdr) == APR_SUCCESS)
    locked++;

  if(szp)
    *szp = hdr->nelts;
  if(fp)
    *fp = hdr->first;
  if(np)
    *np = hdr->next;

  if(locked)
    zeke_shmlog_unlock(hdr);
  return APR_SUCCESS;
}

ZEKE_API(apr_status_t) zeke_shmlog_size_set(struct zeke_shmlog_header **hdrp,
                                            apr_uint32_t sz)
{
  struct zeke_shmlog_header *hdr;
  apr_status_t st;
  apr_uint16_t locked = 0;

  if(sz < 1 || !hdrp || !*hdrp)
    return APR_EINVAL;
  hdr = *hdrp;
  SHMLOG_ASSERT(hdr);
  locked = hdr->write_locks;
  st = zeke_shmlog_lock(hdr);
  if(st != APR_SUCCESS)
    return st;

  if(sz != hdr->nelts) {
    apr_shm_t *old = shmlog;
    shmlog = NULL;
    get_shmlog(shmpool,1);
    *hdrp = zeke_shmlog_open(NULL,(unsigned)sz);
    if(*hdrp)
      apr_shm_destroy(old);
  }
  if(!locked)
    st = zeke_shmlog_unlock(hdr);
  return st;
}

static apr_status_t reset_shmlog_ident(void *id)
{
  shmlog_ident = (char*)id;
  if(shmlog_base != shmlog_ident)
    shmlog_base = shmlog_ident;
  return APR_SUCCESS;
}

ZEKE_API(void) zeke_shmlog_ident_set(const char *i)
{
  if(i && *i) {
    if(!shmpool) {
      if(!shmlog_base)
        shmlog_base = shmlog_ident;
      assert(apr_pool_create(&shmpool,NULL) == APR_SUCCESS);
      zeke_pool_cleanup_register(shmpool, shmlog_ident, reset_shmlog_ident);
    }
    if(shmlog_base == shmlog_ident)
      shmlog_base = apr_pstrdup(shmpool,i);
    shmlog_ident = apr_pstrndup(shmpool,i,31);
  }
}

ZEKE_API(const char*) zeke_shmlog_ident_get(void)
{
  return shmlog_ident;
}

ZEKE_API(const zeke_log_entry_t*)
zeke_shmlog_write(struct zeke_shmlog_header *hdr, const char *msg,
                                                 apr_size_t sz)
{
  zeke_log_entry_t *e;
  apr_uint32_t next;
  SHMLOG_ASSERT(hdr);

  e = shmlog_idx(hdr,hdr->next);
  assert(e != NULL);
  apr_cpystrn(e->source, shmlog_ident, sizeof(e->source));
  memcpy(e->info,msg,sz);
  if(sz < sizeof(e->info))
    e->info[sz] = '\0';
  if(hdr->next >= hdr->nelts)
    next = 0;
  else
    next = hdr->next + 1;
  if(hdr->first == next) {
    if(hdr->first >= hdr->nelts)
      hdr->first = 0;
    else
      hdr->first++;
  }
  hdr->next = next;
  return e;
}

ZEKE_API(const zeke_log_entry_t*)
zeke_shmlog_entry_get(struct zeke_shmlog_header *hdr, unsigned i)
{
  if(!hdr || i >= hdr->nelts)
    return NULL;

  SHMLOG_ASSERT(hdr);
  return shmlog_idx(hdr,i);
}

ZEKE_API(const zeke_log_entry_t*)
zeke_shmlog_read(struct zeke_shmlog_header *hdr, int consume)
{
  zeke_log_entry_t *e = NULL;
  apr_uint32_t first,pos;
  int update = 0;
  SHMLOG_ASSERT(hdr);

  pos = hdr->first;
  for(first = hdr->first; first != hdr->next && consume > 0; consume--) {
    pos = first;
    update++;
    if(++first > hdr->nelts)
      first = 0;
  }
  if(pos != hdr->next)
    e = shmlog_idx(hdr,pos);
  if(update)
    hdr->first = first;
  return e;
}

/* stream interface */
static zslogf_t *zslogf_new(apr_pool_t *p)
{
  zslogf_t *f;
  struct zeke_shmlog_header *hdr;

  hdr = zeke_shmlog_open(p,-1);
  SHMLOG_ASSERT(hdr);
  f = calloc(1,sizeof(*f));
  if(f != NULL) {
    f->magic = ZEKE_SHMLOG_STREAM_MAGIC;
    f->hdr = hdr;
    f->idx = -1;
  }
  return f;
}

static int read_copy(zslogf_t *f, char **bufp, zk_size_t *lenp, apr_size_t *szp)
{
  apr_size_t sz;
  assert(f->rcp != NULL);
  sz = strlen(f->rcp);
  if(sz > *lenp)
    sz = *lenp;
  memcpy(*bufp,f->rcp,sz);
  *bufp += sz;
  *lenp += sz;
  if(!*f->rcp)
    f->rcp = NULL;
  if(*lenp)
    **bufp = '\0';
  *szp += sz;
  return *lenp;
}

static zk_ssize_t zslogf_read(void *fp, char *buf, zk_size_t len)
{
  zslogf_t *f = (zslogf_t*)fp;
  zeke_log_entry_t *e;
  apr_size_t sz = 0;

  if(!buf || !f) {
    errno = EINVAL;
    return -1;
  }
  SHMLOG_STREAM_ASSERT(f);
  SHMLOG_ASSERT(f->hdr);
  if(f->rcp) {
    if(!read_copy(f,&buf,&len,&sz))
      return (int)sz;
  }
  if (f->idx == -1)
    f->idx = f->hdr->first;
  if(f->idx == f->hdr->next) {
    f->idx = -1;
    return 0;
  }
  e = shmlog_idx(f->hdr,f->idx);
  if(len >= ZEKE_LOG_ENTRY_SIZE)
    len = ZEKE_LOG_ENTRY_SIZE-1;
  memcpy(f->rbuf,e->info,len);
  f->rbuf[ZEKE_LOG_ENTRY_SIZE-1] = '\0';
  f->rcp = f->rbuf;
  read_copy(f,&buf,&len,&sz);
  return (zk_ssize_t)sz;
}

static zk_ssize_t zslogf_write(void *fp, const char *buf, zk_size_t len)
{
  zslogf_t *f = (zslogf_t*)fp;
  apr_ssize_t l = len;
  apr_size_t sz,total = 0;
  if(!buf || !f) {
    errno = EINVAL;
    return -1;
  }
  SHMLOG_STREAM_ASSERT(f);
  SHMLOG_ASSERT(f->hdr);

  for(; l > 0; l -= sz) {
    sz = (l > DEFAULT_BUFLEN-1 ? DEFAULT_BUFLEN-1 : l);
    zeke_shmlog_write(f->hdr,buf,sz);
    total += sz;
  }
  return (int)total;
}

static int zslogf_close(void *fp)
{
  zslogf_t *f = (zslogf_t*)fp;
  SHMLOG_STREAM_ASSERT(f);

  free(f);
  return 0;
}

static apr_status_t zslogf_cleanup(void *fp)
{
  fclose((FILE*)fp);
  return APR_SUCCESS;
}

ZEKE_API(FILE*) zeke_shmlog_stream_open(struct zeke_shmlog_header **hp,
                                        apr_pool_t *cont)
{
  zslogf_t *f = zslogf_new(cont);
  FILE *fp = NULL;

  if(f) {
    SHMLOG_STREAM_ASSERT(f);

    if(hp)
      *hp = f->hdr;

#ifdef HAVE_FUNOPEN
    fp = funopen(f,zslogf_read,zslogf_write, NULL, zslogf_close);
#elif defined(HAVE_FOPENCOOKIE)
    do {
      static cookie_io_functions_t funcs = {
        .read = zslogf_read,
        .write = zslogf_write,
        .seek = NULL,
        .close = zslogf_close
      };
      fp = fopencookie(f,"w+",funcs);
    } while(0);
#else
#error "not supported"
#endif /* HAVE_FUNOPEN */

    if(fp && cont) {
      apr_pool_cleanup_register(cont,fp,apr_pool_cleanup_null,zslogf_cleanup);
      apr_pool_pre_cleanup_register(cont,fp,zslogf_cleanup);
    }
  }
  return fp;
}
