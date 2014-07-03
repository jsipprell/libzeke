/* libzeke regular expression support.

currently only pcre is supported
*/
#include "internal.h"

#include "libzeke_regex.h"
#include <pcre.h>

#ifdef ZEKE_SINGLE_THREADED
# ifdef ZEKE_USE_THREADS
# undef ZEKE_USE_THREADS
# endif
#elif defined(APR_HAS_THREADS)
# ifndef ZEKE_USE_THREADS
# define ZEKE_USE_THREADS
# endif
#endif

#define ZEKE_REGEX_ERROR(r,err,msg,off) \
        zeke_regex_error_set((zeke_regex_t*)(r),(err),(msg),(off))

#define ZEKE_REGEX_STATUS(r,st) { \
        if(ZEKE_STATUS_IS_OKAY((st))) { \
          ZEKE_REGEX_ERROR(r,0,NULL,0); \
        } else if((st) != ZEREGEXERROR && !ZEKE_STATUS_IS_REGEX_ERROR((st))) { \
          ZEKE_REGEX_ERROR(r,0,NULL,0); \
          ((zeke_regex_t*)(r))->status = (st); \
        } else if((st) == ZEREGEXERROR) { \
          ((zeke_regex_t*)(r))->status = APR_FROM_ZK_ERROR((st)); \
        } else { \
          ((zeke_regex_t*)(r))->status = (st); } }

typedef void *(*regex_malloc_fn)(size_t);
typedef void (*regex_free_fn)(void*);

#ifdef LIBZEKE_SERIALIZE_PCRE
#define ZEKE_RE_MANAGED(p) zre_set_memory_pool((p),NULL);
#define ZEKE_RE_UNMANAGED(p) zre_unset_memory_pool((p),NULL)
#define ZEKE_R_BEGIN(r) zre_set_memory_pool((r)->pool,(r)->re);
#define ZEKE_R_END(r) zre_unset_memory_pool((r)->pool,(r)->re)
#define ZEKE_M_BEGIN(m) zre_set_memory_pool((m)->pool,((pcre*)(m))->re);
#define ZEKE_M_END(m) zre_unset_memory_pool((r)->pool,((pcre*)(m))->re)
#define ZPCRE_FREE(r)
#define ZPCRE_SUBSTRING(p,s) (s)
#else /* !LIBZEKE_SERIALIZE_PCRE */
#define ZEKE_RE_MANAGED(p) /* noop */
#define ZEKE_RE_UNMANAGED(p) /* noop */
#define ZEKE_R_BEGIN(r) /* noop */
#define ZEKE_R_END(r) /* noop */
#define ZEKE_M_BEGIN(r) /* noop */
#define ZEKE_M_END(r) /* noop */
#define ZPCRE_FREE(r) pcre_free( (r) )
#define ZPCRE_SUBSTRING(p,s) _zpcre_dup_substring( (p), (s) )
#endif /* LIBZEKE_SERIALIZE_PCRE */

#ifndef ZEKE_RE_DEFAULT_MAX_GROUPS
#define ZEKE_RE_DEFAULT_MAX_GROUPS 30
#endif

#define SETIDXPAIR(dst,src) \
    { *(dst) = *(src); \
      *((dst)+1) = (*(src) < 0 || *((src)+1) < *(dst)) ? -1 : ( *((src)+1) - *(src) ); }

#define GETIDXOFF(idx) *(idx)
#define GETIDXLEN(idx) *((idx)+1)
#define GETIDXNEXT(idx) *(idx) + *((idx)+1)


#define ZEKE_INDEX_GROUP(arr,grp) &APR_ARRAY_IDX((arr),((grp)-1)*2,int)

struct zeke_regex_t {
  apr_pool_t *pool;
  pcre *re;
  pcre_extra *extra;
  const char *pattern;
  apr_uint64_t flags;
  int err;
  int erroffset;
  zk_status_t status;
  int self_owned_pool;
  const char *errmsg;
};

struct zeke_regex_match_t {
  apr_pool_t *pool;
  const zeke_regex_t *regex;
  const pcre *re;
  const char *subject;
  int *ovector;
  apr_uint64_t flags;
  apr_hash_t *named_group_cache;
  apr_uint32_t start,end;
  apr_array_header_t *indices;
  apr_array_header_t *groups;
  int self_owned_pool;
};

ZEKE_POOL_IMPLEMENT_ACCESSOR(regex)
ZEKE_POOL_IMPLEMENT_ACCESSOR(regex_match)

static apr_pool_t *regex_global_pool = NULL;
static apr_pool_t *regex_pool = NULL;
#if defined(ZEKE_USE_THREADS) && defined(LIBZEKE_SERIALIZE_PCRE)
static apr_thread_mutex_t *regex_memmgt_mutex = NULL;
#endif /* ZEKE_USE_THREADS && LIBZEKE_SERIALIZE_PCRE */
#ifdef LIBZEKE_SERIALIZE_PCRE
static regex_malloc_fn save_malloc = NULL;
static regex_free_fn save_free = NULL;
#endif

static apr_status_t match_cleanup(void*);
static apr_status_t remove_match_cleanup(void*);
static void zeke_regex_error_set(zeke_regex_t*, int, const char*, apr_off_t);

ZEKE_PRIVATE(void) zeke_regex_init(void)
{
  if(regex_global_pool == NULL)
    regex_global_pool = zeke_root_subpool_create();
  if(regex_pool == NULL)
    regex_pool = regex_global_pool;

#if defined(ZEKE_USE_THREADS) && defined(LIBZEKE_SERIALIZE_PCRE)
  if(regex_memmgt_mutex == NULL) {
    assert(apr_thread_mutex_create(&regex_memmgt_mutex,APR_THREAD_MUTEX_NESTED,regex_global_pool) == APR_SUCCESS);
  }
#endif
}

#ifndef LIBZEKE_SERIALIZE_PCRE
static inline const char *_zpcre_dup_substring(apr_pool_t *pool, const char *s)
{
  const char *ds = s ? apr_pstrdup(pool,s) : NULL;

  assert(!s || ds != NULL);
  if(s)
    pcre_free_substring(s);
  return ds;
}

#else /* LIBZEKE_SERIALIZE_PCRE */
static void *zre_regex_alloc(size_t sz)
{
  assert(regex_global_pool != NULL);
  if(regex_pool == regex_global_pool)
    WARN("allocating %lu bytes from global memory pool for a regex (this will leak)",(unsigned int)sz);
  return apr_palloc(regex_pool,sz);
}

static void zre_regex_free(void *ignored)
{
  assert(regex_global_pool != NULL);
  if(regex_pool == regex_global_pool)
    WARN("freeing memory without global memory pool being locked");
}

static apr_status_t force_unlock(void *ignored)
{
  if(!regex_pool || regex_pool == regex_global_pool)
    return APR_SUCCESS;

  regex_pool = regex_global_pool;
#ifdef ZEKE_USE_THREADS
  apr_thread_mutex_unlock(regex_memmgt_mutex);
#endif
  return APR_SUCCESS;
}

static apr_status_t force_pcre_decref(void *data)
{
  assert(pcre_refcount((pcre*)data,-1) == 0);
  return APR_SUCCESS;
}

static void zre_set_memory_pool(apr_pool_t *pool,pcre *r)
{
  if(pool == regex_pool)
    return;
#ifdef ZEKE_USE_THREADS
  assert(apr_thread_mutex_lock(regex_memmgt_mutex) == APR_SUCCESS);
#endif
  if(!regex_pool || regex_pool == regex_global_pool) {
    save_malloc = pcre_malloc;
    save_free = pcre_free;
  }

  regex_pool = pool;
  apr_pool_cleanup_register(pool,NULL,force_unlock,apr_pool_cleanup_null);
  if(r) {
    pcre_refcount(r,1);
    apr_pool_cleanup_register(pool,r,force_pcre_decref,apr_pool_cleanup_null);
  }
  pcre_malloc = zre_regex_alloc;
  pcre_free = zre_regex_free;
}

static void zre_unset_memory_pool(apr_pool_t *pool,pcre *r)
{
  assert(regex_pool == pool);
  if(r) {
    apr_pool_cleanup_kill(pool,r,force_pcre_decref);
    assert(pcre_refcount(r,-1) == 0);
  }
  regex_pool = regex_global_pool;
  apr_pool_cleanup_kill(pool,NULL,force_unlock);

  if(save_malloc != NULL) {
    pcre_malloc = save_malloc;
    save_malloc = NULL;
  }
  if(save_free != NULL) {
    pcre_free = save_free;
    save_free = NULL;
  }

#ifdef ZEKE_USE_THREADS
  assert(apr_thread_mutex_unlock(regex_memmgt_mutex) == APR_SUCCESS);
#endif
}
#endif /* LIBZEKE_SERIALIZE_PCRE */

#define ZPCREFLAG(f) if((flags & ZEKE_RE_##f) == ZEKE_RE_##f) options |= PCRE_##f
#define ZPCREFLAGX(in,out) if((flags & (in)) == (in)) options |= (out)

static int flags_to_pcre_options(apr_uint64_t flags)
{
  int options = 0;

  ZPCREFLAG(ANCHORED);
#ifdef PCRE_BSR_ANYCRLF
  ZPCREFLAG(BSR_ANYCRLF);
#endif
#ifdef PCRE_BSR_UNICODE
  ZPCREFLAG(BSR_UNICODE);
#endif
  ZPCREFLAG(CASELESS);
  ZPCREFLAG(DOLLAR_ENDONLY);
  ZPCREFLAG(DOTALL);
#ifdef PCRE_DUPNAMES
  ZPCREFLAG(DUPNAMES);
#endif
  ZPCREFLAG(EXTENDED);
  ZPCREFLAG(FIRSTLINE);
#ifdef PCRE_JAVASCRIPT_COMPAT
  ZPCREFLAGX(ZEKE_RE_JS_COMPAT,PCRE_JAVASCRIPT_COMPAT);
#endif
  ZPCREFLAG(MULTILINE);
#ifdef PCRE_NEWLINE_CR
  ZPCREFLAG(NEWLINE_CR);
#endif
#ifdef PCRE_NEWLINE_LF
  ZPCREFLAG(NEWLINE_LF);
#endif
#ifdef PCRE_NEWLINE_CRLF
  ZPCREFLAG(NEWLINE_CRLF);
#endif
#ifdef PCRE_NEWLINE_ANYCRLF
  ZPCREFLAG(NEWLINE_ANYCRLF);
#endif
#ifdef PCRE_NEWLINE_ANY
  ZPCREFLAG(NEWLINE_ANY);
#endif
  ZPCREFLAG(NO_AUTO_CAPTURE);
  ZPCREFLAG(UNGREEDY);
  ZPCREFLAG(UTF8);
  ZPCREFLAG(NO_UTF8_CHECK);
  ZPCREFLAG(NOTBOL);
  ZPCREFLAG(NOTEOL);
  ZPCREFLAG(NOTEMPTY);

  if(flags & ZEKE_RE_CASE_SENSITIVE)
    options &= ~PCRE_CASELESS;

  return options;
}

static apr_status_t zeke_regex_cleanup(void *data)
{
  zeke_regex_t *r = (zeke_regex_t*)data;

  if(r->extra) {
    pcre_extra *extra = r->extra;
    r->extra = NULL;
    ZPCRE_FREE(extra);
  }

  if(r->re) {
    pcre *re = r->re;
    r->re = NULL;
    ZPCRE_FREE(re);
  }

  return APR_SUCCESS;
}

/* API */
ZEKE_API(zk_status_t) zeke_regex_create_ex(zeke_regex_t **regex,
                                           const char *pattern,
                                           apr_uint64_t flags,
                                           apr_pool_t *pool)
{
  zeke_regex_t *r;
  int self_created_pool = 0;

  if(pattern == NULL || !regex)
    return APR_EINVAL;

  if(regex_global_pool == NULL)
    zeke_regex_init();
  if(pool == NULL)
    pool = regex_global_pool;
  if(flags & ZEKE_RE_SUBPOOL) {
    assert(apr_pool_create(&pool,pool) == APR_SUCCESS);
    zeke_pool_tag(pool,apr_psprintf(pool,"regex subpool for /%s/",pattern));
    self_created_pool++;
  }
  r = apr_palloc(pool,sizeof(zeke_regex_t));
  assert(r != NULL);
  r->pool = pool;
  if((flags & ZEKE_RE_NO_PATTERN_COPY) == 0)
    r->pattern = apr_pstrdup(pool,pattern);
  else
    r->pattern = pattern;
  r->flags = flags;
  r->extra = NULL;
  r->err = r->erroffset = 0;
  r->errmsg = NULL;
  r->self_owned_pool = self_created_pool;
  r->status = ZEOK;
  apr_pool_cleanup_register(pool,r,zeke_regex_cleanup,apr_pool_cleanup_null);

  ZEKE_RE_MANAGED(pool) {
    const char *e = NULL;
    r->re = pcre_compile2(r->pattern,flags_to_pcre_options(flags),
                          &r->err,&e,&r->erroffset,NULL);
    assert(r->re != NULL || (e && r->err != 0));
    if(e)
      r->errmsg = apr_pstrdup(pool,e);
    if(r->re != NULL) {
      if(r->flags & ZEKE_RE_STUDY) {
        r->extra = pcre_study(r->re,0,&e);
        if(e)
          r->errmsg = apr_pstrdup(pool,e);
      }
    }
  } ZEKE_RE_UNMANAGED(pool);

  *regex = r;
  if(!r->re || r->err != 0)
    ZEKE_REGEX_STATUS(r,ZEREGEXERROR)
  else if(r->err == 0 && r->re)
    ZEKE_REGEX_STATUS(r,ZEOK)
  return r->status;
}

ZEKE_API(zk_status_t) zeke_regex_create(zeke_regex_t **regex,
                                        const char *pattern,
                                        apr_uint64_t flags)
{
  flags |= ZEKE_RE_STUDY|ZEKE_RE_UTF8|ZEKE_RE_SUBPOOL;
  return zeke_regex_create_ex(regex,pattern,flags,NULL);
}

ZEKE_API(zk_status_t) zeke_regex_destroy(const zeke_regex_t *regex)
{
  zeke_regex_t *r = (zeke_regex_t*)regex;
  apr_pool_t *pool = r->pool;

  apr_pool_cleanup_kill(pool,NULL,remove_match_cleanup);
  if(r->self_owned_pool) {
    assert(pool != regex_pool);
    apr_pool_destroy(pool);
  }

  return APR_SUCCESS;
}

ZEKE_API(zk_status_t) zeke_regex_match_destroy(const zeke_regex_match_t *match)
{
  zeke_regex_match_t *m = (zeke_regex_match_t*)match;
  apr_pool_t *pool = m->pool;

  if(m->self_owned_pool) {
    assert(pool != regex_pool);
    apr_pool_destroy(pool);
  } else {
    apr_pool_cleanup_kill(pool,m,match_cleanup);
    if(m->regex && m->regex->pool)
      apr_pool_cleanup_kill(m->regex->pool,m,remove_match_cleanup);
    m->pool = NULL;
    m->regex = NULL;
  }
  return APR_SUCCESS;
}

ZEKE_API(zk_status_t) zeke_regex_status(const zeke_regex_t *r)
{
  if(r == NULL)
    return APR_EINVAL;
  return r->status;
}

static void zeke_regex_error_set(zeke_regex_t *r, int err, const char *msg, apr_off_t offset)
{
  r->err = err;
  r->erroffset = offset;
  r->errmsg = msg;
  if(err < 0 || msg != NULL)
    r->status = APR_FROM_ZK_ERROR(ZEREGEXERROR);
  else if(err == 0 && msg == NULL)
    r->status = APR_SUCCESS;
}

ZEKE_API(const char*) zeke_regex_error_get(const zeke_regex_t *r,
                                           const char **where,
                                           apr_off_t *offset)
{
  if(!r->errmsg)
    return NULL;

  if(offset)
    *offset = r->erroffset;
  if(where)
    *where = (r->pattern+r->erroffset);
  return r->errmsg;
}

ZEKE_API(const char*) zeke_regex_errstr(const zeke_regex_t *r, apr_pool_t *pool)
{
  return zeke_regex_errstr_ex(r,r->status,pool);
}

ZEKE_API(const char*) zeke_regex_errstr_ex(const zeke_regex_t *r,
                                           zk_status_t st,
                                           apr_pool_t *pool)
{
  if(pool == NULL && r != NULL)
    pool = r->pool;

  if(ZEKE_STATUS_IS_REGEX_ERROR(st) && r != NULL) {
    const char *where = NULL;
    apr_off_t offset = 0;
    const char *err = zeke_regex_error_get(r,&where,&offset);

    if(where)
      return apr_psprintf(pool,"%s (%"APR_OFF_T_FMT"@/%s/)",
                          err,offset,where);
    return (pool == r->pool ? err : apr_pstrdup(pool,err));
  }

  return zeke_perrstr(st,pool);
}

static const char *zeke_regex_error_vfmt(const zeke_regex_t *r,
                                           zk_status_t st,
                                           apr_pool_t *pool,
                                           const char *fmt, va_list ap)
{
  const char *msg = "";

  if(pool == NULL && r != NULL)
    pool = r->pool;

  if(fmt != NULL)
    msg = apr_pstrcat(pool,apr_pvsprintf(pool,fmt,ap),": ",NULL);

  if(ZEKE_STATUS_IS_REGEX_ERROR(st) && r != NULL) {
    const char *where = NULL;
    apr_off_t offset = 0;
    const char *err = zeke_regex_error_get(r,&where,&offset);

    if(where)
      return apr_psprintf(pool,"%s%s (%"APR_OFF_T_FMT"@/%s/)",
                          msg,err,offset,where);
    return apr_pstrcat(pool,err,NULL);
  }

  return apr_pstrcat(pool,msg,zeke_perrstr(st,pool),NULL);
}

ZEKE_API(const char*) zeke_regex_error_fmt(const zeke_regex_t *r,
                                           apr_pool_t *pool, const char *fmt, ...)
{
  const char *e;
  va_list ap;

  va_start(ap,fmt);
  e = zeke_regex_error_vfmt(r,r->status,pool,fmt,ap);
  va_end(ap);

  return e;
}

ZEKE_API(const char*) zeke_regex_error_fmt_ex(const zeke_regex_t *r,
                                           zk_status_t st,
                                           apr_pool_t *pool,
                                           const char *fmt, ...)
{
  const char *e;
  va_list ap;

  va_start(ap,fmt);
  e = zeke_regex_error_vfmt(r,st,pool,fmt,ap);
  va_end(ap);

  return e;
}

static apr_status_t match_cleanup(void *data)
{
  zeke_regex_match_t *m = (zeke_regex_match_t*)data;

  assert(m->pool != NULL);
  if(m->regex && m->regex->pool)
    apr_pool_cleanup_kill(m->regex->pool,data,remove_match_cleanup);
  m->re = NULL;
  m->pool = NULL;
  m->regex = NULL;
  return APR_SUCCESS;
}

static apr_status_t remove_match_cleanup(void *data)
{
  zeke_regex_match_t *m = (zeke_regex_match_t*)data;

  assert(m->pool != NULL);
  if(m->pool && m->regex) {
    apr_pool_cleanup_kill(m->pool,data,match_cleanup);
    m->regex = NULL;
  }
  return APR_SUCCESS;
}

static void capture_groups(zeke_regex_match_t *match, const char *subject,
                            int nvecs)
{
  int i;

  if(nvecs > 1) {
    match->indices = apr_array_make(match->pool,nvecs-1,sizeof(int)*2);
    for(i = 1; i < nvecs; i++) {
      int *idx  = (int*)apr_array_push(match->indices);
      SETIDXPAIR(idx,match->ovector+i*2);
    }
  }

  match->groups = apr_array_make(match->pool,nvecs,sizeof(const char*));
  for(i = 0; i < nvecs; i++) {
    const char *group = NULL;
    int err;
    int autofree = 1;
    err = pcre_get_substring(subject,match->ovector,nvecs,i,&group);
    if(err < 0) {
      group = apr_psprintf(match->pool,"ERROR %d",err);
      ERR("%s: %s",subject,group);
      autofree = 0;
    }
    assert(group != NULL);
    APR_ARRAY_PUSH(match->groups,const char*) = (autofree ? ZPCRE_SUBSTRING(match->pool,group) : group);
  }
}

ZEKE_API(zeke_regex_match_t*) zeke_regex_exec_ex(const zeke_regex_t *r,
                                                 const char *subject,
                                                 apr_size_t *groups,
                                                 apr_ssize_t max_groups,
                                                 apr_pool_t *pool)
{
  zeke_regex_match_t *match = NULL;
  int self_created_pool = 0;
  int *ovec = NULL;
  int ovecsize;

  assert(r != NULL && r->pool != NULL);
  if(!r || !subject || !r->re) {
    ZEKE_REGEX_STATUS(r,APR_EINVAL)
    return NULL;
  }
  if(pool == NULL) {
    if((r->flags & ZEKE_RE_SUBPOOL) || r->pool == regex_global_pool) {
      assert(apr_pool_create(&pool,r->pool) == APR_SUCCESS);
      self_created_pool++;
    } else
      pool = r->pool;
  }
  
  if(max_groups == -1)
    max_groups = ZEKE_RE_DEFAULT_MAX_GROUPS;

  ovecsize = (max_groups+1)*3;
  ovec = apr_palloc(pool,ovecsize*sizeof(int));
  assert(ovec != NULL);
  ZEKE_R_BEGIN(r) {
    int i;

    subject = apr_pstrdup(pool,subject);
    if(r->flags & ZEKE_RE_ONCE) {
      int workspace[21];
      i = pcre_dfa_exec(r->re,r->extra,subject,strlen(subject),0,0,ovec,ovecsize,
                        workspace,20);
    } else {
      i = pcre_exec(r->re,r->extra,subject,strlen(subject),0,0,ovec,ovecsize);
    }
    if(i > 0) {
      match = apr_palloc(pool,sizeof(zeke_regex_match_t));
      if(match != NULL) {
        match->pool = pool;
        match->re = r->re;
        match->regex = r;
        match->subject = subject;
        match->flags = r->flags;
        match->ovector = ovec;
        match->start = *ovec;
        match->end = *(ovec+1);
        match->indices = NULL;
        match->named_group_cache = NULL;
        match->groups = NULL;
        match->self_owned_pool = self_created_pool;
        assert(pool != NULL);
        if(pool != r->pool) {
          apr_pool_cleanup_register(pool,match,match_cleanup,apr_pool_cleanup_null);
          apr_pool_pre_cleanup_register(r->pool,match,remove_match_cleanup);
        }

        capture_groups(match,subject,i);
        ZEKE_REGEX_STATUS(r,APR_SUCCESS)
      } else
        ZEKE_REGEX_STATUS(r,APR_ENOMEM)
    } else if(i == 0) {
      match = NULL;
      ZEKE_REGEX_ERROR(r,PCRE_ERROR_NOMATCH,"no match",0);
    } else if (i < 0) {
      match = NULL;
      switch(i) {
      case PCRE_ERROR_NOMEMORY:
        ZEKE_REGEX_ERROR(r,i,"insufficient memory",0);
        break;
      case PCRE_ERROR_NOMATCH:
        ZEKE_REGEX_ERROR(r,i,"no match",0);
        break;
      case PCRE_ERROR_BADUTF8:
#if defined(PCRE_ERROR_BADUTF16) && PCRE_ERROR_BADUTF16 != PCRE_ERROR_BADUTF8
      case PCRE_ERROR_BADUTF16:
#endif
#if defined(PCRE_ERROR_BADUTF32) && PCRE_ERROR_BADUTF32 != PCRE_ERROR_BADUTF8
      case PCRE_ERROR_BADUTF32:
#endif
#ifdef PCRE_ERROR_BADUTF8_OFFSET
      case PCRE_ERROR_BADUTF8_OFFSET:
# if defined(PCRE_ERROR_BADUTF16_OFFSET) && PCRE_ERROR_BADUTF16_OFFSET != PCRE_ERROR_BADUTF8_OFFSET
      case PCRE_ERROR_BADUTF16_OFFSET:
# endif
#endif
        ZEKE_REGEX_ERROR(r,i,"unicode error",0);
        break;
      case PCRE_ERROR_MATCHLIMIT:
        ZEKE_REGEX_ERROR(r,i,"too many matches",0);
        break;
      case PCRE_ERROR_INTERNAL:
        ZEKE_REGEX_ERROR(r,i,"internal pcre error",0);
        break;
      case PCRE_ERROR_BADOPTION:
        ZEKE_REGEX_ERROR(r,i,"bad pcre option",0);
        break;
#ifdef PCRE_ERROR_BADNEWLINE
      case PCRE_ERROR_BADNEWLINE:
        ZEKE_REGEX_ERROR(r,i,"bad newline",0);
        break;
#endif
      default:
        ZEKE_REGEX_ERROR(r,i,apr_psprintf(r->pool,"pcre exec failure %d",i),0);
        break;
      }
    }
    if(groups && i > 0)
      *groups = i-1;

  } ZEKE_R_END(r);

  if(match == NULL && self_created_pool)
    apr_pool_destroy(pool);
  return match;
}

ZEKE_API(zeke_regex_match_t*) zeke_regex_exec(const zeke_regex_t *r,
                                              const char *subject,
                                              apr_size_t *groups)
{
  apr_ssize_t max_groups = -1;

  if(groups && *groups > 0 && *groups < 65535)
    max_groups = *groups;
  return zeke_regex_exec_ex(r,subject,groups,max_groups,NULL);
}

ZEKE_API(apr_size_t) zeke_regex_exec_groups_count(const zeke_regex_match_t *m)
{
  assert(m->indices != NULL);
  return m->indices->nelts;
}

ZEKE_API(zk_status_t) zeke_regex_match_group_index(const zeke_regex_match_t *m,
                                                  apr_uint32_t group,
                                                  apr_off_t *start,
                                                  apr_ssize_t *len,
                                                  apr_off_t *next)
{
  int *idx;
  apr_off_t off = 0;

  assert(m->indices != NULL);

  if(group-1 >= m->indices->nelts)
    return APR_ENOENT;

  idx = ZEKE_INDEX_GROUP(m->indices,group);
  off = (apr_off_t)GETIDXOFF(idx);
  if(start)
    *start = off;
  if(len)
    *len = (apr_ssize_t)GETIDXLEN(idx);
  if(off > -1 && next)
    *next = (apr_off_t)GETIDXNEXT(idx);

  return APR_SUCCESS;
}

ZEKE_API(int) zeke_regex_match_group_exists(const zeke_regex_match_t *m, apr_uint32_t group)
{
  assert(m->indices != NULL);

  if(group-1 >= m->indices->nelts)
    return 0;

  return (*ZEKE_INDEX_GROUP(m->indices,group) >= 0);
}

ZEKE_API(const char *) zeke_regex_match_group(const zeke_regex_match_t *m,
                                              apr_uint16_t group,
                                              apr_pool_t *pool)
{
  zk_status_t st;
  ZEKE_MAY_ALIAS(const char) *sub = NULL;
  apr_size_t datalen = 0;

  st = zeke_regex_match_group_get(m,group,NULL,NULL,(void**)&sub,&datalen,pool);
  if(ZEKE_STATUS_IS_OKAY(st)) {
    return sub;
  } else
    zeke_apr_eprintf(st,"group %u, m->groups->nelts=%d",(unsigned)group,m->groups->nelts);
  return NULL;
}

ZEKE_API(zk_status_t) zeke_regex_match_group_get(const zeke_regex_match_t *m,
                                                 apr_uint16_t group,
                                                 apr_off_t *start,
                                                 apr_off_t *end,
                                                 void **data,
                                                 apr_size_t *datalen,
                                                 apr_pool_t *pool)
{
  const char *sub;
  int make_copy = 0;

  if(pool == NULL) {
    pool = m->pool;
    make_copy++;
  } else if(data && (!datalen || !*datalen))
      make_copy++;

  if(!make_copy && (!data || !*data || !datalen))
    return APR_EINVAL;

  assert(m->groups != NULL);
  if(group >= m->groups->nelts)
    return APR_EINVAL;

  if(group == 0) {
    if(start)
      *start = m->start;
    if(end)
      *end = m->end;
  } else {
    int *idx;

    assert(m->indices != NULL);
    idx = ZEKE_INDEX_GROUP(m->indices,group);
    if(start)
      *start = GETIDXOFF(idx);
    if(end)
      *end = GETIDXOFF(idx)=GETIDXLEN(idx);
  }

  if(data) {
    sub = APR_ARRAY_IDX(m->groups,group,const char *);
    if(make_copy) {
      *data = apr_pstrdup(pool,sub);
      if(datalen)
        *datalen = strlen(sub);
    } else {
      if(!datalen || *datalen < strlen(sub)+1)
        return APR_EINVAL;
      apr_cpystrn((char*)data,sub,strlen(sub)+1);
      *datalen = strlen(sub);
    }
  }

  return APR_SUCCESS;
}

ZEKE_API(apr_array_header_t*) zeke_regex_match_groups_get(const zeke_regex_match_t *m,
                                                          apr_pool_t *pool)
{
  assert(m->groups != NULL);
  if(pool)
    return apr_array_copy(pool,m->groups);
  return apr_array_copy_hdr(m->pool,m->groups);
}

ZEKE_API(const char*) zeke_regex_match_errstr_ex(const zeke_regex_match_t *m, zk_status_t st,
                                                 apr_pool_t *pool)
{
  return zeke_regex_errstr_ex(m->regex,st,pool);
}

ZEKE_API(const char*) zeke_regex_match_errstr(const zeke_regex_match_t *m, apr_pool_t *pool)
{
  return zeke_regex_errstr_ex(m->regex,m->regex->status,pool);
}

ZEKE_API(int) zeke_regex_is_error(const zeke_regex_t *r)
{
  return (ZEKE_STATUS_IS_REGEX_ERROR(r->status) && r->err != PCRE_ERROR_NOMATCH);
}

ZEKE_API(int) zeke_regex_match_is_error(const zeke_regex_match_t *m)
{
  return zeke_regex_is_error(m->regex);
}

ZEKE_API(const char*) zeke_regex_match_error_fmt_ex(const zeke_regex_match_t *m, zk_status_t st,
                                                    apr_pool_t *pool, const char *fmt, ...)
{
  const char *e;
  va_list ap;

  va_start(ap,fmt);
  e = zeke_regex_error_vfmt(m->regex,st,pool,fmt,ap);
  va_end(ap);
  return e;
}

ZEKE_API(const char*) zeke_regex_match_error_fmt(const zeke_regex_match_t *m, apr_pool_t *pool,
                                                 const char *fmt, ...)
{
  const char *e;
  va_list ap;

  va_start(ap,fmt);
  e = zeke_regex_error_vfmt(m->regex,m->regex->status,pool,fmt,ap);
  va_end(ap);
  return e;
}

ZEKE_API(const char*) zeke_regex_match_named_group(const zeke_regex_match_t *m,
                                                   const char *name)
{
  const char *cp = NULL;
  apr_hash_t *h = m->named_group_cache;
  if(h && (cp = apr_hash_get(h,name,APR_HASH_KEY_STRING)) != NULL)
    return cp;

  if(h) {
    h = apr_hash_make(m->pool);
    assert(h != NULL);

    ((zeke_regex_match_t*)m)->named_group_cache = h;
  }

  if(pcre_get_named_substring(m->re,m->subject,m->ovector,m->groups->nelts,name,&cp) == 0) {
    cp = ZPCRE_SUBSTRING(m->pool,cp);
    apr_hash_set(h,name,APR_HASH_KEY_STRING,cp);
  }
  return cp;
}

