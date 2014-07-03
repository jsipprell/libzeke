#include "internal.h"
#include "connection.h"
#include "io.h"
#include "util.h"

#include "libzeke_regex.h"

#ifdef QSORT_R_GNUC
typedef int (*qsort_compare_fn)(const void*,const void*,void*);
#define QSORT_COMPARE_DECL(sym,ctx,a,b) \
              int sym (a, b, struct zeke_sort_context* ctx)
#define QSORT_R(p,n,sz,ctx,compr_fn) \
              qsort_r((p), (n), (sz), (qsort_compare_fn)(compr_fn), (ctx))
#else
typedef int (*qsort_compare_fn)(void*,const void*,const void*);
#define QSORT_COMPARE_DECL(sym,ctx,a,b) \
              int sym (struct zeke_sort_context* ctx, a, b)
#define QSORT_R(p,n,sz,ctx,compr_fn) \
        qsort_r((p), (n), (sz), (ctx), (qsort_compare_fn)(compr_fn))
#endif

#ifndef ZEKE_SEQUENCE_REGEX
#define ZEKE_SEQUENCE_REGEX "[_-](\\d{6,})$"
#endif

static char rootpath[2048] = "/";
static const char *nodepath_root = "";

extern apr_pool_t *zk_error_pool_BEGIN(void);
extern void zk_error_pool_END(const apr_pool_t*);

ZEKE_API(apr_status_t) zeke_indirect_wipe(void *data)
{
  void **p = (void**)data;
  if(p && *p)
    *p = NULL;
  return APR_SUCCESS;
}

ZEKE_API(void) zeke_pool_cleanup_register(apr_pool_t *p,
                                          const void *data,
                                          apr_status_t (*cleanup)(void*))
{
  apr_pool_cleanup_register(p,data,cleanup,apr_pool_cleanup_null);
}

ZEKE_API(const char*) zeke_nodepath_joinv(apr_pool_t *pool, va_list ap)
{
  va_list ap2;
  const char *arg;
  apr_size_t sz;
  apr_array_header_t *arr;

  va_copy(ap2,ap);

  arg = va_arg(ap,const char*);
  for(sz = 1; arg != NULL; sz++)
    arg = va_arg(ap,const char*);

  if(sz > 1) {
    arr = apr_array_make(pool,sz,sizeof(const char*));
    arg = va_arg(ap2,const char*);
    if(*nodepath_root || (arg != NULL && strcmp(arg, nodepath_root) != 0))
      APR_ARRAY_PUSH(arr,const char*) = nodepath_root;
    for(sz = 1; arg != NULL; sz++) {
      while(*arg == '/') arg++;
      if(strchr(arg,'/')) {
        char *cp = apr_pstrdup(pool,arg);
        apr_ssize_t i;

        for(i = strlen(arg)-1; i > 0; i--) {
          if(*(cp+i) == '/')
            *(cp+i) = '\0';
          else
            break;
        }
        arg = cp;
      }
      if(*arg) {
        APR_ARRAY_PUSH(arr,const char*) = arg;
      }
      arg = va_arg(ap2,const char*);
    }
  }

  va_end(ap2);

  if(sz < 2)
    return apr_pstrdup(pool,rootpath);
  return apr_array_pstrcat(pool,arr,'/');
}

ZEKE_API(const char*) zeke_nodepath_join(apr_pool_t *pool, ...)
{
  va_list ap;
  const char *np;

  va_start(ap, pool);
  np = zeke_nodepath_joinv(pool, ap);
  va_end(ap);
  return np;
}

ZEKE_API(apr_status_t) zeke_nodepath_chroot_set(const char *newroot)
{
  char chroot[2048] = "/";
  char *cp;

  if(*newroot && *newroot != '/' && *(newroot+1))
    return APR_EBADPATH;
  else while(*newroot == '/')
    newroot++;

  cp = apr_cpystrn(&chroot[1],newroot,sizeof(chroot)-1);
  if(*newroot && *(cp-1) != *(newroot+strlen(newroot)-1))
    return APR_EBADPATH;
  for(cp--; cp > chroot && *cp == '/'; cp--)
    *cp = '\0';
  apr_cpystrn(rootpath,chroot,sizeof(rootpath));
  if(strcmp(rootpath,"/") == 0)
    nodepath_root = "";
  else
    nodepath_root = rootpath;
  return APR_SUCCESS;
}

ZEKE_API(const char*) zeke_nodepath_chroot_get(void)
{
  return rootpath;
}

static apr_interval_time_t update_timers(apr_time_t *start,
                                         apr_time_t *stop,
                                         apr_interval_time_t *elapsed,
                                         apr_interval_time_t max,
                                         apr_interval_time_t interval)
{
  apr_interval_time_t timer = APR_TIME_C(-1);

  *elapsed = APR_TIME_C(0);
  if(max > APR_TIME_C(-1))
    timer = max;
  if(interval > APR_TIME_C(-1) && (interval < timer || timer == APR_TIME_C(-1)))
    timer = interval;
  if(*start == APR_TIME_C(0))
    *start = apr_time_now();
  else {
    *stop = apr_time_now();
    *elapsed = *stop - *start;
  }

  if(timer > APR_TIME_C(-1) && *elapsed > APR_TIME_C(0))
    timer -= (*elapsed > timer ? timer : *elapsed);
  return timer;
}

ZEKE_API(zk_status_t) zeke_app_run_loop(const char *zkhosts,
                                        zeke_runloop_callback_fn setup,
                                        apr_uint32_t flags,
                                        void *app_data)
{
  return zeke_app_run_loop_ex(zkhosts,-1,-1,NULL,NULL,
                              setup,NULL,flags,app_data,NULL);
}

ZEKE_API(zk_status_t) zeke_app_run_loop_interval(const char *zkhosts,
                                                 apr_interval_time_t interval_time,
                                                 zeke_runloop_callback_fn interval,
                                                 zeke_runloop_callback_fn setup,
                                                 apr_uint32_t flags,
                                                 void *app_data)
{
  return zeke_app_run_loop_ex(zkhosts,-1,interval_time,interval,NULL,
                              setup,NULL,flags,app_data,NULL);
}

ZEKE_PRIVATE(void) delayed_startup(const zeke_watcher_cb_data_t *cbd)
{
  zeke_connection_t *conn = (zeke_connection_t*)cbd->connection;
  apr_pool_t *pool = zeke_connection_pool_get(conn);
  ZEKE_MAY_ALIAS(zeke_delayed_startup_t) *startup = NULL;

  if(cbd->type == ZOO_SESSION_EVENT && cbd->state == ZOO_CONNECTED_STATE) {
    apr_pool_userdata_get((void**)&startup,LIBZEKE_RUNLOOP_DELAYED_STARTUP_KEY,pool);
    assert(startup != NULL);
    zeke_connection_named_watcher_remove(conn,startup->registered_name);
    apr_pool_userdata_setn(NULL,LIBZEKE_RUNLOOP_DELAYED_STARTUP_KEY,NULL,pool);
    startup->callback(conn,startup->data);
  }
}


ZEKE_API(zk_status_t) zeke_app_run_loop_ex(const char *zkhosts,
                                           apr_interval_time_t max_run_time,
                                           apr_interval_time_t interval_time,
                                           zeke_runloop_callback_fn interval,
                                           zeke_runloop_callback_fn timeout,
                                           zeke_runloop_callback_fn startup,
                                           zeke_runloop_callback_fn shutdown,
                                           apr_uint32_t flags,
                                           void *app_data,
                                           apr_pool_t *root_pool)
{
  zk_status_t st = ZEOK;
  static apr_pool_t *loop_pool = NULL;
  unsigned long c = 0;
  apr_interval_time_t loop_timer,run_timer;
  apr_interval_time_t elapsed = APR_TIME_C(0);
  apr_time_t start_time = APR_TIME_C(0);
  apr_time_t stop_time = APR_TIME_C(0);
  apr_pool_t *pool = NULL;
  zeke_connection_t *conn = NULL;

  if(loop_pool == NULL || loop_pool != root_pool) {
    if(root_pool != NULL)
      loop_pool = root_pool;
    else {
      loop_pool = zeke_root_subpool_create();
      zeke_pool_tag(loop_pool,"zeke_app_run_loop root pool");
    }
  }

  run_timer = max_run_time;
  if(run_timer <= APR_TIME_C(0))
    run_timer = APR_TIME_C(-1);

  if(interval) {
    if(interval_time <= APR_TIME_C(0))
      return APR_FROM_ZK_ERROR(ZENOINTERVAL);
  } else if(interval_time > APR_TIME_C(0)) {
    return APR_FROM_ZK_ERROR(ZENOCALLBACK);
  } else
    interval_time = APR_TIME_C(-1);

  for(c = 0; ZEKE_STATUS_IS_OKAY(st); c++) {
    if(pool == NULL) {
      st = apr_pool_create(&pool,loop_pool);
      if(st != APR_SUCCESS)
        break;
      zeke_pool_tag(pool,apr_psprintf(pool,"zeke_app_run_loop subpool, loop %lu",c));
    }
    if(conn == NULL) {
      st = zeke_connection_create(&conn,zkhosts,NULL,pool);
      if(!ZEKE_STATUS_IS_OKAY(st))
        break;
      if(startup) {
        if(flags & ZEKE_RUNLOOP_DELAY_STARTUP_CALLBACK) {
          apr_pool_t *p = zeke_connection_pool_get(conn);
          zeke_delayed_startup_t *start = apr_palloc(p,sizeof(zeke_delayed_startup_t));
          start->callback = startup;
          start->registered_name = LIBZEKE_RUNLOOP_DELAYED_STARTUP_KEY;
          start->data = app_data;
          zeke_connection_named_watcher_add(conn,start->registered_name,delayed_startup);
          if(!ZEKE_STATUS_IS_OKAY(st))
            break;
          assert(apr_pool_userdata_setn(start,LIBZEKE_RUNLOOP_DELAYED_STARTUP_KEY,NULL,p) == APR_SUCCESS);
        } else
          startup(conn,app_data);
      }
      loop_timer = update_timers(&start_time,&stop_time,&elapsed,run_timer,interval_time);
    }
    st = zeke_io_loop(conn,loop_timer > APR_TIME_C(-1) ? &loop_timer : NULL,NULL);
    LOG("loop timer is: %ldms",(unsigned long)loop_timer/1000);
    update_timers(&start_time,&stop_time,&elapsed,run_timer,interval_time);
    LOG("  actual elapsed: %ldms",(unsigned long)elapsed/1000);
    /* zeke_io_loop() fills in loop_timer for us with the total_elapsed time */
    elapsed = loop_timer;
    if(!ZEKE_STATUS_IS_OKAY(st)) {
      if(APR_STATUS_IS_TIMEUP(st)) {
        assert(elapsed >= APR_TIME_C(0));
        if(run_timer > APR_TIME_C(-1) && elapsed >= run_timer) {
          if(timeout)
            timeout(conn,app_data);
          else {
            zeke_connection_close(conn);
            break;
          }
        }
        if(interval && elapsed >= interval_time) {
          interval(conn,app_data);
          start_time = stop_time = APR_TIME_C(0);
          /* make sure the max time is absolute even though interval time
          is resetting */
          if(run_timer > APR_TIME_C(0))
            run_timer -= (run_timer > elapsed ? run_timer : elapsed);
          loop_timer = update_timers(&start_time,&stop_time,&elapsed,run_timer,interval_time);
          st = ZEOK;
          continue;
        }
        /* if the user didn't perform a full close on the connection, restart */
        if(zeke_connection_is_open(conn)) {
          st = ZEOK;
          start_time = stop_time = apr_time_now();
          if(max_run_time > APR_TIME_C(-1)) {
            run_timer = max_run_time;
            if(elapsed < interval_time) {
              elapsed = interval_time - elapsed;
              start_time -= elapsed;
              run_timer += elapsed;
            }
          }
          loop_timer = update_timers(&start_time,&stop_time,&elapsed,run_timer,interval_time);
          continue;
        }
      }
    } else if(!zeke_connection_is_active(conn)) break;
  }

  if(conn != NULL && shutdown)
    shutdown(conn,app_data);

  if(pool != NULL)
    apr_pool_destroy(pool);

  if(loop_pool != root_pool)
    apr_pool_clear(loop_pool);

  return st;
}

ZEKE_API(const char*)zeke_get_fqdn(apr_pool_t *pool)
{
#ifndef MAXHOSTNAMELEN
#define MAXHOSTNAMELEN 1024
#endif
  char h[MAXHOSTNAMELEN+1];
  char *fqdn = NULL;
  apr_sockaddr_t *sockaddr;
  char *hostname;
  apr_status_t st;

  if((st = apr_gethostname(h,sizeof(h)-1,pool)) != APR_SUCCESS) {
    zeke_apr_error("apr_gethostname",st);
  } else {
    h[sizeof(h)-1] = '\0';
    if((st = apr_sockaddr_info_get(&sockaddr,h,APR_UNSPEC,0,0,pool)) == APR_SUCCESS) {
      if((apr_getnameinfo(&hostname,sockaddr,0) == APR_SUCCESS) &&
         (strchr(hostname,'.') != NULL)) {
        fqdn = apr_pstrdup(pool,hostname);
      } else if(strchr(h,'.') != NULL) {
        fqdn = apr_pstrdup(pool,h);
      } else {
        apr_sockaddr_ip_get(&hostname,sockaddr);
        fqdn = apr_pstrdup(pool,hostname);
      }
    } else
      zeke_apr_error("apr_sockaddr_info_get",st);
  }

  if(!fqdn)
    fqdn = apr_pstrdup(pool,"127.0.0.1");
  return fqdn;
}

struct zeke_sort_context {
  apr_pool_t *pool;
  zeke_regex_t *re;
  zeke_sort_compare_fn compare;
  zeke_context_t *user_context;
};

static int compr_numeric(zeke_context_t *ctx, const char *s1, const char *s2)
{
  apr_int64_t n1,n2;
  apr_status_t st;

  n1 = apr_strtoi64(s1,NULL,10);
  st = apr_get_os_error();
  if(st != APR_SUCCESS)
    zeke_fatal(s1,st);
  n2 = apr_strtoi64(s2,NULL,10);
  st = apr_get_os_error();
  if(st != APR_SUCCESS)
    zeke_fatal(s2,st);


  if(n1 < n2)
    return -1;
  if(n1 > n2)
    return 1;
  return 0;
}

static int compr_re(int *result,const char *key1,const char *key2,
                    struct zeke_regex_t *re, apr_pool_t *pool)
{
  int res = 0;
  apr_size_t groups1 = 0;
  apr_size_t groups2 = 0;
  zeke_regex_match_t *m1 = zeke_regex_exec(re,key1,&groups1);
  zeke_regex_match_t *m2 = zeke_regex_exec(re,key2,&groups2);
  zeke_sort_compare_fn compare = compr_numeric;

  if(m1) {
    ZEKE_ASSERTV(groups1 > 0,"%" APR_SIZE_T_FMT ": %s",groups1,zeke_regex_match_group(m1,0,pool));
    if(!m2)
      res = -1;
    else
      key1 = zeke_regex_match_group(m1,1,pool);
  } else compare = NULL;
  if(res == 0 && m2) {
    ZEKE_ASSERT(groups2 > 0,zeke_regex_match_group(m2,0,pool));
    if(!m1)
      res = 1;
    else
      key2 = zeke_regex_match_group(m2,1,pool);
  } else compare = NULL;

  if(m1)
    zeke_regex_match_destroy(m1);
  if(m2)
    zeke_regex_match_destroy(m2);

#if 0
  LOG("table_sort res=%d, key1=%s, key2=%s",res,key1,key2);
#endif

  if(res == 0 && key1 && key2) {
    if(compare)
      *result = compare(pool,key1,key2);
    else
      *result = apr_strnatcmp(key1,key2);
    return 1;
  }
  else if(res != 0) {
    *result = res;
    return 1;
  }

  return 0;
}
static QSORT_COMPARE_DECL(compr_index_ents, ctx, const char **n1,
                                                 const char **n2)
{
  if(ctx && ctx->compare)
    return ctx->compare(ctx->user_context,*n1,*n2);
  else if(ctx && ctx->re) {
    int res = 0;
    if(compr_re(&res,*n1,*n2,ctx->re,ctx->pool))
      return res;
  }

  return apr_strnatcmp(*n1,*n2);
}

static QSORT_COMPARE_DECL(compr_table_ents, ctx,
                          const apr_table_entry_t *n1,
                          const apr_table_entry_t *n2)
{
  if(n1->key == NULL || n2->key == NULL) {
    if(n1->key == n2->key) return 0;
    return n1->key ? 1 : -1;
  }

  if(ctx && ctx->compare)
    return ctx->compare(ctx->user_context,n1->key,n2->key);
  else if(ctx && ctx->re) {
    int res = 0;
    if(compr_re(&res,n1->key,n2->key,ctx->re,ctx->pool))
      return res;
  }

  return apr_strnatcmp(n1->key,n2->key);
}

ZEKE_API(zk_status_t) zeke_array_sort(apr_array_header_t *a,
                                      zeke_sort_compare_fn compare,
                                      zeke_regex_t *re,
                                      apr_pool_t *pool,
                                      zeke_context_t *user_context)
{
  struct zeke_sort_context ctx;

  ctx.pool = NULL;
  ctx.re = re;
  ctx.compare = compare;
  ctx.user_context = user_context;

  if(!ctx.compare && !ctx.re) {
    zk_status_t st;
    assert(apr_pool_create(&ctx.pool,pool) == APR_SUCCESS);
    st = zeke_regex_create_ex(&ctx.re,ZEKE_SEQUENCE_REGEX,
                              ZEKE_RE_DOLLAR_ENDONLY|ZEKE_RE_STUDY,ctx.pool);
    assert(st == ZEOK);
    assert(ctx.re != NULL);
  }

  QSORT_R(a->elts,a->nelts,a->elt_size,&ctx,compr_index_ents);
  if(ctx.pool)
    apr_pool_destroy(ctx.pool);
  return APR_SUCCESS;
}

ZEKE_API(apr_table_t*) zeke_table_sort(const apr_table_t *t,
                                  zeke_sort_compare_fn compare,
                                  apr_pool_t *pool,
                                  zeke_context_t *user_context)
{
  apr_table_t *new_table;
  struct zeke_sort_context ctx;
  const apr_array_header_t *h1;
  apr_table_entry_t *te;
  int i;

  ctx.pool = NULL;
  ctx.re = NULL;
  ctx.compare = compare;
  ctx.user_context = user_context;

  if(apr_is_empty_table(t)) {
    if(pool)
      return apr_table_copy(pool,t);
    return NULL;
  }

  h1 = apr_array_copy(pool,apr_table_elts(t));
  if(apr_is_empty_array(h1) || h1->nelts < 1)
    return apr_table_make(pool,0);

  if(!ctx.compare) {
    zk_status_t st;
    assert(apr_pool_create(&ctx.pool,pool) == APR_SUCCESS);
    st = zeke_regex_create_ex(&ctx.re,ZEKE_SEQUENCE_REGEX,
                              ZEKE_RE_DOLLAR_ENDONLY|ZEKE_RE_STUDY,ctx.pool);
    assert(st == ZEOK);
    assert(ctx.re != NULL);
  }

  QSORT_R(h1->elts,h1->nelts,h1->elt_size,&ctx,compr_table_ents);

  new_table = apr_table_make(pool,h1->nelts);
  te = (apr_table_entry_t*)h1->elts;
  for(i = 0; i < h1->nelts; i++)
    if(te[i].key) apr_table_setn(new_table,te[i].key,te[i].val);

  if(ctx.pool)
    apr_pool_destroy(ctx.pool);

  return new_table;
}

ZEKE_API(apr_array_header_t*) zeke_table_index(const apr_table_t *t, int sort, apr_pool_t *pool)
{
  return zeke_table_index_ex(t,sort,1,NULL,pool,NULL);
}

ZEKE_API(apr_array_header_t*) zeke_table_index_ex(const apr_table_t *t,
                                                  int sort, int shallow,
                                                  zeke_sort_compare_fn compare,
                                                  apr_pool_t *pool,
                                                  zeke_context_t *user_context)
{
  struct zeke_sort_context ctx;
  const apr_array_header_t *h1;
  apr_array_header_t *index;
  apr_table_entry_t *te;
  int i;

  ctx.pool = NULL;
  ctx.re = NULL;
  ctx.compare = compare;
  ctx.user_context = user_context;
  if(apr_is_empty_table(t))
    return apr_array_make(pool,1,sizeof(const char*));

  h1 = apr_table_elts(t);
  index = apr_array_make(pool,h1->nelts,sizeof(const char*));
  assert(index != NULL);

  te = (apr_table_entry_t*)h1->elts;
  for(i = 0; i < h1->nelts; i++)
    if(te[i].key) {
      APR_ARRAY_PUSH(index,const char*) = shallow ? te[i].key : apr_pstrdup(pool,te[i].key);
    }

  if(sort) {
    if(!ctx.compare) {
      zk_status_t st;
      assert(apr_pool_create(&ctx.pool,pool) == APR_SUCCESS);
      st = zeke_regex_create_ex(&ctx.re,ZEKE_SEQUENCE_REGEX,
                              ZEKE_RE_DOLLAR_ENDONLY|ZEKE_RE_STUDY,ctx.pool);
      assert(st == ZEOK);
      assert(ctx.re != NULL);
    }
    QSORT_R(index->elts,index->nelts,index->elt_size,&ctx,compr_index_ents);
  }

  if(ctx.pool != NULL)
    apr_pool_destroy(ctx.pool);
  return index;
}

struct fmtflex {
  unsigned magic;
#define FMTFLEX_MAGIC 0x984412ab
  apr_pool_t *p;
  apr_size_t sz;
  char *s; /* points to beginning of aligned buffer before this struct */
  apr_array_header_t *addl; /* optional additional buffers in order */
};

static int fmtflex_flush(apr_vformatter_buff_t *vb)
{
  struct fmtflex *flex;
  assert(vb != NULL); assert(vb->endpos != NULL);
  assert(vb->curpos >= vb->endpos);

  flex = (struct fmtflex*)((char*)vb+sizeof(*vb));
  assert(flex->magic == FMTFLEX_MAGIC);
  assert(flex->p != NULL);
  assert(flex->sz == APR_ALIGN_DEFAULT(256));
  if(flex->addl == NULL) {
    char *newbuf;
    flex->addl = apr_array_make(flex->p,5,flex->sz);
    assert(flex->addl != NULL);
    newbuf = apr_array_push(flex->addl);
    memcpy(newbuf,flex->s,flex->sz);
    flex->s = newbuf;
    vb->curpos = newbuf;
  }
  vb->endpos = (char*)apr_array_push(flex->addl) + flex->addl->elt_size;
  return 0;
}

ZEKE_API(const char*) zeke_format_log_message(const char *fmt, ...)
{
  apr_pool_t *p;
  char *buf;
  va_list ap;
  struct fmtflex *flex;
  apr_vformatter_buff_t *vfmtbuf;

  p = zk_error_pool_BEGIN();
  assert(p != NULL);
  buf = apr_palloc(p,APR_ALIGN_DEFAULT(sizeof *vfmtbuf)+
                     APR_ALIGN_DEFAULT(sizeof *flex)+
                     APR_ALIGN_DEFAULT(256)+
                     APR_ALIGN_DEFAULT(16));
  assert(buf != NULL);
  vfmtbuf = (struct apr_vformatter_buff_t*)buf;
  buf += APR_ALIGN_DEFAULT(sizeof *vfmtbuf);
  flex = (struct fmtflex*)buf;
  buf += APR_ALIGN_DEFAULT(sizeof *flex);
  flex->magic = FMTFLEX_MAGIC;
  flex->p = p;
  flex->sz = APR_ALIGN_DEFAULT(256);
  flex->s = buf;
  flex->addl = NULL;
  vfmtbuf->curpos = flex->s;
  vfmtbuf->endpos = flex->s + flex->sz;
  va_start(ap,fmt);
  if(apr_vformatter(fmtflex_flush,vfmtbuf,fmt,ap) > 0) {
    buf = flex->s;
    if(vfmtbuf->curpos >= vfmtbuf->endpos)
      *(vfmtbuf->endpos-1) = '\0';
    else
      *(vfmtbuf->curpos) = '\0';
  } else buf = "apr_vformatter buffer overrun error";
  va_end(ap);
  zk_error_pool_END(p);
  return buf;
}

ZEKE_PRIVATE(void) zeke_pool_vtag(apr_pool_t *p, const char *fmt, va_list ap)
{
  char buf[4096] = "";

  if(p && fmt) {
    apr_vsnprintf(buf,sizeof(buf),fmt,ap);
    if(buf[0])
      zeke_pool_tag(p,buf);
  }
}

ZEKE_API(void) zeke_pool_tag_ex(apr_pool_t *p, const char *fmt, ...)
{
  va_list ap;

  va_start(ap,fmt);
  zeke_pool_vtag(p,fmt,ap);
  va_end(ap);
}

ZEKE_API(void) zeke_pool_tag(apr_pool_t *p, const char *tag)
{
  if(p) {
    if(tag) {
      tag = strdup(tag);
      apr_pool_tag(p,tag);
      assert(apr_pool_userdata_setn(tag,"libzeke:pool:tag",NULL,p) == APR_SUCCESS);
    } else {
      assert(apr_pool_userdata_setn(NULL,"libzeke:pool:tag",NULL,p) == APR_SUCCESS);
    }
  }
}

ZEKE_API(const char*) zeke_pool_tag_get(apr_pool_t*p)
{
  const char *tag = NULL;
  if(p)
    apr_pool_userdata_get((void**)&tag,"libzeke:pool:tag",p);
  return tag;
}
