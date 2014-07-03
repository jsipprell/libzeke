#include "tools.h"
#include "libzeke_log.h"

#if defined(APR_HAS_THREADS) && defined(APR_FOPEN_XTHREAD)
#define ZOO_LOG_FLAGS APR_FOPEN_WRITE|APR_FOPEN_CREATE|APR_FOPEN_APPEND|\
                      APR_FOPEN_BUFFERED|APR_FOPEN_NOCLEANUP|APR_FOPEN_XTHREAD
#else
#define ZOO_LOG_FLAGS APR_FOPEN_WRITE|APR_FOPEN_CREATE|APR_FOPEN_APPEND|\
                      APR_FOPEN_BUFFERED|APR_FOPEN_NOCLEANUP
#endif

#ifndef ZKTOOL_OPT_LPAD
#define ZKTOOL_OPT_LPAD 7
#endif
#ifndef ZKTOOL_OPT_IDENT
#define ZKTOOL_OPT_IDENT 2
#endif
#ifndef ZKTOOL_OPT_RPAD
#define ZKTOOL_OPT_RPAD 10
#endif

#ifndef ZKTOOL_OPT_FMTSTR_L
#define ZKTOOL_OPT_FMTSTR_L "%-" APR_STRINGIFY(ZKTOOL_OPT_LPAD) "s"
#endif
#ifndef ZKTOOL_OPT_FMTSTR_R
#define ZKTOOL_OPT_FMTSTR_R "%-" APR_STRINGIFY(ZKTOOL_OPT_RPAD) "s"
#endif

#ifndef ZKTOOL_OPT_DESC
#define ZKTOOL_OPT_DESC "           %%s"
#endif

#ifndef ZKTOOL_OPT_INDENT_STR
#define ZKTOOL_OPT_INDENT_STR " "
#endif

static apr_pool_t *log_pool = NULL;
static apr_file_t *log_stream = NULL;
static apr_status_t close_zookeeper_logfile(void *data)
{
  if(log_pool != NULL) {
    log_pool = NULL;
    log_stream = NULL;
    zoo_set_log_stream(NULL);
    apr_file_close((apr_file_t*)data);
  }
  return APR_SUCCESS;
}

static apr_status_t close_zookeeper_shmlog(void *data)
{
  if(log_pool != NULL) {
    zoo_set_log_stream(NULL);
    log_pool = NULL;
    log_stream = NULL;
  }
  return APR_SUCCESS;
}

ZKTOOL_PRIVATE(apr_status_t)
  zktool_enable_shmlog(apr_pool_t *p)
{
  apr_status_t st = APR_SUCCESS;
  zeke_shmlog_header_t *shm;
  apr_pool_t *pool = NULL;
  FILE *fp = NULL;

  if(p == NULL)
    pool = zeke_root_subpool_create();
  else
    assert(apr_pool_create(&pool,p) == APR_SUCCESS);

  fp = zeke_shmlog_stream_open(&shm,pool);
  if(fp == NULL) {
    st = apr_get_os_error();
    apr_pool_destroy(pool);
    pool = NULL;
  } else {
    assert(shm != NULL);
    st = zeke_shmlog_lock(shm);
    if(st == APR_SUCCESS) {
      if(log_pool != NULL)
        apr_pool_destroy(log_pool);
      zoo_set_log_stream(fp);
      log_pool = pool;
      apr_pool_cleanup_register(pool,fp,close_zookeeper_shmlog,
                                        close_zookeeper_shmlog);
    }
  }
  if(st != APR_SUCCESS) {
    if(fp != NULL)
      fclose(fp);
    if(pool != NULL)
      apr_pool_destroy(pool);
  }
  return st;
}

ZKTOOL_PRIVATE(apr_status_t)
  zktool_set_logfile(const char *filename, apr_pool_t *p)
{
  apr_status_t st;
  apr_file_t *f = NULL;
  apr_pool_t *pool;
  FILE *fp;
  apr_os_file_t fd;

  if(filename == NULL) {
    if(log_pool != NULL)
      apr_pool_destroy(log_pool);
    else
      zoo_set_log_stream(NULL);
    return APR_SUCCESS;
  }

  if(p == NULL)
    pool = zeke_root_subpool_create();
  else
    assert(apr_pool_create(&pool,p) == APR_SUCCESS);

  st = apr_file_open(&f, filename, ZOO_LOG_FLAGS, APR_OS_DEFAULT, pool);
  if(st == APR_SUCCESS) {
    apr_file_inherit_unset(f);
    if((st = apr_os_file_get(&fd,f)) == APR_SUCCESS) {
      fp = fdopen(fd,"a");
      if(fp == NULL) {
        st = apr_get_os_error();
      } else {
        zoo_set_log_stream(fp);
        log_pool = pool;
        apr_pool_cleanup_register(pool,f,close_zookeeper_logfile,close_zookeeper_logfile);
      }
    }
  }

  if(st != APR_SUCCESS && f != NULL)
    apr_file_close(f);
  return st;
}

ZKTOOL_PRIVATE(const char *)
  zktool_format_opt(const apr_getopt_option_t *opt, const char *metaval, apr_pool_t *pool)
{
  const char *n = NULL;
  const char *optfmt = NULL;
  const char *optfmt_long_with_arg = ZKTOOL_OPT_INDENT_STR "-%c, %s\n" ZKTOOL_OPT_INDENT_STR ZKTOOL_OPT_DESC;
  const char *optfmt_long = ZKTOOL_OPT_INDENT_STR "-%c, %s\n" ZKTOOL_OPT_INDENT_STR ZKTOOL_OPT_DESC;
  const char *optfmt_no_short_with_arg = ZKTOOL_OPT_INDENT_STR ZKTOOL_OPT_FMTSTR_L "\n" ZKTOOL_OPT_INDENT_STR ZKTOOL_OPT_DESC;
  const char *optfmt_no_short = ZKTOOL_OPT_INDENT_STR ZKTOOL_OPT_FMTSTR_R " %%s";
  const char *optfmt_no_long = ZKTOOL_OPT_INDENT_STR "-%c " ZKTOOL_OPT_FMTSTR_L " %%s";
  const char *optfmt_no_long_with_arg = ZKTOOL_OPT_INDENT_STR "-%c " ZKTOOL_OPT_FMTSTR_L " %%s";

  if(opt->name && *opt->name) {
    if(opt->has_arg) {
      if(!metaval)
        metaval = "VALUE";
    }
    if(opt->has_arg && metaval && *metaval)
      n = apr_psprintf(pool,"--%s=%s",opt->name,metaval);
    else
      n = apr_psprintf(pool,"--%s",opt->name);
  } else if(apr_isalnum(opt->optch)) {
    if(opt->has_arg && !metaval)
      metaval = "VALUE";
    if(opt->has_arg && metaval && *metaval)
      n = metaval;
    else
      n = "";
  }
  if(opt->name && *opt->name) {
    if(opt->has_arg && !apr_isalnum(opt->optch))
      optfmt = apr_psprintf(pool,optfmt_no_short_with_arg,n);
    else if(!opt->has_arg && apr_isalnum(opt->optch))
      optfmt = apr_psprintf(pool,optfmt_long,(char)opt->optch,n);
    else if(!opt->has_arg && !apr_isalnum(opt->optch))
      optfmt = apr_psprintf(pool,optfmt_no_short,n);
    else
      optfmt = apr_psprintf(pool,optfmt_long_with_arg,(char)opt->optch,n);
  } else if(apr_isalnum(opt->optch)) {
    if(opt->has_arg)
      optfmt = apr_psprintf(pool,optfmt_no_long_with_arg,(char)opt->optch,n);
    else
      optfmt = apr_psprintf(pool,optfmt_no_long,(char)opt->optch,n);
  }
  assert(optfmt != NULL);
  return optfmt;
}

static inline size_t
get_max_line_len(const char *line)
{
  const char *cp;
  size_t maxl = 0, l = 0;

  for(cp = line; *cp != '\0'; cp++) {
    if(*cp == '\n' || *cp == '\r') {
      if (l > maxl) maxl = l;
      l = 0;
    } else if(!isspace(*cp))
      l++;
  }
  return (l > maxl ? l : maxl);
}

ZKTOOL_PRIVATE(void)
  zktool_display_options(const apr_getopt_option_t *options, apr_file_t *f, apr_pool_t *pool)
{
  int self_created_pool = 0;
  const apr_getopt_option_t *opt;
  const char *optfmt;

  if(pool == NULL) {
    pool = zeke_root_subpool_create();
    self_created_pool++;
  }

  if(f == NULL) {
    apr_status_t st;

    if((f = zstdout) == NULL && (f = zstderr) == NULL) {
      st = apr_file_open_stderr(&f,pool);
      if(st != APR_SUCCESS) {
        st = apr_file_open_stdout(&f,pool);
        if(st != APR_SUCCESS)
          return;
      }
      apr_file_inherit_unset(f);
    }
  }

  for(opt = options; opt->name != NULL || opt->optch != 0; opt++) {
    optfmt = zktool_format_opt(opt,NULL,pool);
    assert(optfmt != NULL);
#if 0
    optfmt = apr_pstrcat(pool,optfmt,apr_psprintf(pool,"%%%ds%%s\n",(int)(30-strlen(optfmt))),NULL);
#endif
    apr_file_printf(f,optfmt,opt->description);
    apr_file_putc('\n',f);
  }

  if(self_created_pool)
    apr_pool_destroy(pool);
}

