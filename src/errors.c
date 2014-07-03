/* Basic apr-wrapped error handling for zk */
#include "internal.h"

static apr_pool_t *error_pool = NULL;

static const char *ext_error_tab[] = {
  "no error",
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  "too many reconnect attempts",
  "too many timeouts",
  "an expected callback function pointer was passed as NULL",
  "an expected time interval was passed as NULL, -1, or 0",
  "zookeeper session has been closed",
  "regular expression error",
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL
};

#define ERROR_POOL_BEGIN _access_error_pool(1); {
#define ERROR_POOL_END } _access_error_pool(-1);

static inline apr_pool_t *_access_error_pool(int ref) {
  static int error_pool_refcount = 0;
  static unsigned error_pool_count = 0;

  if(error_pool_count > 10 && error_pool_refcount == 0 && ref > 0) {
    apr_pool_clear(error_pool);
    error_pool_count = 0;
  }

  error_pool_refcount += ref;
  error_pool_count++;

  assert(error_pool != NULL);
  return error_pool;
}

void zeke_apr_error(const char *msg,apr_status_t status)
{
  if(zstderr)
    apr_file_printf(zstderr,"%s: %s\n",msg,zeke_errstr(status));
  else
    ERR("%s: %s",msg,zeke_errstr(status));
}

ZEKE_PRIVATE(apr_pool_t*) zk_error_pool_BEGIN(void)
{
  return _access_error_pool(1);
}

ZEKE_PRIVATE(void) zk_error_pool_END(const apr_pool_t *p)
{
  assert(error_pool == (apr_pool_t*)p);
  _access_error_pool(-1);
}

ZEKE_API(void) zeke_apr_eprintf(apr_status_t status, const char *fmt, ...)
{
  va_list ap;
  apr_pool_t *pool = ERROR_POOL_BEGIN;

  va_start(ap,fmt);
  if(zstderr) {
    apr_file_puts(apr_pvsprintf(pool,fmt,ap),zstderr);
    apr_file_putc(':',zstderr);
    apr_file_printf(zstderr," %s\n",zeke_errstr(status));
    apr_file_flush(zstderr);
  } else {
    ERR("%s: %s",apr_pvsprintf(pool,fmt,ap),zeke_errstr(status));
  }

  va_end(ap);

  ERROR_POOL_END;
}

void zeke_fatal(const char *msg,apr_status_t status)
{
  zeke_apr_error(msg,status);
  exit(abs(status) & 0xff);
}

ZEKE_API(void) zeke_abort(const char *fmt, ...)
{
  va_list ap;
  apr_pool_t *pool = ERROR_POOL_BEGIN;

  va_start(ap,fmt);
  if(zstderr) {
    apr_file_puts(apr_pvsprintf(pool,fmt,ap),zstderr);
    apr_file_putc('\n',zstderr);
    apr_file_flush(zstderr);
  } else {
    ERR("%s",apr_pvsprintf(pool,fmt,ap));
  }
  va_end(ap);

  ERROR_POOL_END;
  abort();
}

ZEKE_API(const char *) zeke_abort_msg(const char *fmt, ...)
{
  va_list ap;
  const char *msg;
  apr_pool_t *pool = ERROR_POOL_BEGIN;

  va_start(ap,fmt);
  msg = apr_pvsprintf(pool,fmt,ap);
  va_end(ap);

  ERROR_POOL_END;
  return msg;
}

void zeke_error_init(apr_pool_t *parent)
{
  apr_status_t rc = APR_SUCCESS;

  if(parent) {
    if(error_pool) {
      apr_pool_destroy(error_pool);
      error_pool = NULL;
    }
  }

  if(!error_pool) {
    if((rc = apr_pool_create(&error_pool,parent)) != APR_SUCCESS)
      zeke_fatal("apr_pool_create",rc);
    zeke_pool_tag(error_pool,"ZEKE GLOBAL Error Pool");
  }
}

const char *zeke_perrstr(zk_status_t st,apr_pool_t *pool)
{
  const char *msg = NULL;

  if(pool == NULL) {
    if(error_pool == NULL)
      zeke_error_init(NULL);
  }

  pool = ERROR_POOL_BEGIN;


  if(APR_STATUS_IS_ZK_EXT_ERROR(st)) {
    const char *buf = NULL;
    if(ZEKE_TO_ZK_ERROR(st) < (sizeof(ext_error_tab)/sizeof(const char*)) &&
       ZEKE_TO_ZK_ERROR(st) >= 0)
      buf = ext_error_tab[ZEKE_TO_ZK_ERROR(st)];
    if(!buf)
      msg = apr_psprintf(pool,"unknown extended error %d",ZEKE_TO_ZK_ERROR(st));
    else
      msg = apr_pstrdup(pool,buf);
  } else if(APR_STATUS_IS_ZK_ERROR(st)) {
    return apr_pstrdup(pool,zerror(ZEKE_TO_ZK_ERROR(st)));
  } else {
    char buf[ZEKE_BUFSIZE];
    char *bp;

    if(st == APR_EBUSY)
      bp = "Busy subsystem";
    else
      bp = apr_strerror(st,buf,sizeof(buf));
    msg = apr_psprintf(pool,"(%d) %s",st,bp);
  }
  ERROR_POOL_END;
  return msg;
}

ZEKE_API(const char*) zeke_zkstate_desc(int state)
{
  if (state == 0)
    return "CLOSED_STATE";
  if (state == ZOO_CONNECTING_STATE)
    return "CONNECTING_STATE";
  if (state == ZOO_ASSOCIATING_STATE)
    return "ASSOCIATING_STATE";
  if (state == ZOO_CONNECTED_STATE)
    return "CONNECTED_STATE";
  if (state == ZOO_EXPIRED_SESSION_STATE)
    return "EXPIRED_SESSION_STATE";
  if (state == ZOO_AUTH_FAILED_STATE)
    return "AUTH_FAILED_STATE";

  return "INVALID_STATE";
}

ZEKE_API(const char*) zeke_zkevent_desc(int type)
{
  if (type == ZOO_CREATED_EVENT)
    return "CREATED_EVENT";
  if (type == ZOO_DELETED_EVENT)
    return "DELETED_EVENT";
  if (type == ZOO_CHANGED_EVENT)
    return "CHANGED_EVENT";
  if (type == ZOO_CHILD_EVENT)
    return "CHILD_EVENT";
  if (type == ZOO_SESSION_EVENT)
    return "SESSION_EVENT";
  if (type == ZOO_NOTWATCHING_EVENT)
    return "NOTWATCHING_EVENT";

  return "UNKNOWN_EVENT_TYPE";
}
