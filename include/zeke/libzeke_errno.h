#ifndef _LIBZEKE_ERRNO_H
#define _LIBZEKE_ERRNO_H

#include <zeke/libzeke.h>
#include <zeke/libzeke_util.h>

#include <zookeeper/zookeeper_log.h>

#ifdef __cplusplus
extern "C"
{
#endif

#ifdef NDEBUG
#define MIN_LOG_LEVEL ZOO_LOG_LEVEL_DEBUG
#define LOG(fmt, ...) 
#else
# if defined(DEBUGGING) && defined(DEBUG_LIBZEKE)
# define MIN_LOG_LEVEL ZOO_LOG_LEVEL_DEBUG
# elif defined(DEBUGGING)
# define MIN_LOG_LEVEL ZOO_LOG_LEVEL_INFO
# else
# define MIN_LOG_LEVEL ZOO_LOG_LEVEL_ERROR
# endif
#define LOG(fmt, ...)       do { \
                              if(logLevel > MIN_LOG_LEVEL) \
                                log_message(ZOO_LOG_LEVEL_DEBUG, __LINE__, __func__, \
                                zeke_format_log_message( (fmt), ## __VA_ARGS__ )); \
                            } while(0)
#endif 
#define ZKDEBUG(fmt, ...) do { \
                              if(logLevel >= ZOO_LOG_LEVEL_DEBUG) \
                                log_message(ZOO_LOG_LEVEL_DEBUG, __LINE__, __func__, \
                                zeke_format_log_message( (fmt), ## __VA_ARGS__ )); \
                            } while(0)
#define ERR(fmt, ...)       do { \
                              if(logLevel >= ZOO_LOG_LEVEL_ERROR) \
                                log_message(ZOO_LOG_LEVEL_ERROR, __LINE__, __func__, \
                                zeke_format_log_message( (fmt), ## __VA_ARGS__ )); \
                            } while(0)
#define WARN(fmt, ...)      do { \
                              if(logLevel >= ZOO_LOG_LEVEL_WARN) \
                                log_message(ZOO_LOG_LEVEL_WARN, __LINE__, __func__, \
                                zeke_format_log_message( (fmt), ## __VA_ARGS__ )); \
                           } while(0)
#define INFO(fmt, ...)    do { \
                            if(logLevel >= ZOO_LOG_LEVEL_INFO) \
                              log_message(ZOO_LOG_LEVEL_INFO, __LINE__, __func__, \
                              zeke_format_log_message( (fmt), ## __VA_ARGS__ )); \
                          } while(0)

/* Zookeeper errors actually start at 9 and work backwards, so start 500 ahead
 * of the allowed apr user-range so we can effectively map zookeeper errors
 * into apr error space.
 */

#define ZEKE_ZK_ERRSPACE_SIZE 500
#define ZEKE_ZK_EXT_ERRSPACE_SIZE 50

#define ZEKE_ZK_START_ERR (APR_OS_START_USERERR + ZEKE_ZK_ERRSPACE_SIZE)

#define ZEKE_FROM_ZK_ERROR(e) ((e) == 0 || ((e) == ZEOK+ZEKE_ZK_START_ERR) \
                                        ? APR_SUCCESS : (e) + ZEKE_ZK_START_ERR)
#define APR_FROM_ZK_ERROR(e) ZEKE_FROM_ZK_ERROR(e)

#define ZEKE_TO_ZK_ERROR(e) ((e) == 0 || ((e) == ZEOK+ZEKE_ZK_START_ERR) \
                                       ? APR_SUCCESS : (e) - ZEKE_ZK_START_ERR)
#define APR_TO_ZK_ERROR(e) ZEKE_TO_ZK_ERROR(e)

#define APR_STATUS_IS_ZK_ERROR(s)  ((s) <= ZEKE_ZK_START_ERR && (s) >= APR_OS_START_USERERR)
#define APR_STATUS_IS_ZK_EXT_ERROR(s) ((s) > ZEKE_ZK_START_ERR && (s) < ZEKE_ZK_START_ERR + ZEKE_ZK_EXT_ERRSPACE_SIZE)
#define APR_STATUS_IS_ZEKE_ERROR(s) ((s) < ZEKE_ZK_START_ERR + ZEKE_ZK_EXT_ERRSPACE_SIZE && (s) >= APR_OS_START_USERERR)

enum ZEKE_ERRORS {
  ZEOK = 0,
#define ZEOK ZEOK
  ZETOOMANYRETRIES = 10,
#define ZETOOMANYRETRIES ZETOOMANYRETRIES
  ZETOOMANYTIMEOUTS = 11,
#define ZETOOMANYTIMEOUTS ZETOOMANYTIMEOUTS
  ZENOCALLBACK = 12,
#define ZENOCALLBACK ZENOCALLBACK
  ZENOINTERVAL = 13,
#define ZENOINTERVAL ZENOINTERVAL
  ZENOSESSION = 14,
#define ZENOSESSION ZENOSESSION
  ZEREGEXERROR = 15
#define ZEREGEXERROR ZEREGEXERROR 
};

#define ZEKE_STATUS_CAN_RETRY(s) \
  (!APR_STATUS_IS_ZK_EXT_ERROR( (s) ) && \
   ( ZEKE_TO_ZK_ERROR( (s) ) == ZSESSIONEXPIRED || \
     ZEKE_TO_ZK_ERROR( (s) ) == ZNOTHING || \
     ZEKE_TO_ZK_ERROR( (s) ) == ZSESSIONMOVED || \
     ZEKE_TO_ZK_ERROR( (s) ) == ZOPERATIONTIMEOUT || \
     ZEKE_TO_ZK_ERROR( (s) ) == ZCONNECTIONLOSS ))

#define ZEKE_STATUS_IS_OKAY(s) (ZEKE_TO_ZK_ERROR(s) == ZEOK)
#define ZEKE_STATUS_IS_TOO_MANY_RETRIES(s) (APR_STATUS_IS_ZK_EXT_ERROR(s) && ZEKE_TO_ZK_ERROR(s) == ZETOOMANYRETRIES)
#define ZEKE_STATUS_IS_TOOMANYRETRIES(s) ZEKE_STATUS_IS_TOO_MANY_RETRIES(s)
#define ZEKE_STATUS_IS_TOO_MANY_TIMEOUTS(s) (APR_STATUS_IS_ZK_EXT_ERROR(s) && ZEKE_TO_ZK_ERROR(s) == ZETOOMANYTIMEOUTS)
#define ZEKE_STATUS_IS_TOOMANYTIMEOUTS(s) ZEKE_STATUS_IS_TOO_MANY_TIMEOUTS(s)
#define ZEKE_STATUS_IS_NO_CALLBACK(s) (APR_STATUS_IS_ZK_EXT_ERROR(s) && ZEKE_TO_ZK_ERROR(s) == ZENOCALLBACK)
#define ZEKE_STATUS_IS_NOCALLBACK(s) ZEKE_STATUS_IS_NO_CALLBACK(s)
#define ZEKE_STATUS_IS_NO_INTERVAL(s) (APR_STATUS_IS_ZK_EXT_ERROR(s) && ZEKE_TO_ZK_ERROR(s) == ZENOINTERVAL)
#define ZEKE_STATUS_IS_NOINTERVAL(s) ZEKE_STATUS_IS_NO_INTERVAL(s)
#define ZEKE_STATUS_IS_NO_SESSION(s) (APR_STATUS_IS_ZK_EXT_ERROR(s) && ZEKE_TO_ZK_ERROR(s) == ZENOSESSION)
#define ZEKE_STATUS_IS_NOSESSION(s) ZEKE_STATUS_IS_NO_SESSION(s)
#define ZEKE_STATUS_IS_CONNECTION_CLOSED(s) ZEKE_STATUS_IS_NO_SESSION(s)
#define ZEKE_STATUS_IS_REGEX_ERROR(s) (APR_STATUS_IS_ZK_EXT_ERROR(s) && ZEKE_TO_ZK_ERROR(s) == ZEREGEXERROR)
#define ZEKE_STATUS_IS_REGEXERROR(s) ZEKE_STATUS_IS_REGEX_ERROR

ZEKE_EXPORT void zeke_error_init(apr_pool_t*);
#define zeke_errstr(s) zeke_perrstr( (s), NULL )
ZEKE_EXPORT const char *zeke_perrstr(zk_status_t,apr_pool_t*);
#define zeke_pstrerr(p,s) zeke_perrstr( (s), (p) )

ZEKE_EXPORT void zeke_fatal(const char *msg,apr_status_t);
ZEKE_EXPORT void zeke_apr_error(const char *msg,apr_status_t);
ZEKE_EXPORT const char *zeke_zkstate_desc(int);
ZEKE_EXPORT const char *zeke_zkevent_dest(int);

ZEKE_EXPORT void zeke_abort(const char *fmt, ...) ZEKE_PRINTF(1,2);
ZEKE_EXPORT const char *zeke_abort_msg(const char *fmt, ...) ZEKE_PRINTF(1,2);
ZEKE_EXPORT void zeke_apr_eprintf(apr_status_t status, const char *fmt, ...) ZEKE_PRINTF(2,3);

#if defined(NDEBUG) && !defined(DEBUGGING)
#define ZEKE_ASSERT(cond,msg) cond
#define ZEKE_ASSERTV(cond,fmt,...) cond
#else /* !NDEBUG || DEBUGGING */
#define ZEKE_ASSERT(cond,msg) { if ( !(cond) ) { \
    zeke_abort("assertion failure: '%s' at " APR_POOL__FILE_LINE__,  (msg) ); \
    } }
#define ZEKE_ASSERTV(cond, fmt, ...) { if ( !(cond) ) { \
    zeke_abort("assertion failure: '%s' at " APR_POOL__FILE_LINE__, (zeke_abort_msg(fmt, __VA_ARGS__ )) ); \
    } }
#endif /* NDEBUG/DEBUGGING */

#ifdef __cplusplus
}
#endif

#endif /* _LIBZEKE_ERRNO_H */
