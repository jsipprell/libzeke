#ifndef _ZEKE_TOOLS_H
#define _ZEKE_TOOLS_H

#include "libzeke.h"
#include <apr_env.h>
#include "compat.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#ifdef ZEKE_HIDDEN
#define ZKTOOL_PRIVATE(type) ZEKE_HIDDEN type
#define ZKTOOL_DECL(type) ZEKE_HIDDEN extern type
#if defined(__GNUC__) && __GNUC__ >= 4
# define ZKTOOL_DECL_VA(type) ZEKE_HIDDEN extern type \
                             __attribute__((sentinel))
# else
# define ZKTOOL_DECL_VA ZKTOOL_DECL
#endif
#define ZKTOOL_DECL_PFMT(type,a1,a2) ZEKE_HIDDEN extern type \
                             __attribute__((format (printf,a1,a2)))
#else
#define ZKTOOL_PRIVATE(type) type
#define ZKTOOL_DECL(type) extern type
#if defined(__GNUC__) && __GNUC__ >= 4
# define ZKTOOL_DECL_VA(type) extern type __attribute__((sentinel))
# else
# define ZKTOOL_DECL_VA ZKTOOL_DECL
#endif
#define ZKTOOL_DECL_PFMT(type,a1,a2) extern type \
                            __attribute__((format (printf,a1,a2)))
#endif

/* Timers */
typedef struct {
  apr_time_t start,stop;
} zktool_wait_t;
typedef apr_interval_time_t zktool_timer_t;

#define ZKTOOL_IS_TIMER_SET(timeouts,i) ((timeouts)[i] != APR_TIME_C(0))
#define ZKTOOL_CURRENT_TIME_ELAPSED(waits,i) (((waits)[i].stop = apr_time_now()) - (waits)[i].start)
#define ZKTOOL_STATIC_TIME_ELAPSED(waits,i) ((waits)[i].stop - (waits)[i].start)
#define ZKTOOL_CURRENT_TIME_ELAPSED_SECS(waits,i) apr_time_sec(ZKTOOL_CURRENT_TIME_ELAPSED(waits,i))
#define ZKTOOL_IS_TIMER_EXPIRED(waits,timeouts,i) \
        (ZKTOOL_IS_TIMER_SET(timeouts,i) && ZKTOOL_CURRENT_TIME_ELAPSED(waits,i) >= (timeouts[i]))

#define ZKTOOL_TIME(n) ((n) <= APR_TIME_C(0) ? ((n) = apr_time_now()) : (n))
#define ZKTOOL_TIME_ELAPSED(w,n) (((w).stop = ZKTOOL_TIME((n))) - (w).start)

/* tools for making/unmaking wait()/wait4() style 32 bit status codes */
#define ZKTOOL_MKSTATUS_APR(apr,sig,code) \
        ((((apr) & 0x000f) ? ((apr) & 0x000f) : APR_PROC_EXIT) | (((sig << 8) & 0xff00) | \
                                                                   (((code) << 8) & 0xff00)))
#define ZKTOOL_MKSTATUS_FULL(apr,sig,code,u) \
        (ZKTOOL_MKSTATUS_APR(apr,sig,code) | (((u) << 4) & 0x00f0))
#define ZKTOOL_MKSTATUS_SIGNAL(s) ZKTOOL_MKSTATUS(APR_PROC_SIGNAL,(s),0)
#define ZKTOOL_MKSTATUS_CORE(s) ZKTOOL_MKSTATUS(APR_PROC_SIGNAL_CORE,(s),0)
#define ZKTOOL_MKSTATUS(code) ZKTOOL_MKSTATUS(APR_PROC_EXIT,0,(code))
#define ZKTOOL_MKSTATUS_OC(code,why) (ZKTOOL_STATUS_IFSIGNALED(why) ? \
                          ZKTOOL_MKSTATUS_APR(why&(APR_PROC_SIGNAL|APR_PROC_SIGNAL_CORE),code,0) : \
                          ZKTOOL_MKSTATUS_APR(why&APR_PROC_EXIT,0,code))
#define ZKTOOL_STATUS_OR_EXITCODE(s,code) (((s) & 0x00ff) | (((code) << 8) & 0xff00))
#define ZKTOOL_STATUS_OR_SIGNAL(s,sig) (((s) & 0x00ff) | (((sig) << 8) & 0xff00))
#define ZKTOOL_STATUS_OR_USERDATA(s,u) (((s & 0xff0f) | ((u << 4) & 0x00f0)))
#define ZKTOOL_STATUS_IFSIGNALED(s) (APR_PROC_CHECK_SIGNALED((s))|APR_PROC_CHECK_CORE_DUMP((s)))
#define ZKTOOL_STATUS_IFCOREDUJP APR_PROC_CHECK_CORE_DUMP
#define ZKTOOL_STATUS_IFEXIT(s) APR_PROC_CHECK_EXIT
#define ZKTOOL_STATUS_EXITCODE_RAW(s) (((s) >> 8) & 0x00ff)
#define ZKTOOL_STATUS_SIGNAL_RAW(s) (((s) >> 8) & 0x00ff)
#define ZKTOOL_STATUS_USERDATA(s) (((s) >> 4) & 0x000f)
#define ZKTOOL_STATUS_WHY(s) ((s) & 0x000f)
#define ZKTOOL_STATUS_EXITCODE(s) (((s) & APR_PROC_EXIT) ? ZKTOOL_STATUS_EXITCODE_RAW(s) : 0)
#define ZKTOOL_STATUS_SIGNAL(s) (ZKTOOL_STATUS_IFSIGNALED(s) ? ZKTOOL_STATUS_SIGNAL_RAW(s) : 0)
#define ZKTOOL_STATUS_FIX(st) \
  (((st) & 0xfff0) | (((st) & 0xff00) ? (APR_PROC_SIGNAL|((st)&APR_PROC_SIGNAL_CORE)): \
                                                  (((st) & 0xff00) ? APR_PROC_EXIT)))

/* Common functions */
ZKTOOL_DECL(apr_status_t)
  zktool_parse_interval_time(apr_interval_time_t*, const char*, apr_pool_t*);

ZKTOOL_DECL(const char*)
  zktool_interval_format(apr_interval_time_t, apr_pool_t*);

ZKTOOL_DECL(void)
  zktool_display_options(const apr_getopt_option_t*,apr_file_t*,apr_pool_t*);

ZKTOOL_DECL(apr_status_t)
  zktool_set_logfile(const char*, apr_pool_t*);

ZKTOOL_DECL(apr_status_t)
  zktool_enable_shmlog(apr_pool_t*);

ZKTOOL_DECL(void)
  zktool_reset_timers(zktool_timer_t*,zktool_wait_t*,apr_size_t ntimers,
                      int index, apr_interval_time_t value);

ZKTOOL_DECL(apr_size_t)
  zktool_update_timers(zktool_timer_t*,zktool_wait_t*,apr_size_t ntimers,
                       apr_interval_time_t *shortest, int *updates, apr_size_t max_updates);

ZKTOOL_DECL(int)
  zktool_update_timeouts(zktool_timer_t*,zktool_wait_t*,apr_size_t ntimers,
                         int index, apr_interval_time_t *shortest);


/* table index util module */
ZKTOOL_DECL(apr_hash_t*)
  zktool_table_index_get_ex(apr_table_t*, const char *name);
#define zktool_table_index_get(t) zktool_table_index_get_ex((t),NULL)

ZKTOOL_DECL(unsigned int) zktool_table_index_update_ex(apr_table_t*, const char *name);
#define zktool_table_index_update(t) zktool_table_index_update_ex((t),NULL)

/* virtual transaction ids */
typedef struct zktool_vxid_pool {
  apr_uint32_t magic;
#define ZKTOOL_VXID_MAGIC 0x923ff1d2
  apr_pool_t *p;
  apr_uint32_t next,count;
#ifdef ZEKE_USE_THREADS
  apr_thread_mutex_t *mutex;
  apr_thread_cond_t *cond;
  unsigned waiters, busy;
#endif
  char b[];
} zktool_vxid_pool_t;

ZKTOOL_DECL(apr_uint32_t) zktool_vxid_get(zktool_vxid_pool_t*);
ZKTOOL_DECL(zktool_vxid_pool_t*) zktool_vxid_factory(apr_pool_t*);

#endif /* _ZEKE_TOOLS_H */

