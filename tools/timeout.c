#include "libzeke.h"
#include "libzeke_errno.h"
#include "libzeke_regex.h"
#include "libzeke_util.h"

#include "tools.h"

#ifdef HAVE_ALLOCA_H
# include <alloca.h>
#elif defined __GNUC__
# define alloca __builtin_alloca
#elif defined _AIX
# define alloca __alloca
#elif defined _MSC_VER
# include <malloc.h>
# define alloca _alloca
#else
# include <stddef.h>
# ifdef  __cplusplus
extern "C"
# endif
void *alloca (size_t);
#endif

#ifdef HAVE_MATH_H
#include <math.h>
#endif

#define INT_SUFFIX_PAT "s|sec|seconds|msec|usec|ms|u|us|m|min|minute|minutes|h|hour|hours"
#define FLOAT_SUFFIX_PAT "s|sec|seconds|m|min|minute|minutes|h|hour|hours"

#define TIMEOUT_PARSER_PAT "\\s*(?:(\\d+\\.\\d+)\\s*(" FLOAT_SUFFIX_PAT ")?)|" \
                             "(?:(\\d+)\\s*(" INT_SUFFIX_PAT ")?)\\s*$"
#define TIMEOUT_PARSER_PAT_FLAGS (ZEKE_RE_ANCHORED| \
                                  ZEKE_RE_CASELESS| \
                                  ZEKE_RE_DOLLAR_ENDONLY| \
                                  ZEKE_RE_UTF8| \
                                  ZEKE_RE_STUDY| \
                                  ZEKE_RE_NO_PATTERN_COPY)
/* Generic time interval parser */
ZEKE_MAY_ALIAS(static zeke_regex_t) *interval_re = NULL;

#ifndef apr_time_from_msec
#define apr_time_from_msec(ms) (((apr_time_t)(ms)) * (APR_USEC_PER_SEC / APR_TIME_C(1000)))
#endif

#ifdef HAVE_STRTOLD
typedef long double z_float_t;
#define zstrtof strtold
#define zrint llrintl
#elif defined(HAVE_STRTOD)
typedef double z_float_t;
#define zstrtof strtold
#define zrint llrint
#else
typedef float z_float_t;
#define zstrtof zstrtof
#define zrint llrintf
#endif /* HAVE_STRTOLD/HAVE_STRTOD */

static void timeout_parser_init(void)
{
  apr_pool_t *pool = NULL;
  if(interval_re == NULL) {
    zk_status_t st = zeke_regex_create_ex(&interval_re,
                                       TIMEOUT_PARSER_PAT,
                                       TIMEOUT_PARSER_PAT_FLAGS,
                                       NULL);
    if(interval_re)
      pool = zeke_regex_pool_get(interval_re);
    ZEKE_ASSERTV(ZEKE_STATUS_IS_OKAY(st),
        "timeout parser regex: %s",zeke_regex_errstr_ex(interval_re,st,pool));
    
    apr_pool_cleanup_register(pool,&interval_re,zk_indirect_wipe,apr_pool_cleanup_null);
  }
}

static apr_status_t z_cvt_float(apr_interval_time_t *t, const char *s, const char *units,
                                apr_pool_t *pool)
{
  apr_status_t st = APR_SUCCESS;
  z_float_t value = 0.0;
  long long cvt;
  char *end = NULL;

  value = zstrtof(s,&end);
  if((end && *end != '\0') || (st = apr_get_os_error()) != APR_SUCCESS)
    return (st == APR_SUCCESS ? APR_FROM_OS_ERROR(ERANGE) : st);

  if(units && *units) {
    switch(tolower(*units)) {
    case 'h':
      value *= 60.0;
    case 'm':
      value *= 60.0;
    case 's':
      break;
    default:
      abort();
      break;
    }
  }
  cvt = zrint(value * 1000.0);
  *t = apr_time_from_msec(cvt);

  return st;
}

static apr_status_t z_cvt_int(apr_interval_time_t *t, const char *s, const char *units,
                              apr_pool_t *pool)
{
  apr_status_t st = APR_SUCCESS;
  apr_int64_t value = APR_INT64_C(0);
  char *end = NULL;

  value = apr_strtoi64(s,&end,10);
  if((end && *end != '\0') || (st = apr_get_os_error()) != APR_SUCCESS)
    return (st = APR_SUCCESS ? APR_FROM_OS_ERROR(ERANGE) : st);

  if(units && *units) {
    if(strcasecmp(units,"ms") == 0 || strcasecmp(units,"msec") == 0 ||
                                      strcasecmp(units,"milliseconds") == 0) {
      *t = apr_time_from_msec(value);
    } else if(strncasecmp(units,"micro",5) == 0) {
      *t = (apr_interval_time_t)value;
    } else {
      switch(tolower(*units)) {
      case 'h':
        *t = apr_time_from_sec(value * 60 * 60);
        break;
      case 'm':
        *t = apr_time_from_sec(value * 60);
        break;
      case 'u':
        *t = (apr_interval_time_t)value;
        break;
      case 's':
        *t = apr_time_from_sec(value);
        break;
      default:
        abort();
        break;
      }
    }
  } else *t = apr_time_from_sec(value);

  return st;
}

ZKTOOL_PRIVATE(apr_status_t)
  zktool_parse_interval_time(apr_interval_time_t *t, const char *arg, apr_pool_t *pool)
{
  apr_size_t ngroups = 0;
  apr_status_t st = APR_BADARG;
  zeke_regex_match_t *match = NULL;
  const char *interval = NULL;
  const char *units = NULL;

  assert(t != NULL);
  timeout_parser_init();

  if((match = zeke_regex_exec_ex(interval_re,arg,&ngroups,5,pool)) == NULL) {
    if(zeke_regex_is_error(interval_re))
      zeke_eprintf("%s: %s\n",arg,zeke_regex_errstr(interval_re,pool));
    st = zeke_regex_status(interval_re);
    return st;
  }

  if(zeke_regex_match_group_exists(match,1))
  ZEKE_ASSERTV(ngroups >= 1 && ngroups <= 4,
               "regular expression on '%s' expected four capture groups, not %d.",
               arg,(int)ngroups);

  if(zeke_regex_match_group_exists(match,1)) {
    assert((interval = zeke_regex_match_group(match,1,pool)) != NULL);
    if(ngroups > 1)
      units = zeke_regex_match_group(match,2,pool);
    st = z_cvt_float(t,interval,units,pool);
  } else if(zeke_regex_match_group_exists(match,3)) {
    assert((interval = zeke_regex_match_group(match,3,pool)) != NULL);
    if(ngroups > 3)
      units = zeke_regex_match_group(match,4,pool);
    st = z_cvt_int(t,interval,units,pool);
  } else {
    zeke_abort("unexpected regex failure when parsing '%s'",arg);
  }

  zeke_regex_match_destroy(match);
  return st;
}

ZKTOOL_PRIVATE(const char*)
  zktool_interval_format(apr_interval_time_t t, apr_pool_t *pool)
{
  static char buf[256] = "";
  char *cp = buf;
  int dec = 0;
  const char *units = "us";

#define ZKBL(b,c) (sizeof(b)-((c)-(b)))
#define ZKFMT "%" APR_TIME_T_FMT

  if(t < APR_TIME_C(0))
    return pool ? apr_pstrdup(pool,"n/a") : "n/a";
  else if(t == APR_TIME_C(0))
    return pool ? apr_pstrdup(pool,"0") : "0";

  if(t >= APR_TIME_C(1000))
    units = "ms";
  if(t >= APR_USEC_PER_SEC)
    units = "s";

  if(t >= APR_USEC_PER_SEC) {
    cp += apr_snprintf(cp,ZKBL(buf,cp),ZKFMT,apr_time_sec(t));
    dec++;
    t = apr_time_usec(t);
  }
  if(t && t > APR_TIME_C(1000)) {
    if(dec)
      cp = apr_cpystrn(cp,".",ZKBL(buf,cp));
    dec++;
    cp += apr_snprintf(cp,ZKBL(buf,cp),ZKFMT,apr_time_msec(t));
    t = t % APR_TIME_C(1000);
  }
  if(dec < 2 && apr_time_usec(t) > APR_TIME_C(0)) {
    if(dec < 2) {
      cp = apr_cpystrn(cp,".",ZKBL(buf,cp));
      dec++;
    }
    cp += apr_snprintf(cp,ZKBL(buf,cp),ZKFMT,apr_time_usec(t));
  }

  if(dec > 1) {
    char *dp = cp-1;
    for(; dp > buf && *dp == '0' && *(dp-1) != '.'; dp--) {
      *dp = '\0';
      cp = dp;
    }
  }
  if(cp > buf && units)
    apr_cpystrn(cp,units,ZKBL(buf,cp));

#undef ZKBL
#undef ZKFMT
  return pool ? apr_pstrdup(pool,buf) : buf;
}

ZKTOOL_PRIVATE(void)
  zktool_reset_timers(zktool_timer_t *timers, zktool_wait_t *waits, apr_size_t ntimers,
                      int index, apr_interval_time_t value)
{
  int i;
  apr_time_t now = apr_time_now();

  for(i = 0; i < ntimers; i++) {
    if(index < 0 || i == index) {
      waits[i].start = now;
      waits[i].stop = APR_TIME_C(0);
      if(value > APR_TIME_C(0))
        timers[i] = value;
      if(index >= 0)
        break;
    }
  }
}

ZKTOOL_PRIVATE(apr_size_t)
  zktool_update_timers(zktool_timer_t *timers, zktool_wait_t *waits, apr_size_t ntimers,
                       apr_interval_time_t *shortest, int *updates, apr_size_t max_updates)
{
  int i;
  apr_size_t count = 0;
  apr_time_t now = APR_TIME_C(0);


  if(shortest)
    *shortest = APR_TIME_C(-1);

  for(i = 0; i < ntimers; i++) {
    if(ZKTOOL_IS_TIMER_SET(timers,i)) {
      if(waits[i].start == APR_TIME_C(0))
        waits[i].start = ZKTOOL_TIME(now);
      else {
        apr_interval_time_t elapsed = ZKTOOL_TIME_ELAPSED(waits[i],now);
        if(elapsed >= timers[i]) {
          if(updates && count < max_updates)
            *updates++ = i;
          count++;
#if 0
          if(shortest && (*shortest == APR_TIME_C(-1) || *shortest > timers[i]))
            *shortest = timers[i];
#endif
        } else if(shortest) {
          elapsed = timers[i] - elapsed;
          if(*shortest == APR_TIME_C(-1) || *shortest > elapsed)
            *shortest = elapsed;
        }
      }
    }
  }

  return count;
}

ZKTOOL_PRIVATE(int)
  zktool_update_timeouts(zktool_timer_t *timers, zktool_wait_t *waits, apr_size_t ntimers,
                         int index, apr_interval_time_t *shortest)
{
  apr_size_t count;
  int *expired = NULL;
 
  expired = alloca((ntimers+1) * sizeof(int));
  assert(expired != NULL);

  count = zktool_update_timers(timers,waits,ntimers,shortest,expired,ntimers+1);

  if(index > -1) {
    if(count > 0) {
      apr_size_t i;

      for(i = 0; i < count; i++)
        if(expired[i] == index)
          return 1;
    }
    return 0;
  }

  return (int)count;
}
