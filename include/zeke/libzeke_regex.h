#ifndef _LIBZEKE_REGEX_H
#define _LIBZEKE_REGEX_H

#include <zeke/libzeke.h>
#include <zeke/libzeke_util.h>

#ifdef __cplusplus
extern "C"
{
#endif

/* This is the libzeke public regular expression API, currently based on pcre */
typedef struct zeke_regex_match_t zeke_regex_match_t;

ZEKE_POOL_EXPORT_ACCESSOR(regex);
ZEKE_POOL_EXPORT_ACCESSOR(regex_match);

#define ZKRE64(bit) (APR_UINT64_C(1) << bit)

/* Flags for passing to zeke_regex_create */
enum {
  ZEKE_RE_ANCHORED = ZKRE64(0),
#define ZEKE_RE_ANCHORED ZEKE_RE_ANCHORED
  ZEKE_RE_BSR_ANYCRLF = ZKRE64(1),
#define ZEKE_RE_BSR_ANYCRLF ZEKE_RE_BSR_ANYCRLF
  ZEKE_RE_BSR_UNICODE = ZKRE64(2),
#define ZEKE_RE_BSR_UNICODE ZEKE_RE_BSR_UNICODE
  ZEKE_RE_CASELESS = ZKRE64(3),
#define ZEKE_RE_CASELESS ZEKE_RE_CASELESS
  ZEKE_RE_DOLLAR_ENDONLY = ZKRE64(4),
#define ZEKE_RE_DOLLAR_ENDONLY ZEKE_RE_DOLLAR_ENDONLY
  ZEKE_RE_DOTALL = ZKRE64(5),
#define ZEKE_RE_DOTALL ZEKE_RE_DOTALL
  ZEKE_RE_DUPNAMES = ZKRE64(6),
#define ZEKE_RE_DUPNAMES ZEKE_RE_DUPNAMES
  ZEKE_RE_EXTENDED = ZKRE64(7),
#define ZEKE_RE_EXTENDED ZEKE_RE_EXTENDED
  ZEKE_RE_FIRSTLINE = ZKRE64(8),
#define ZEKE_RE_FIRSTLINE ZEKE_RE_FIRSTLINE
  ZEKE_RE_JS_COMPAT = ZKRE64(9),
#define ZEKE_RE_JS_COMPAT ZEKE_RE_JS_COMPAT
  ZEKE_RE_MULTILINE = ZKRE64(10),
#define ZEKE_RE_MULTILINE ZEKE_RE_MULTILINE
  ZEKE_RE_NEWLINE_CR = ZKRE64(11),
#define ZEKE_RE_NEWLINE_CR ZEKE_RE_NEWLINE_CR
  ZEKE_RE_NEWLINE_LF = ZKRE64(12),
#define ZEKE_RE_NEWLINE_LF ZEKE_RE_NEWLINE_LF
  ZEKE_RE_NEWLINE_CRLF = ZKRE64(13),
#define ZEKE_RE_NEWLINE_CRLF ZEKE_RE_NEWLINE_CRLF
  ZEKE_RE_NEWLINE_ANYCRLF = ZKRE64(14),
#define ZEKE_RE_NEWLINE_ANYCRLF ZEKE_RE_NEWLINE_ANYCRLF
  ZEKE_RE_NEWLINE_ANY = ZKRE64(15),
#define ZEKE_RE_NEWLINE_ANY ZEKE_RE_NEWLINE_ANY
  ZEKE_RE_NO_AUTO_CAPTURE = ZKRE64(16),
#define ZEKE_RE_NO_AUTO_CAPTURE ZEKE_RE_NO_AUTO_CAPTURE
  ZEKE_RE_UNGREEDY = ZKRE64(17),
#define ZEKE_RE_UNGREEDY ZEKE_RE_UNGREEDY
  ZEKE_RE_UTF8 = ZKRE64(18),
#define ZEKE_RE_UTF8 ZEKE_RE_UTF8
  ZEKE_RE_NO_UTF8_CHECK = ZKRE64(19),
#define ZEKE_RE_NO_UTF8_CHECK ZEKE_RE_NO_UTF8_CHECK
  ZEKE_RE_NOTBOL = ZKRE64(20),
#define ZEKE_RE_NOTBOL ZEKE_RE_NOTBOL
  ZEKE_RE_NOTEOL = ZKRE64(21),
#define ZEKE_RE_NOTEOL ZEKE_RE_NOTEOL
  ZEKE_RE_NOTEMPTY = ZKRE64(22),
#define ZEKE_RE_NOTEMPTY ZEKE_RE_NOTEMPTY

  /* Customized additions: */
  /* causes pattern to be studied upon compilation */
  ZEKE_RE_STUDY = ZKRE64(33),
#define ZEKE_RE_STUDY ZEKE_RE_STUDY
  /* Causes pattern to only be scanned once, no backtracking */
  ZEKE_RE_ONCE = ZKRE64(34),
#define ZEKE_RE_ONCE ZEKE_RE_ONCE
  /* Always use a subpool */
  ZEKE_RE_SUBPOOL = ZKRE64(35),
#define ZEKE_RE_SUBPOOL ZEKE_RE_SUBPOOL
  /* Don't copy the pattern */
  ZEKE_RE_NO_PATTERN_COPY = ZKRE64(36),
#define ZEKE_RE_NO_PATTERN_COPY ZEKE_RE_NO_PATTERN_COPY
  /* This just removes caseless */
  ZEKE_RE_CASE_SENSITIVE = ZKRE64(37),
#define ZEKE_RE_CASE_SENSITIVE ZEKE_RE_CASE_SENSITIVE
};

#define ZEKE_RE_CASE_INSENSITIVE ZEKE_RE_CASELESS

ZEKE_EXPORT zk_status_t zeke_regex_create_ex(zeke_regex_t**,const char *pattern,apr_uint64_t options,
                                             apr_pool_t*);
ZEKE_EXPORT zk_status_t zeke_regex_create(zeke_regex_t**,const char *patter,apr_uint64_t options);
ZEKE_EXPORT const char *zeke_regex_error_get(const zeke_regex_t*, const char **where, apr_off_t *offset);
ZEKE_EXPORT const char *zeke_regex_error_fmt_ex(const zeke_regex_t*, zk_status_t, apr_pool_t*,
                                                const char *fmt, ...)
                       ZEKE_PRINTF(4,5);
ZEKE_EXPORT const char *zeke_regex_error_fmt(const zeke_regex_t*, apr_pool_t*,
                                             const char *fmt, ...)
                       ZEKE_PRINTF(3,4);

ZEKE_EXPORT int zeke_regex_is_error(const zeke_regex_t*);
ZEKE_EXPORT const char *zeke_regex_errstr_ex(const zeke_regex_t*, zk_status_t, apr_pool_t*);
ZEKE_EXPORT const char *zeke_regex_errstr(const zeke_regex_t*, apr_pool_t*);

ZEKE_EXPORT zk_status_t zeke_regex_status(const zeke_regex_t*);
ZEKE_EXPORT zk_status_t zeke_regex_destroy(const zeke_regex_t*);
ZEKE_EXPORT zeke_regex_match_t *zeke_regex_exec_ex(const zeke_regex_t*,
                                                   const char *subject,
                                                   apr_size_t *groups,
                                                   apr_ssize_t max_groups,
                                                   apr_pool_t*);
ZEKE_EXPORT zeke_regex_match_t *zeke_regex_exec(const zeke_regex_t*,const char *subject,apr_size_t *groups);
ZEKE_EXPORT apr_size_t zeke_regex_match_groups_count(const zeke_regex_match_t*);
ZEKE_EXPORT zk_status_t zeke_regex_match_destroy(const zeke_regex_match_t*);
ZEKE_EXPORT int zeke_regex_match_is_error(const zeke_regex_match_t*);
ZEKE_EXPORT const char *zeke_regex_match_errstr_ex(const zeke_regex_match_t*, zk_status_t, apr_pool_t*);
ZEKE_EXPORT const char *zeke_regex_match_errstr(const zeke_regex_match_t*, apr_pool_t*);
ZEKE_EXPORT const char *zeke_regex_match_error_fmt_ex(const zeke_regex_match_t*, zk_status_t, apr_pool_t*,
                                                      const char *fmt, ...)
                       ZEKE_PRINTF(4,5);
ZEKE_EXPORT const char *zeke_regex_match_error_fmt(const zeke_regex_match_t*, apr_pool_t*,
                                                      const char *fmt, ...)
                       ZEKE_PRINTF(3,4);

ZEKE_EXPORT zk_status_t zeke_regex_match_group_get(const zeke_regex_match_t*,
                                                   apr_uint16_t group,
                                                   apr_off_t *start,
                                                   apr_off_t *end,
                                                   void **data,
                                                   apr_size_t *datalen,
                                                   apr_pool_t *pool);
ZEKE_EXPORT const char *zeke_regex_match_group(const zeke_regex_match_t*, apr_uint16_t group, apr_pool_t*);
ZEKE_EXPORT apr_array_header_t *zeke_regex_match_groups_get(const zeke_regex_match_t*, apr_pool_t*);
ZEKE_EXPORT const char *zeke_regex_match_named_group(const zeke_regex_match_t*,const char *name);
ZEKE_EXPORT int zeke_regex_match_group_exists(const zeke_regex_match_t*, apr_uint32_t group);
ZEKE_EXPORT zk_status_t zeke_regex_match_group_index(const zeke_regex_match_t*,
                                                     apr_uint32_t group,
                                                     apr_off_t *start,
                                                     apr_ssize_t *len,
                                                     apr_off_t *next);

#ifdef __cplusplus
}
#endif

#endif /* _LIBZEKE_REGEX_H */
