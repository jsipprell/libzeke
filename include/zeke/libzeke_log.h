#ifndef _LIBZEKE_LOG_H
#define _LIBZEKE_LOG_H

#include <zeke/libzeke.h>

#ifdef __cplusplus
extern "C"
{
#endif

/* generic logging (no level) */
#define zeke_log(fmt, ...) log_message(ZOO_LOG_LEVEL_INFO, 0, \
                           zeke_shmlog_ident_get(), \
                           zeke_format_log_message( (fmt), ## __VA_ARGS__ ))
#define zeke_log_id(id, fmt, ...) log_message(ZOO_LOG_LEVEL_INFO, 0, (id), \
                           zeke_format_log_message( (fmt), ## __VA_ARGS__ ))
#define zeke_log_pid(id, pid, fmt, ...) log_message(ZOO_LOG_LEVEL_INFO, (pid), (id), \
                           zeke_format_log_message( (fmt), ## __VA_ARGS__ ))

/* Low-level shared memory ring buffer logging */

#ifndef ZEKE_LOG_ENTRY_SIZE
#define ZEKE_LOG_ENTRY_SIZE 1024
#endif

typedef struct zeke_log_entry {
  char source[32];
  char info[ZEKE_LOG_ENTRY_SIZE];
} zeke_log_entry_t;

typedef struct zeke_shmlog_header zeke_shmlog_header_t;
typedef struct zeke_shmlock zeke_shmlock_t;

/* Open a new shared memory log. The pool will be userd as a parent for the
 * entire system if it has not yet been initialized which means that any passed
 * pool MUST be long lived. Passing NULL will create a new global long lived
 * pool.
 *
 * The second argument can configure the total number of log entries in the
 * ringer buffer, pass -1 or 0 to use the default. If the number passed, other
 * that -1 or 0, is different than the current number of an open global shm
 * logger then a new logger will be created which will *completely* replace the
 * old one. This is no different than calling zeke_shmlog_size_set() after
 * calling zeke_shmlog_open().
 */
ZEKE_EXPORT zeke_shmlog_header_t *zeke_shmlog_open(apr_pool_t*, int nrecs);

/* Returns the maximum number of log records the global shared memory log
 * system is configured for in the second argument.  Returns the first viable
 * record in the third arg (0 based, up to nrecs-1).
 *
 * Returns the next record index that will be written to in the fourth arg (0
 * based, up to nrecs-1).
 *
 * If any of the last three arguments are NULL they are ignored.
 *
 * Attempts to read the number of records will try to lock and unlock the
 * shared memory region but will silently ignore failures.
 */
ZEKE_EXPORT apr_status_t zeke_shmlog_size_get(zeke_shmlog_header_t*,
                                              apr_uint32_t *nrecs,
                                              apr_uint32_t *first_rec,
                                              apr_uint32_t *next_rec);

/* Set the maximum number of log records to global shared memory log system
 * will hold its ring buffer. If this number is different than the currently
 * configured number a new shared memory region will be created and the old one
 * destroyed. *THIS WILL RESULT IN THE TOTAL LOSS OF ALL INFORMATION IN THE
 * RING BUFFER*.
 *
 * NB: This function *may* replace the opaque pointer referenced by the first
 * argument.
 */
ZEKE_EXPORT apr_status_t zeke_shmlog_size_set(zeke_shmlog_header_t**, apr_uint32_t nrecs);

/* Set the identifying label assigned to each log entry. This identifier has a
 * 32 character limit and will be truncated automatically.
 */
ZEKE_EXPORT void zeke_shmlog_ident_set(const char *ident);
ZEKE_EXPORT const char *zeke_shmlog_ident_get(void);

/* Lock the shared memory region opportunistically. Only one process or thread
 * may hold this lock at one time. If the region is already lock this call will
 * never block but simply return an error.
 */
ZEKE_EXPORT apr_status_t zeke_shmlog_lock(zeke_shmlog_header_t*);

/* Unlock the shared memory region. Only the process that acquired the lock
 * should attempt to unlock it.
 */
ZEKE_EXPORT apr_status_t zeke_shmlog_unlock(zeke_shmlog_header_t*);

/* Write one log entry to the shared memory ring buffer, increment the internal
 * offset to indicate this and return the new entry.
 */
ZEKE_EXPORT const zeke_log_entry_t *zeke_shmlog_write(zeke_shmlog_header_t*,
                                         const char *buf, apr_size_t len);

/* Read one log entry from the shared memory ring buffer. If consume is !0 the
 * internal read pointer will be updated `consume` positions forward.  To read
 * one entry and position for next pass consume as 1, to skip one entry and
 * then return the next as well as positioning to the next beyond that for the
 * next read use consume = 2, etc.
 */
ZEKE_EXPORT const zeke_log_entry_t *zeke_shmlog_read(zeke_shmlog_header_t*,
                                                     int consume);

/* Read a specific record number from the shared memory region.  The index
 * number must be less than the total number of records as returned by
 * zeke_shmlog_size_get() otherwise NULL is returned. *THERE IS NO GUARANTEE
 * THAT THE OBJECT RETURNED HAS EVER BEEN WRITTEN!*. If the caller wishes to
 * ensure the returned object will not be overwritten they must first call
 * zeke_shmlog_lock().
 */
ZEKE_EXPORT const zeke_log_entry_t *zeke_shmlog_entry_get(zeke_shmlog_header_t*,
                                                           unsigned index);
/* Stream interface */

/* Open a new stdio FILE* stream to the shared memory log. The stream is always
 * opened in read/write mode and may be accessed as a normal file with
 * fread()/fwrite(). It does not support fseek() or ftell().
 *
 * The first argument will be filled in with the opaque context pointer for the
 * shared memory log region if non-NULL.
 *
 * The second argument will link the stream to a pool such that when the pool
 * is cleared or destroyed the stream will be "closed". This is optional and
 * passing NULL will result in the stream remaining open until fclose() is
 * called.
 */
ZEKE_EXPORT FILE *zeke_shmlog_stream_open(zeke_shmlog_header_t **hdrp,
                                          apr_pool_t *cont);


/* shared memory locking */
ZEKE_EXPORT apr_status_t zeke_shmlock_create(zeke_shmlock_t **newlock,
                                             const char *fname,
                                             apr_lockmech_e mech,
                                             apr_pool_t*);
ZEKE_EXPORT apr_status_t zeke_shmlock_rdlock(zeke_shmlock_t*, int block);
ZEKE_EXPORT apr_status_t zeke_shmlock_wrlock(zeke_shmlock_t*, int block);
#define zeke_shmlock_lock(l) zeke_shmlock_wrlock((l),1)
ZEKE_EXPORT apr_status_t zeke_shmlock_unlock(zeke_shmlock_t*);

#ifdef __cplusplus
}
#endif

#endif /* _LIBZEKE_LOG_H */
