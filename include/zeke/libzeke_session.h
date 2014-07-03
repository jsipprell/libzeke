#ifndef _LIBZEKE_SESSION_H
#define _LIBZEKE_SESSION_H

#include <zeke/libzeke.h>

#ifdef __cplusplus
extern "C"
{
#endif

/* Private session-life context objects accessed via a unique caller-provided
key. These will always live as long as a zookeeper session is active (which 
may actually be LONGER than the connection the are initially associated with.
Sessions are only loosely related to connections. */
typedef void (*zeke_session_cleanup_fn)(zeke_context_t*,void *);

/* Create new private session context storage, zeke_session_context_create()
will return existing storage if it exists otherwise it will allocate new storage.
Initial storage space will be zero-filled.
!! Unique keys are a REQUIREMENT !! */
ZEKE_EXPORT zeke_context_t *zeke_session_context_create(const zeke_connection_t*,
                                                        const char *key,
                                                        apr_size_t size);

/* Release a given session context so that its key may be reused.
 Note that this does NOT free any associated memory, for that either
 apr_pool_clear() or apr_pool_destroy() must be called. */

ZEKE_EXPORT void zeke_session_context_release(const zeke_connection_t*,
                                              const char *key);

/* Return existing private session storage for a unique key. If storage
has not yet been allocated (via a call to create() above) then NULL
is returned. */
ZEKE_EXPORT zeke_context_t *zeke_session_context(const zeke_connection_t*,
                                                       const char *key);

/* Returns !0 if the current session exists and has an entry for the
   specified key. */
ZEKE_EXPORT int zeke_session_context_exists(const zeke_connection_t*,
                                            const char *key);
/* Register a cleanup to be called when the current session is torn down.
The cleanup callback will be called with two arguments:
  1. A pointer to the storage as previously allocated above.
  2. A user-defined private opaque pointer passed as the third argument
     here.
*/
ZEKE_EXPORT void zeke_session_cleanup_register(const zeke_connection_t*,
                                               const char *key,
                                               const void *arg,
                                               zeke_session_cleanup_fn cleanup);

/* Force all cleanup handlers registered for the given unique session context
key to run. Once run they will never run again, however new registered cleanup
handlers will run (either via another call to zeke_session_cleanups_run or when
the session is torn down). */
ZEKE_EXPORT zk_status_t zeke_session_cleanups_run(const zeke_connection_t*,
                                                  const char *key);
#ifdef __cplusplus
}
#endif

#endif /* _LIBZEKE_SESSION_H */
