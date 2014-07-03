#ifndef _LIBZEKE_H
#define _LIBZEKE_H

#ifdef __cplusplus
extern "C"
{
#endif

#ifndef _LIBZEKE_CONFIG_H
#define _LIBZEKE_CONFIG_H
# ifdef HAVE_LIBZEKE_CONFIG_H
# include <zeke/libzeke_config.h>
# endif
#endif /* _LIBZEKE_CONFIG_H */

#include <zeke/libzeke_version.h>

#ifndef ZEKE_PRINTF
#define ZEKE_PRINTF(s,e) __attribute__((format (printf, s, e)))
#endif

#ifndef ZEKE_USE_THREADS
# define UNTHREADED
# ifdef THREADED
# undef THREADED
# endif /* THREADED */
#else /* ! ZEKE_USE_THREADS */
# ifndef THREADED
# define THREADED
# endif /* THREADED */
#endif /* ZEKE_USE_THREADS */

#if defined(HAVE_RELAXED_ALIAS_ATTRIBUTE) && defined(RELAX_STRICT_ALIASING_ATTRIBUTE)
#define ZEKE_ALIASABLE RELAX_STRICT_ALIASING_ATTRIBUTE
#define ZEKE_MAY_ALIAS(type) type RELAX_STRICT_ALIASING_ATTRIBUTE
#else
#define ZEKE_MAY_ALIAS(type) type
#define ZEKE_ALIASABLE
#endif

#ifdef HAVE_VISIBILITY_ATTRIBUTE
# ifdef VISIBILITY_EXPORT
# define ZEKE_PUBLIC VISIBILITY_EXPORT extern
# define ZEKE_EXPORT VISIBILITY_EXPORT extern
# else
# define ZEKE_PUBLIC extern
# define ZEKE_EXPORT extern
# endif /* VISIBILITY_EXPORT */

# ifdef VISIBILITY_HIDDEN
# define ZEKE_HIDDEN VISIBILITY_HIDDEN
# define ZEKE_EXTERN VISIBILITY_HIDDEN extern
# else
# define ZEKE_EXTERN extern
# define ZEKE_HIDDEN
# endif /* VISIBILITY_HIDDEN */
#else /* !HAVE_VISIBILITY_ATTRIBUTE */
# define ZEKE_PUBLIC extern
# define ZEKE_EXPORT extern
# define ZEKE_EXTERN extern
# define ZEKE_HIDDEN
#endif

#define ZEKE_DECLARE_GENERIC_ACCESSOR(type,rtype,cls) \
  ZEKE_EXTERN cls zeke_##rtype##_t *zeke_##type##_##rtype##_get \
    (const zeke_##type##_t*)
#define ZEKE_EXPORT_GENERIC_ACCESSOR(type,rtype,cls) \
  ZEKE_EXPORT cls zeke_##rtype##_t *zeke_##type##_##rtype##_get \
    (const zeke_##type##_t*)
#define ZEKE_DECLARE_CONST_ACCESSOR(type,rtype) ZEKE_DECLARE_GENERIC_ACCESSOR(type,rtype,const)
#define ZEKE_EXPORT_CONST_ACCESSOR(type,rtype) ZEKE_EXPORT_GENERIC_ACCESSOR(type,rtype,const)
#define ZEKE_DECLARE_ACCESSOR(type,rtype) ZEKE_DECLARE_GENERIC_ACCESSOR(type,rtype,)
#define ZEKE_EXPORT_ACCESSOR(type,rtype) ZEKE_EXPORT_GENERIC_ACCESSOR(type,rtype,)

#define ZEKE_CONST_CONN_DECLARE_ACCESSOR(type) \
  ZEKE_EXTERN const zeke_connection_t *zeke_##type##_connection_get \
    (const zeke_##type##_t*)
#define ZEKE_CONST_CONN_EXPORT_ACCESSOR(type) \
  ZEKE_EXPORT const zeke_connection_t *zeke_##type##_connection_get \
    (const zeke_##type##_t*)
#define ZEKE_CONN_DECLARE_ACCESSOR(type) \
  ZEKE_EXTERN zeke_connection_t *zeke_##type##_connection_get \
    (const zeke_##type##_t*)
#define ZEKE_CONN_EXPORT_ACCESSOR(type) \
  ZEKE_EXPORT zeke_connection_t *zeke_##type##_connection_get \
    (const zeke_##type##_t*)
#define ZEKE_POOL_DECLARE_ACCESSOR(type) \
  ZEKE_EXTERN apr_pool_t *zeke_##type##_pool_get \
    (const zeke_##type##_t*)
#define ZEKE_POOL_EXPORT_ACCESSOR(type) \
  ZEKE_EXPORT apr_pool_t *zeke_##type##_pool_get \
    (const zeke_##type##_t*)

#if defined(__GNUC__) && __GNUC__ >= 4
#define ZEKE_EXPORT_SENTINEL ZEKE_EXPORT __attribute__((sentinel))
#define ZEKE_PUBLIC_SENTINEL ZEKE_PUBLIC __attribute__((sentinel))
#define ZEKE_EXTERN_SENTINEL ZEKE_EXTERN __attribute__((sentinel))
#else
#define ZEKE_EXPORT_SENTINEL ZEKE_EXPORT
#define ZEKE_PUBLIC_SENTINEL ZEKE_PUBLIC
#define ZEKE_EXTERN_SENTINEL ZEKE_EXTERN
#endif

/* libapr-1 */
#include <apr.h>
#include <apr_portable.h>
#include <apr_pools.h>
#include <apr_hash.h>
#include <apr_tables.h>
#include <apr_file_io.h>
#include <apr_file_info.h>
#include <apr_env.h>
#include <apr_poll.h>
#include <apr_signal.h>
#include <apr_strings.h>
#include <apr_time.h>
#include <apr_getopt.h>
#include <apr_general.h>
#include <apr_lib.h>

/* libaprutil-1 */
#include <apr_strmatch.h>
#include <apr_uuid.h>

/* libzookeeper_mt / libzookeeper_st */
#include <zookeeper/zookeeper.h>

#ifndef APR_VERSION_AT_LEAST
#define APR_VERSION_AT_LEAST(major,minor,patch)                    \
                          (((major) < APR_MAJOR_VERSION)           \
  || ((major) == APR_MAJOR_VERSION && (minor) < APR_MINOR_VERSION) \
  || ((major) == APR_MAJOR_VERSION && (minor) == APR_MINOR_VERSION && (patch) <= APR_PATCH_VERSION))
#endif  /* APR_VERSION_AT_LEAST */

#ifndef APR_ARRAY_PUSH
#define APR_ARRAY_PUSH(ary,type) (*((type *)apr_array_push(ary)))
#endif

#ifndef APR_ARRAY_IDX
#define APR_ARRAY_IDX(ary,i,type) (((type *)(ary)->elts)[i])
#endif

#define ZEKE_MAGIC_CHECK(o,m) do { \
  assert((void*)(o) != (void*)0); \
  assert(!(m) || ((o)->magic == (m))); \
} while(0)
#define ZEKE_NULL_CHECK(o) assert((void*)(o) != (void*)0)
#define ZEKE_MAGIC_CHECK_NULLOK(o,m) do { \
  if((void*)(o) != (void*)0) { assert(!(m) || ((o)->magic == (m))); } \
} while(0)

/* common types */
typedef apr_status_t zk_status_t;
typedef void zeke_context_t;
typedef apr_uint32_t zeke_magic_t;
typedef struct zeke_connection zeke_connection_t;
typedef struct zeke_private zeke_private_t;
typedef struct zeke_callback_data zeke_callback_data_t;
typedef struct zeke_watcher_callback_data zeke_watcher_cb_data_t;

ZEKE_POOL_EXPORT_ACCESSOR(connection);

struct zeke_watcher_callback_data {
  apr_pool_t *pool;
  zeke_private_t *priv;
  const zeke_context_t *ctx;
  const zeke_connection_t *connection;
  int type,state;
  const char *path;
};

struct zeke_callback_data {
  apr_pool_t *pool;
  zeke_private_t *priv;
  const zeke_context_t *ctx;
  const zeke_connection_t *connection;
  zk_status_t status;
  const struct Stat *stat;
  const char *value;
  apr_ssize_t value_len;
  union {
    const void *opaque;
    const struct String_vector *strings;
    struct ACL_vector *acl;
    zoo_op_result_t *results;
  } vectors;
};

typedef enum {
  ZEKE_CALLBACK_IGNORE = 0,
#define ZEKE_CALLBACK_IGNORE 0
  ZEKE_CALLBACK_DESTROY = 1,
#define ZEKE_CALLBACK_DESTROY 1
} zeke_cb_t;

typedef zeke_cb_t (*zeke_callback_fn)(const zeke_callback_data_t*);
typedef void (*zeke_watcher_fn)(const zeke_watcher_cb_data_t*);
typedef int (*zeke_sort_compare_fn)(zeke_context_t*,const char *key1, const char *key2);

#ifdef HUGE_STRING_LEN
#define ZEKE_BUFSIZE HUGE_STRING_LEN
#else
#define ZEKE_BUFSIZE 4096
#endif

#ifndef ZEKE_DEFAULT_ACL
#define ZEKE_DEFAULT_ACL NULL
#endif

/* very global globals */
ZEKE_EXPORT apr_file_t *zstdout;
ZEKE_EXPORT apr_file_t *zstdin;
ZEKE_EXPORT apr_file_t *zstderr;

#define zeke_printf(format, ...) apr_file_printf(zstdout, format, ## __VA_ARGS__)
#define zeke_eprintf(format, ...) apr_file_printf(zstderr, format, ## __VA_ARGS__)

/* top-level library prototypes */

/* ALWAYS call zeke_init() or zeke_init_cli() before any other libzeke calls. These wrap the related
   libapr initializer calls, so it's not necessary to also call the apr versions */
ZEKE_EXPORT zk_status_t zeke_init(apr_pool_t*);
ZEKE_EXPORT zk_status_t zeke_init_cli(int*argc,
                                  char const *const **argv,
                                  char const *const **env);

/* This is a potentially abnormal method of terminating the running application. Technically
   speaking, this is actually always called via an atexit() handler, however it does nothing
   to ensure clean zookeeper connection closes. Use zeke_safe_shutdown() for that */
ZEKE_EXPORT void zeke_terminate(void);

/* This is the only provided mechanism for completely terminating the running application
   in a "clean" way (will correctly terminate open connections, etc). If termination is not
   immediately possible it will occur once the current i/o operation completes. */
ZEKE_EXPORT void zeke_safe_shutdown(int exitcode);

/* Version of safe shutdown which is fully reentrant.  It will, however, never terminate
   immediately but simply set a flag that termination should happen ASAP.
   Safe for use inside signal handlers. */
ZEKE_EXPORT void zeke_safe_shutdown_r(int exitcode);

/* Extended version of zeke_safe_shutdown which will print a printf style formatted string
   immediately before exiting. This can actually be called multiple times (if shutdown
   cannot occur immediately, otherwise termination would occur immediately after the
   first call) in which case each shutdown msg will be printed in the order that
   zeke_safe_shutdown_ex() was called. Newlines will be added if not present at the end
   of the msg string.
*/
ZEKE_EXPORT void zeke_safe_shutdown_ex(int exitcode, const char *fmt, ...)
                                        ZEKE_PRINTF(2,3);
/* Vararg version of zeke_safe_shutdown_ex(); */
ZEKE_EXPORT void zeke_safe_shutdown_va(int exitcode, const char *fmt, va_list ap);

/* A version of safe shutdown for use with a specific connection. The connection
 * is closed followed by normal shutdown operations.
 */
ZEKE_EXPORT void zeke_safe_shutdown_conn(const zeke_connection_t *conn, int code);

/* Returns !0 if libzeke is shutting down, or 0 if not. If code is !NULL it
   will be filled in with the exitcode to be used once safe shutdown is
   completed. This may be adjusted at any time before shutdown by simply calling
   zeke_safe_shutdown() again.
*/
ZEKE_EXPORT int zeke_is_safe_shutdown(int *code);

/* create a new secondary root child pool */
ZEKE_EXPORT apr_pool_t *zeke_root_subpool_create(void);

/* Register an exit handler to be called when libzeke is shutting down. This will
   always be called after the subpool created by zeke_root_subpool_create() have
   been destroyed but while the root pool is still viable. */
ZEKE_EXPORT void  zeke_atexit(void (*func)(void*), void *arg);

#ifdef HAVE_ASSERT_H
#include <assert.h>
#else
#include <zeke/libzeke_errno.h>
#define assert ZEKE_ASSERT
#endif

/* Connection API prototypes */
/* Return the connection handle associated with a zhandle, will be NULL if
 the connection has not yet been started. */
ZEKE_EXPORT const zeke_connection_t *zeke_zhandle_connection_get(const zhandle_t*);

/* Return the apr_pool_t associated with the context of a given connection,
   this pool is *different* that the session_pool in that it may exist longer
   than a single session. */
ZEKE_EXPORT apr_pool_t *zeke_connection_context_pool_get(const zeke_connection_t*);

/* Return the apr memory pool associated with a given session. This memory pool is
ONLY available after the session is fully up and functioning normally. */
ZEKE_EXPORT apr_pool_t *zeke_session_pool(const zeke_connection_t*);

/* Return the apr memory pool associated with the context of a connection associated
with a zhandle (lots of indirection!). This pool will always be around
longer than the session pool but other than this, the exact lifetime is undefined. */
ZEKE_EXPORT apr_pool_t *zeke_zhandle_context_pool(const zhandle_t*);

/* Return the libzookeeper handle associated with a connection. This will be NULL
if zeke_connection_close() has been called, an error has occurred or
zeke_connection_start() has not been called. */
ZEKE_EXPORT const zhandle_t *zeke_connection_zhandle_get(const zeke_connection_t*);

/* Returns a *highly ephemeral* apr_socket_t associated with a connection.
This is ONLY available after a call to zeke_connection_poll*() or
zeke_connection_socket_update().

!! The returned socket should not be stored anywhere less ephemeral than itself.
It may well be invalid after the next call to zeke_connection_poll*( or
zeke_connection_socket_update() !!
*/
ZEKE_EXPORT const apr_socket_t *zeke_connection_socket_get(const zeke_connection_t*);

/* This is a version of zeke_connection_socket_get() that will create a new
socket if necessary and return additional required io parameters. It can also
update an existing socket, *possibly invalidating and replacing with a new one*.
This can be called any time after zeke_connection_start() returns successfully

@events@ if non-NULL, will be a bit field filled in with the desired operations
         expressed by APR_POLL constants (APR_POLLIN for reading, APR_POLLOUT for
         writing)
@timeout@ if non-NULL, will be filled in with the maximum number of microseconds
          libzookeeper would prefer to block/sleep while waiting for the
          next i/o operation.
@context@ if non-NULL, will be filled in with an opaque pointer to internal
          data which should be associated with any i/o performed on the socket.
*/
ZEKE_EXPORT zk_status_t zeke_connection_socket_update(zeke_connection_t*,
                                                      apr_socket_t**,
                                                      apr_int16_t *events,
                                                      apr_interval_time_t *timeout,
                                                      void **context);

/* Similar to zeke_connection_socket_update, but for non-poll based I/O.
 * If successful returns flags set to some combo of APR_READ|APR_WRITE
 * and timeout set to the desired zookeeper timeout. If the underlying
 * socket has become invalidated, this call will destroy it (conn->sock
 * will be NULL) and return APR_EOF.
 */
ZEKE_EXPORT zk_status_t zeke_connection_socket_maint(zeke_connection_t*,
                                                     apr_uint32_t *flags,
                                                     apr_interval_time_t *timeout);

ZEKE_EXPORT zk_status_t zeke_connection_create(zeke_connection_t**,
                                               const char *zkhosts,
                                               zeke_watcher_fn,
                                               apr_pool_t*);


ZEKE_EXPORT zk_status_t zeke_connection_create_ex(zeke_connection_t**,
                                                  const char *zkhosts,
                                                  zeke_watcher_fn,
                                                  zhandle_t*,
                                                  int use_subpool,
                                                  apr_pool_t*);

/* Close an existing zk connection, regardless of state. This will invalidate
the zookeeper handle and destroy any associated memory pools. This can be
dangerous to do inside of callback handlers (usual symtom is double-free errors
from glibc), uses zeke_connection_shutdown() in those cases. */
ZEKE_EXPORT zk_status_t zeke_connection_close(zeke_connection_t*);

/* Shutdown an existing zk connection, regardless of state, but does not release
associated resoruces. */
ZEKE_EXPORT zk_status_t zeke_connection_shutdown(zeke_connection_t*);

/* Returns 1 if the connection is open, otherwise 0. */
ZEKE_EXPORT int zeke_connection_is_open(const zeke_connection_t*);

/* Returns 1 if a connection is open and an active session established,
otherwise 0. */
ZEKE_EXPORT int zeke_connection_is_active(const zeke_connection_t*);

ZEKE_EXPORT int zeke_connection_is_closed(const zeke_connection_t*);
ZEKE_EXPORT int zeke_connection_is_starting(const zeke_connection_t*);

/* Begin a zookeeper session, the zeke_connection_t pointer must first
be created by calling zeke_connection_create() or zeke_connection_create_ex().
The client argument may contain an existing session id in which case this
session will be resumed (or attempt to resume).

The retries argument should point to a a maximum allowed retries, it will
be decremented for each attempt to initiate a session and abort upon reaching
zero. This applies ONLY to the initiation, not to the entire session warmup.

Note that the actual connection happens asyncronously. This simply starts
the connection and ensures there are no immediate errors (by retrying
`retries` times).

If a session later fails, it will be necessary to call this function again
in order to restart it.
*/
ZEKE_EXPORT zk_status_t zeke_connection_start(zeke_connection_t*,
                                              clientid_t *client,
                                              unsigned *retries);

/* Add a global watcher to a connection. The watcher will be called back
   for generic session activity (connection, disconnection, etc)
*/
ZEKE_EXPORT void zeke_connection_watcher_add(zeke_connection_t*,
                                             zeke_watcher_fn);

/* Add a "named" watcher which should have a unique name. These are just
   like standard global watchers but can be removed via
   zeke_connection_named_watcher_remove().
*/
ZEKE_EXPORT void zeke_connection_named_watcher_add(zeke_connection_t*,
                                                          const char *name,
                                                          zeke_watcher_fn);
/* Remove a named watcher */
ZEKE_EXPORT zk_status_t zeke_connection_named_watcher_remove(zeke_connection_t*,
                                                             const char *name);

/* Fills in a caller-supplied apr_pollfd_t structure for i/o polling.
   @npollfds@ is both an input and output arg:
    - If passed it should contain the maximum number
      of continuous apr_pollfd_t structures pointed
      to by the second argument. On return it will
      contain the actual number of structurs filled
      in. *This may be zero*, in which case the apr_pollfd_t
      should not be polled or included in a poll.
    - If NULL, only a single apr_pollfd_t structure
      will be filled out.
  @min_timeout@ is both an input and output arg:
    - If passed it should contain the minimum desired poll timeout.
    If libzookeeper wishes to use a lower timeout it will be returned
    in this arg. If the value pointed to is -1 the libzookeeper
    desired timeout will *always* be returned.
    - If NULL, no timeout will be returned.
*/
ZEKE_EXPORT zk_status_t zeke_connection_pollfd_set(zeke_connection_t*,
                                                   apr_pollfd_t*,
                                                   apr_size_t *npollfds,
                                                   apr_interval_time_t *min_timeout);


/* Dispatcher API Prototypes */

/* These can be used to determine the status of a zookeeper operation from a callback
   as well as to set the status for callback chaining. */
ZEKE_EXPORT zk_status_t zeke_callback_status_get(const zeke_callback_data_t*);
ZEKE_EXPORT zk_status_t zeke_callback_status_set(const zeke_callback_data_t*,
                                                 zk_status_t);

/* Create the main callback structure. If pool is provided then a new pool is NOT
 * created, and will not be destroyed when zeke_callback_data_destroy() is called.
 * if pull == NULL and new child pool is created from the main dispatch pool and
 * will be later released or explicitly destroyed by zeke_callback_data_destroy()
 */
ZEKE_EXPORT zeke_callback_data_t *zeke_callback_data_create(const zeke_connection_t*,
                                                            zeke_watcher_fn watcher,
                                                            apr_pool_t *pool);
ZEKE_EXPORT zeke_callback_data_t *zeke_callback_data_create_ex(const zeke_connection_t*,
                                                               zeke_watcher_fn watcher,
                                                               apr_pool_t*,
                                                               int create_pool);

ZEKE_EXPORT zk_status_t zeke_callback_data_destroy(zeke_callback_data_t*);

/* Initate an async zoo node creation operation, the callback will be called when the
   operation completes, successfully or otherwise */
ZEKE_EXPORT zk_status_t zeke_acreate(zeke_callback_data_t**, const zeke_connection_t*,
                                    const char *path,
                                    const char *value, apr_ssize_t value_len,
                                    const struct ACL_vector *acl, int flags,
                                    zeke_callback_fn callback,const zeke_context_t *ctx);

/* Initiate an async zoo node deletion operation, the callback will be called when then
   operation competes successfully or otherwise */
ZEKE_EXPORT zk_status_t zeke_adelete(zeke_callback_data_t**, const zeke_connection_t*,
                                     const char *path,
                                     int version,zeke_callback_fn callback,
                                     const zeke_context_t *ctx);

/* Initiate an async zoo node existence test operation, the callback will be called when then
   operation competes successfully or otherwise */
ZEKE_EXPORT zk_status_t zeke_aexists(zeke_callback_data_t**, const zeke_connection_t*,
                                     const char *path,
                                     zeke_watcher_fn watcher,
                                     zeke_callback_fn callback,
                                     const zeke_context_t *ctx);

/* Initiate an async zoo node get operation, the callback will be called when then
   operation competes successfully or otherwise */
ZEKE_EXPORT zk_status_t zeke_aget(zeke_callback_data_t**, const zeke_connection_t*,
                                  const char *path,
                                  zeke_watcher_fn watcher,
                                  zeke_callback_fn callback,
                                  const zeke_context_t *ctx);

/* Initiate an async zoo node set operation, the callback will be called when then
   operation competes successfully or otherwise */
ZEKE_EXPORT zk_status_t zeke_aset(zeke_callback_data_t**, const zeke_connection_t*,
                                 const char *path,
                                 const char *buffer,
                                 apr_size_t buflen,
                                 int version,
                                 zeke_callback_fn,
                                 const zeke_context_t *ctx);

/* Initiate an async zoo node child list operation, the callback will be called when then
   operation competes successfully or otherwise */
ZEKE_EXPORT zk_status_t zeke_aget_children(zeke_callback_data_t**,
                                const zeke_connection_t*,
                                const char *path,
                                zeke_watcher_fn watcher,
                                zeke_callback_fn callback,
                                const zeke_context_t *ctx);

/* Initiate an async zoo node sync operation, the callback will be called when then
   operation competes successfully or otherwise */
ZEKE_EXPORT zk_status_t zeke_async(zeke_callback_data_t**,
                                   const zeke_connection_t*,
                                   const char *path,
                                   zeke_callback_fn callback,
                                   const zeke_context_t *ctx);

/* Initiate an async zoo node acl get operation, the callback will be called when then
   operation competes successfully or otherwise */
ZEKE_EXPORT zk_status_t zeke_aget_acl(zeke_callback_data_t**,
                                   const zeke_connection_t*,
                                   const char *path,
                                   zeke_callback_fn callback,
                                   const zeke_context_t *ctx);

/* Initiate an async zoo node acl set operation, the callback will be called when then
   operation competes successfully or otherwise */
ZEKE_EXPORT zk_status_t zeke_aset_acl(zeke_callback_data_t**,
                                  const zeke_connection_t*,
                                  const char *path, int version,
                                  const struct ACL_vector *acl,
                                  zeke_callback_fn callback,
                                  const zeke_context_t *ctx);

/* Initiate an async zoo transaction (multiple ops atomically) operation, the callback will be called when then
   operation competes successfully or otherwise. For a higher level interface to zookeeper transactions,
   see libzeke_trans.h */
ZEKE_EXPORT zk_status_t zeke_amulti(zeke_callback_data_t**,
                                  const zeke_connection_t*,
                                  const apr_array_header_t *ops,
                                  zeke_callback_fn callback,
                                  const zeke_context_t *ctx);

/* Force a watcher callback context to a particular value. Occasionally useful (rarely) when chaining
   callbacks.
   DEPRECATED! DO NOT USE! (use zeke_callback_separate_watcher() instead)
 */
ZEKE_EXPORT void zeke_set_watcher_context(zeke_callback_data_t *cbd,
                                               const zeke_context_t *ctx);

/* Force a watcher callback to use an unrelated memory pool so that its lifespan is in no way determined
 * by the non-watcher callback pool's life.
 */
ZEKE_EXPORT void zeke_callback_separate_watcher(zeke_callback_data_t *cbd,
                                                const zeke_context_t *ctx);

#ifdef __cplusplus
}
#endif

#endif /* _LIBZEKE_H */
