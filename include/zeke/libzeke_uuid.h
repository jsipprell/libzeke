#ifndef _LIBZEKE_UUID_H_
#define _LIBZEKE_UUID_H_

#include <zeke/libzeke.h>

#ifdef __cplusplus
extern "C"
{
#endif

/* Public api headers for the libzeke uuid registry module which allows a host or service to register
itself underneath a common well-known zookeeper node. (such as "/registry" or "/foo/uuids"). 

The most basic element of a registered item is a name,uuid tulple with the name being, by default, the
fully-qualified hostname of the running system and the uuid being randomly generated as needed. Both
of these can be overridden though. Registered tuples can be emphemeral or non-empheral.

Upon successful registry the module will provide a list of all currently registered tuples, including
itself. */

/* The opaque handle type for this object. */
typedef struct zeke_uuid_registry_t zeke_uuid_registry_t;

/* zeke_uuid_registry_pool_get(the_registry) -> apr memory pool that the registry was allocated from */
ZEKE_POOL_EXPORT_ACCESSOR(uuid_registry);

/* Callback types */
typedef void (*zeke_uuid_complete_fn)(const zeke_uuid_registry_t*,zeke_context_t *context);
typedef void (*zeke_uuid_timeout_fn)(const zeke_uuid_registry_t*,apr_interval_time_t elapsed,zeke_context_t *context);


/* Bit flags for passing to zeke_uuid_registry_create_ex() */

/* Normally, all registrations are ephemeral (ZOO_EPHEMERAL), this flag will make new entries
persist even after disconnection. */
#define ZEKE_UUID_REGISTER_PERMANENT  (1 << 0)

/* If there is already an existing entry for the name being registered use the uuid specified
in the zookeeper registry node instead of the one passed to create_ex(). The default behavior
is to overwrite any discovered uuids for a given name if the discovered uuid doesn't match
what was passed in. */
#define ZEKE_UUID_REGISTER_ACCEPT     (1 << 1)

/* UUID Registry API */

/* Full extended call to create a new uuid registry object.
  @param root The ZK node under which a node will be registered, if NULL defaults to /uuids
  @param name assign the uuid to this service or host name, if NULL defaults to fqdn
  @param uuid optional uuid to set name to, if NULL create a new random uuid if one isn't already registered
  @param timeout Maximum allowed time in usec that the operation may take, if this expires the timeout callback
  will be called. Default is -1 (no timeout)
  @param completed A function which will be passed the registry and the context (below) once registration
  is complete.
  @param timedout A function which will be passed the registry and total elapsed time if timeout expires.
  @param context An arbitrary caller-defined context to be passed to callbacks
  @param flags zookeeper flags to be used to create a new node (if necessary), such as ZOO_EPHEMERAL
  @param conn The libzeke connection the registry will use.
  @return ZEOK/APR_SUCCESS if the operation is pending, otherwise an error code accesible via the errors.h
  macros.
*/
ZEKE_EXPORT zk_status_t zeke_uuid_registry_create_ex(zeke_uuid_registry_t**,
                                                     const char *root,
                                                     const char *name,
                                                     apr_uuid_t *uuid,
                                                     apr_interval_time_t timeout,
                                                     zeke_uuid_complete_fn completed,
                                                     zeke_uuid_timeout_fn timedout,
                                                     zeke_context_t *context,
                                                     int flags,
                                                     const zeke_connection_t *conn);

/* A simplified version of zeke_uuid_registry_create_ex() that assumes defaults for everything except
the name (although NULL still results in fqdn being used) and completed callback. No timeout is provided
for using the simplified forms.

Note that the simplified forms ALWAYS use ZOO_EPHEMERAL */
ZEKE_EXPORT zk_status_t zeke_uuid_registry_create(zeke_uuid_registry_t**,
                                                  const char *name,
                                                  zeke_uuid_complete_fn completed,
                                                  zeke_context_t *context,
                                                  const zeke_connection_t*);

/* A simplified version of zeke_uuid_registry_create_ex() that assumes defaults for everything except
the root name, completed callback and uuid. Both name and uuid can be passed as NULL though and will then
take on their default values.

Note that the simplified forms ALWAYS use ZOO_EPHEMERAL */
ZEKE_EXPORT zk_status_t zeke_uuid_registry_create_uuid(zeke_uuid_registry_t**,
                                                       const char *root,
                                                       const char *name,
                                                       apr_uuid_t *uuid,
                                                       zeke_uuid_complete_fn completed,
                                                       zeke_context_t*,
                                                       const zeke_connection_t*);

/* Returns TRUE if the current registry operation has completed or timed out. It is unsafe to call
any other non-creation function until this returns true. */
ZEKE_EXPORT int zeke_uuid_registry_is_complete(const zeke_uuid_registry_t*);

/* Returns the status of the last completed registry operation */
ZEKE_EXPORT zk_status_t zeke_uuid_registry_status(const zeke_uuid_registry_t*);

/* Destroy an existing uuid registry object, this will also destroy anything that has allocated from the
object's pool (which is always a subpool of a connection). */
ZEKE_EXPORT zk_status_t zeke_uuid_registry_destroy(zeke_uuid_registry_t*);

/* Returns all current registry entries as an apr_hash_t where keys are service names (host names, host:port or
similar) and values are apr_uuid_t objects. If pool is non-NULL a new hash is constructed each time this function
is called (which can get expensive), otherwise the registry's own pool is used to construct the hash the first
time and the cached version can be used after that. The hash can be modified as long as the caller is aware that
by default (pool == NULL) all callers are operating on the same hash and all keys and values are also shared.
Passing a value for pool will result in a non-shared hash (including keys and values) as a side-effect */
ZEKE_EXPORT apr_hash_t *zeke_uuid_registry_hash_get(const zeke_uuid_registry_t*,apr_pool_t*);

/* Returns a shallow index of all node names in the registry. The array itself will be
allocated from the given pool but all entries will point to strings in the registry and
should not be modified. */
ZEKE_EXPORT const apr_array_header_t *zeke_uuid_registry_index_get(const zeke_uuid_registry_t*,
                                                                   int sorted, apr_pool_t*);

/* Return the total elapsed time taken for a uuid registry operation to complete. The operation
must have completed or timed out. */
ZEKE_EXPORT apr_interval_time_t zeke_uuid_registry_request_time_get(const zeke_uuid_registry_t*);

/* Return the name that the operation registered under (as passed to create()). If this was passed as 
NULL originally it will be set to the current host's fully qualified domainname. */
ZEKE_EXPORT const char *zeke_uuid_registry_name_get(const zeke_uuid_registry_t*,apr_pool_t*);

/* Fill in the uuid that the operation registered as (or detected prior registration as). */
ZEKE_EXPORT zk_status_t zeke_uuid_registry_uuid_get(const zeke_uuid_registry_t*, apr_uuid_t*);

/* Return a formatted version of the uuid that the operation registered as (or detected prior
registration as) */
ZEKE_EXPORT const char *zeke_uuid_registry_uuid_format(const zeke_uuid_registry_t*, apr_pool_t*);

/* Return the user context passed when the registry was first created */
ZEKE_EXPORT zeke_context_t * zeke_uid_registry_context(const zeke_uuid_registry_t*);

/* Set the user context */
ZEKE_EXPORT void zeke_uuid_registry_context_set(zeke_uuid_registry_t*,zeke_context_t*);

#ifdef __cplusplus
}
#endif

#endif /* _LIBZEKE_UUID_H_ */
