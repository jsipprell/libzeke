#ifndef _LIBZEKE_EVENT_H
#define _LIBZEKE_EVENT_H

/* libzeke API made available when configured with --with-libevent and linked against
 * libevent-2.
 */

#include <zeke/libzeke.h>

#define ZEKE_IMMEDIATE APR_TIME_C(0)
#ifndef ZEKE_NULL_TIME
#define ZEKE_NULL_TIME APR_TIME_C(0)
#endif
#ifndef ZEKE_FOREVER
#define ZEKE_FOREVER APR_TIME_C(-1)
#endif
#ifndef ZEKE_NO_TIMEOUT
#define ZEKE_NO_TIMEOUT APR_TIME_C(-1)
#endif

#ifdef __cplusplus
extern "C"
{
#endif
/* returns 1 if libzeke is configured to use libevent. If version is !NULL it
 * will be set to a static string containing the libevent version information
 * detected when libzeke was built. This function is ALWAYS available.
 */
ZEKE_EXPORT int zeke_is_libevent_enabled(const char **version);

#ifdef __cplusplus
}
#endif

#ifdef LIBZEKE_USE_LIBEVENT
#include <event.h>

#ifdef __cplusplus
extern "C"
{
#endif

/* Return the libevent event_base for a given pool. If the pool does not yet
 * have an event_base one will be created. When the pool is destroyed the
 * base will be freed *after* any events created by zeke_event_create()
 * have been deleted and freed.
 */
ZEKE_EXPORT struct event_base *zeke_event_base_get(apr_pool_t *cont);

/*
 * The extended version of zeke_event_base_get(), allows creation of
 * new event bases to be conditional.
 */
ZEKE_EXPORT struct event_base *zeke_event_base_get_ex(apr_pool_t *cont,
                                                      int create);

/* Returns 1 if the pool has an existing event base */
ZEKE_EXPORT int zeke_event_base_exists(apr_pool_t*);

/*
 * Set the event base for a pool or NULL if the pool should have no event base.
 * If own is !0 then the event_Base willl be free'd when the pool is cleared or
 * destroyed. NOTE: The old base is NEVER free'd by this call, the caller must
 * do this themselves if they want to prevent leakage.
 */
ZEKE_EXPORT zk_status_t zeke_event_base_set(struct event_base *base,
                                            int own,
                                            apr_pool_t *cont);

/* Wrapper around apr_pool_create(), creates a subpool from a given parent
 * where the new child pool will inherit the event_base of the parent.
 * If the parent does not have a pool, there is no difference between
 * this call and apr_pool_create().
 */
ZEKE_EXPORT apr_status_t zeke_event_base_pool_create(apr_pool_t **child,
                                                     apr_pool_t *parent);

/* Return a new libevent linked to an APR pool. When the pool is destroyed or
 * cleared the event will be deleted from its base (via event_del) and freed.
 * The base for the new event will be always be determined via a call to
 * zeke_event_base_get().
 */
ZEKE_EXPORT zk_status_t zeke_event_create(struct event **evp, apr_pool_t *cont);

/* More extensive version of zeke_event_create(), creates a new generic libevent
 * event associated with a pool such that when the pool is destroyed the event
 * will be removed from it's event_base (if it's been added) and freed
 * via libevent's event_free(). If persist is !0 the event will be created
 * with the EV_PERSIST flag.
 *
 * Flags may be zero or more bitwise ORs of:
 *   APR_READ: Event will be configured as EV_READ.
 *   APR_WRITE: Event will be configured as EV_WRITE.
 */
ZEKE_EXPORT zk_status_t zeke_event_create_ex(struct event **evp, apr_uint32_t flags,
                                             short persist, event_callback_fn handler,
                                             void *handler_arg, apr_pool_t *cont);

/* Associate a new libevent structure with a file so that it can be returned
 * if zeke_file_event_get() is later called. If all fields are provided this
 * will also add the event to event_base using the properties of the file
 * (EV_READ for APR_READ, EV_WRITE for APR_WRITE etc). When the file's pool is
 * destroyed the event will be removed and free'd.
 */
ZEKE_EXPORT zk_status_t zeke_file_event_create_ex(struct event**,
                                                  apr_file_t*,
                                                  event_callback_fn,
                                                  int add, apr_interval_time_t timeout,
                                                  apr_pool_t *base_pool);

/* Shortcut for creating a new event based on a file, does not call event_add().
 */
#define zeke_file_event_create(evp,f,p) zeke_file_event_create_ex((evp),(f),NULL,0,\
                                                          APR_TIME_C(-1),(p))

/* Return the libevent structure associated with a file (usually a pipe)
 * if one exists. */

ZEKE_EXPORT struct event *zeke_file_event_get(apr_file_t*);
/* Add an event created with zeke_file_event_create_ex() to its event_base */
ZEKE_EXPORT zk_status_t zeke_file_event_add(apr_file_t*, apr_interval_time_t timeout);

/* Query the event associated with a file to see if there is an event pending.
 * If the expires argument is non-NULL this will be filled in with the timeout
 * remaining before the event fires, if any is available. If no event has been
 * created for the file, 0 is always returned.
 */
ZEKE_EXPORT int zeke_file_event_pending(apr_file_t*, apr_interval_time_t*);

/* Set the callback and opaque user data passed as the third arg in a standard
 * libevent callback. By default this will be the apr_file_t itself if the
 * event was created with zeke_file_event_create_ex() or
 * zeke_file_event_create_ex(). If NULL is passed for the callback function
 * then the original callback will remain but the argument will change.  The
 * event will be automatically readded if it was pending at the time of the
 * call.
 */
ZEKE_EXPORT zk_status_t zeke_file_event_set(apr_file_t*, event_callback_fn,
                                                                 void *data);

/* Set the libevent types for the event associated with file. Flags can be set
 * to either APR_READ, APR_WRITE or both. If persist is set to non-zero the
 * event will be set to persitence mode so that it will automatically be
 * readded after each callback without any intervention.
 *
 * Note that pending events will be readded once they have been modified but
 * ONLY if the either APR_READ or APR_WRITE is specified or the event was
 * already waiting on a timeout or signal. Timeouts will be automatically
 * adjusted to account for elapsed time that the event has been pending without
 * timing out.
 */
ZEKE_EXPORT zk_status_t zeke_file_event_type_set(apr_file_t*,
                                                 apr_uint32_t flags,
                                                 short persist);

/* Remove an event added by zeke_file_event_add() */
ZEKE_EXPORT zk_status_t zeke_file_event_del(apr_file_t*);

/* Returns the event flags and persist mode for a file's associated
 * event. If the event hasn't been created via zeke_socket_event_create(),
 * APR_EINVAL is returned. If either flags or persist is NULL they will
 * be ignored.
 *
 * Flags:
 *   APR_READ: event will fire for read events.
 *   APR_WRITE: event will fire for write events.
 * persist:
 *   If 0, the event will not automatically re-add after firing.
 *   If !0, the event will automatically re-add after firing.
 */
ZEKE_EXPORT zk_status_t zeke_file_event_type_get(apr_file_t*,
                                                 apr_uint32_t *flags,
                                                 short *persist);

/* Returns the user data passed as the third argument in a libevent standard
 * callback. NULL is always returned if there is no event associated with the
 * file.
 */
ZEKE_EXPORT void *zeke_file_event_data_get(apr_file_t*);

/* ========== Socket version of zeke_file_event_*() functions ======= */

/* Create a new libevent event structure associated with an existing socket.  A
 * later call to zeke_socket_event_get() will return this event. If the flags
 * argument is non-zero and valid then the event will be added to its event
 * base automatically after creation. The event base used is retrieved from
 * "base_pool" via a call to zeke_event_base_get() which will create a new base
 * if one hasn't been previously set for the pool via zeke_event_base_set(). In
 * all cases the event will be deleted from its base and freed when the
 * *socket's* pool is cleared or destroyed.  NOTE: This means NOT the
 * base_pool, although if the base_pool creates a new event_base it will always
 * be destroyed *after* any events associated with it are destroyed.
 *
 * Flags are identical to apr_file_create():
 *   APR_READ - event will be read enabled (EV_READ).
 *   APR_WRITE - event will be write enabled (EV_WRITE).
 */
ZEKE_EXPORT zk_status_t zeke_socket_event_create_ex(struct event**,
                                                    apr_socket_t*,
                                                    event_callback_fn,
                                                    apr_uint32_t flags,
                                                    apr_interval_time_t timeout,
                                                    apr_pool_t *base_pool);
/* Shortcut for create a new event based on a socket, does not call
 * event_add(). */
#define zeke_socket_event_create(evp,s,p) zeke_socket_event_create_ex((evp),(s),NULL,0,\
                                                                     APR_TIME_C(-1),(p))
/* Retrieve the event associated with a socket */
ZEKE_EXPORT struct event *zeke_socket_event_get(apr_socket_t*);

/* Add an event created with zeke_socket_event_create_ex() to its event_base,
 * use APR_TIME_C(-1) for no timeout.
 */
ZEKE_EXPORT zk_status_t zeke_socket_event_add(apr_socket_t*, apr_interval_time_t timeout);

/* Query the event associated with a socket to see if there is an event pending.
 * If the expires argument is non-NULL this will be filled in with the timeout
 * remaining before the event fires, if any is available. If no event has been
 * created for the file, 0 is always returned.
 */
ZEKE_EXPORT int zeke_socket_event_pending(apr_socket_t*, apr_interval_time_t*);

/* Remove an event added by zeke_socket_event_add() */
ZEKE_EXPORT zk_status_t zeke_socket_event_del(apr_socket_t*);

/* Set the callback and opaque user data passed as the third arg in a standard
 * libevent callback. By default this will be the apr_socket_t itself if the
 * event was created with zeke_socket_event_create_ex() or
 * zeke_socket_event_create_ex(). If NULL is passed for the callback function
 * then the original callback will remain but the argument will change.  The
 * event will be automatically readded if it was pending at the time of the
 * call.
 */
ZEKE_EXPORT zk_status_t zeke_socket_event_set(apr_socket_t*, event_callback_fn,
                                                                   void *data);

/* Set the libevent types for the event associated with a socket. Flags can be
 * set to either APR_READ, APR_WRITE or both. If persist is set to non-zero the
 * event will be set to persitence mode so that it will automatically be
 * readded after each callback without any intervention.
 *
 * Note that pending events will be readded once they have been modified but
 * ONLY if the either APR_READ or APR_WRITE is specified or the event was
 * already waiting on a timeout or signal. Timeouts will be automatically
 * adjusted to account for elapsed time that the event has been pending without
 * timing out.
 */
ZEKE_EXPORT zk_status_t zeke_socket_event_type_set(apr_socket_t*,
                                                 apr_uint32_t flags,
                                                 short persist);

/* Returns the event flags and persist mode for a socket's associated
 * event. If the event hasn't been created via zeke_socket_event_create(),
 * APR_EINVAL is returned. If either flags or persist is NULL they will
 * be ignored.
 *
 * Flags:
 *   APR_READ: event will fire for read events.
 *   APR_WRITE: event will fire for write events.
 * persist:
 *   If 0, the event will not automatically re-add after firing.
 *   If !0, the event will automatically re-add after firing.
 */
ZEKE_EXPORT zk_status_t zeke_socket_event_type_get(apr_socket_t*,
                                                   apr_uint32_t *flags,
                                                   short *persist);

/* Return the userdata passed as the third arg to an event associated with a
 * socket. NULL is always returned if there is no event associated with the
 * socket.
 */
ZEKE_EXPORT void *zeke_socket_event_data_get(apr_socket_t*);

#ifdef __cplusplus
}
#endif

#endif /* LIBZEKE_USE_LIBEVENT */
#endif /* _LIBZEKE_EVENT_H */
