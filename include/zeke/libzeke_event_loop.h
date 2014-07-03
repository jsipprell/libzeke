#ifndef _LIBZEKE_EVENT_LOOP_H
#define _LIBZEKE_EVENT_LOOP_H

#include <zeke/libzeke.h>

#ifdef LIBZEKE_USE_LIBEVENT
#include <event.h>
#endif

#ifdef __cplusplus
extern "C"
{
#endif

/* libzeke expose two separate yet related event loop API. Prototypes for each can be found in this
   header
*/

/* Public low-level event loop API */
/* Get/Set an interrupt handler that will be called when an IO loop
   recognizes EINTR. The handler can return 0 to to ignore the
   interupt and continue or any other value to abort the IO loop.
*/
typedef int (*zeke_interrupt_fn)(zeke_connection_t*);
ZEKE_EXPORT void zeke_interrupt_handler_set(zeke_interrupt_fn handler);
ZEKE_EXPORT zeke_interrupt_fn zeke_interrupt_handler_get(void);

/* Execute a sleeping wait loop on the specified connection.
   If I/O is available during this loop it is processed and
   the zookeeper client called appropriately which will dispatch
   async operations as they complete.

   @timeout@ is the total maximum time in microseconds that
   the loop is allowed to run, when this expires APR_TIMEUP
   is returned, otherwise @timeout@ will contain (if not NULL)
   the number of microseconds remaining before timeout would
   occur.

   @reconnect_attempts@ should be set to NULL or a pointer
   to a variable containing the maximum number of acceptable
   reconnection attempts before the loop aborts and an error
   is returned. If non-NULL, the contents of the unsigned
   integer pointed to will be decremented on each reconnect.
   If NULL zeke_io_loop() will attempt reconnection indefinitely.
*/
ZEKE_EXPORT zk_status_t zeke_io_loop(zeke_connection_t*,
                                     apr_interval_time_t *timeout,
                                     unsigned *reconnect_attempts);

/* Identical to zeke_io_loop() except that only @numloops@
   loops will be run. Once @runloops@ have occurred, zeke_io_loopn()
   will return APR_SUCCESS.

   If no error is returned zeke_io_loopn() gaurauntees that upon
   return that all pending zookeeper I/O has been performed.
*/
ZEKE_EXPORT zk_status_t zeke_io_loopn(zeke_connection_t*,
                                      apr_uint32_t numloops,
                                      apr_interval_time_t *timeout,
                                      unsigned *reconnect_attempts);

/* If libevent support is enabled */
#ifdef LIBZEKE_USE_LIBEVENT
ZEKE_EXPORT zk_status_t zeke_event_loop(zeke_connection_t*,
                                        struct event_base*,
                                        apr_interval_time_t *timeout,
                                        unsigned *reconnect_attempts);
ZEKE_EXPORT zk_status_t zeke_event_loopn(zeke_connection_t*,
                                         struct event_base*,
                                         apr_uint32_t numloops,
                                         apr_interval_time_t *timeout,
                                         unsigned *reconnect_attempts);
#endif /* LIBZEKE_USE_LIBEVENT */

/* Public high-level event loop API */

/* Runloop flags */
enum zeke_runloop_flag_e {
  /* Don't place the startup() callback until the connection has started */
  zeke_runloop_delay_startup_callback = (1 << 0)
#define ZEKE_RUNLOOP_DELAY_STARTUP_CALLBACK zeke_runloop_delay_startup_callback
};

/* all runloop (high-level) api callback take the following form: */
typedef void (*zeke_runloop_callback_fn)(zeke_connection_t*,void *);

/* Application helper -- runs a main application loop that sleeps waiting
   for i/o, calling libzooker as appropriate which will then dispatch callbacks
   for various events.

   The loop will run for a maximum of @max_run_time@ microseconds, or
   until error/close if @max_run_time@ is -1. The loop can be interacted
   with in one of four ways (outside of dispatch callbacks):

   1. @startup@, if not NULL this will be called after the connection has
   been created but before any i/o has been performed. At this stage it
   is safe to call zeke_connection_watcher_add() or
   zeke_connection_named_watcher_add() to add a dispatched watcher function
   on the entire connection. This watcher will be called back for session
   events and is a good way for an application to determine when zookeeper
   is fully available for async operations.

   2. @shutdown@, if not NULL this will be called right before the
   connection is finalized. Note that the connection MAY be in an unusable
   state at this point (if an error has occurred, etc).

   3. @timeout@, if not NULL this will be called if @max_run_time@ is
   exceeded. In this case the timeout behavior is slightly altered:
   It is left up to the @timeout@ callback to close or shutdown the
   connection and __if this is not performed, the timeout is reset
   to the original timeout value and @timeout@ will be called again
   in @max_run_time@ microseconds. If not @timeout@ is passed,
   @max_run_time@ is strictly enforced.

   4. @interval@, if not NULL this will be called every @interval_time@
   microseconds during the runloop.
*/
ZEKE_EXPORT zk_status_t zeke_app_run_loop_ex(const char *zkhosts,
                                           apr_interval_time_t max_run_time,
                                           apr_interval_time_t interval_time,
                                           zeke_runloop_callback_fn interval,
                                           zeke_runloop_callback_fn timeout,
                                           zeke_runloop_callback_fn startup,
                                           zeke_runloop_callback_fn shutdown,
                                           apr_uint32_t flags,
                                           void *app_data,
                                           apr_pool_t *root_pool);

/* This is just a simplified version of zeke_app_run_loop_ex() which assumes
 no maximum run time, no interval timer and a single application callback
 for setup. */
ZEKE_EXPORT zk_status_t zeke_app_run_loop(const char *zkhosts,
                                          zeke_runloop_callback_fn setup,
                                          apr_uint32_t flags,
                                          void *app_data);

/* A simplified version of zeke_app_run_loop_ex() which provides a maximum run time
   timeout but no interval timer, shutdown or timeout callbacks. When the timeout
   is reached, APR_ETIME is returned. */
#define zeke_app_run_loop_timeout(zkhosts,max_run_time,setup,flags,app_data) \
        zeke_app_run_loop_ex( (zkhosts), (max_run_time), -1, NULL, NULL, (setup), NULL, \
                              (flags), (app_data), NULL )

/* Another simplified interface which assumes infinite run time but provides
for only a startup callback and an interval timed callback */
ZEKE_EXPORT zk_status_t zeke_app_run_loop_interval(const char *zkhosts,
                                                   apr_interval_time_t interval_time,
                                                   zeke_runloop_callback_fn interval,
                                                   zeke_runloop_callback_fn startup,
                                                   apr_uint32_t flags,
                                                   void *app_data);

#ifdef LIBZEKE_USE_LIBEVENT

/****************************************************************************/
/* libevent-based event loop APIs
 *
 * There are two libevent-based apis for managing event loops, a low-level
 * API and a high-level API.
 *
 * The low-level API is intended for apps which need to run their own event
 * loops directly via libevent's calls such as event_base_loop(). In this
 * case one must call zeke_io_event_loop_create() and pass it an event_base
 * OR let it create one on it's own and retrieve it via
 * zeke_io_event_loop_base_get(). This event_base should then be looped
 * via one of the libevent api calls. To break out of a loop in a fashion
 * which will allow libzeke to track state, use zeke_io_event_loopbreak()
 * or zeke_io_event_loopexit(). These can be tested for using
 * zeke_io_event_loop_gotbreak() and zeke_io_event_loop_gotexit().
 *
 * NOTE: These calls are thread safe but only ONE zeke_io_event_t opaque
 * handle should be created per thread! If more than a single such event
 * loop is created per-thread behavior becomes undefined.
 */
typedef struct io_event_state *zeke_io_event_t;
typedef const struct io_event_state *zeke_io_event_handle_t;

/* Create a new low-level event loop, returning an opaque handle to
 * later reference the loop. Only a single handle should be created
 * per thread and only one thread should run a libevent event loop
 * on the associated event_base.
 *
 * If pool is provided, a subpool will be created for the entirety
 * of all future event loop operations -- this means the pool MUST
 * be longer lived then any possible associated libevent operations.
 * NULL may be passed in which case an unmanaged global pool will
 * be used as the parent pool.
 *
 * If an event_base is provided, the event_base will be associated
 * with the new pool but not "owned" by it. This is NOT recommended.
 * Intead, pass NULL and a new event_base will be created that is
 * "owned" by the event loop pool such that when the pool is destroyed
 * all events will be removed and the event_base will be freed.
 * NEVER call event_base_free() on such an event_base.
 */
ZEKE_EXPORT zeke_io_event_t
  zeke_io_event_loop_create_ex(apr_pool_t *pool, struct event_base *base,
                               const char *tagfmt, ...)
  __attribute__((format (printf, 3, 4)));
ZEKE_EXPORT zeke_io_event_t
  zeke_io_event_loop_create(apr_pool_t *pool, struct event_base *base);

/* Convenience macro to create a new event loop handle and tag it */
#define zeke_io_event_loop_make(p) zeke_io_event_loop_create((p),NULL)
#define zeke_io_event_loop_new_tag(tag) zeke_io_event_loop_create_ex(NULL,NULL,"%s",(tag))
#define zeke_io_event_loop_new_ptagf(fmt, ...) zeke_io_event_loop_create_ex(NULL,NULL, \
                                                                              (fmt), ## __VA_ARGS__)

/* Returns the pool associated with an io_event loop */
ZEKE_EXPORT apr_pool_t *zeke_io_event_loop_pool_get(zeke_io_event_handle_t);

/* Return the event_base associated with the opaque event loop handle.
 * NEVER call event_base_free() on this base unless you KNOW it is no
 * longer associated with a zeke_io_event_t handle and was not created
 * by zeke_io_event_loop_create().
 */
ZEKE_EXPORT struct event_base *zeke_io_event_loop_base_get(zeke_io_event_handle_t);

/* zeke_io_event_loopbreak() calls event_base_loopbreak() but makes the event
 * loop handle checkable via zeke_io_event_loop_gotbreak(). The io_event
 * handle used will be the one assigned to the current thread.
 */
ZEKE_EXPORT void zeke_io_event_loopbreak(void);
/* The second form of loopbreak() is used to break out of a *specific*
 * io event loop rather than the one assigned to the current thread.
 */
ZEKE_EXPORT void zeke_io_event_loopbreak_ex(zeke_io_event_t);
/* zeke_io_event_loopexit() calls event_base_loopexit() but records an integer
 * exit code which is accessible via zeke_event_loop_gotexit(). The zeke io
 * event
 */
ZEKE_EXPORT void zeke_io_event_loopexit(int code);
/* The second form of loopexit() is used to exit from a *specific*
 * io event loop rather than the one assigned to the current thread.
 */
ZEKE_EXPORT void zeke_io_event_loopexit_ex(zeke_io_event_t, int code);

/* zeke_io_event_loop_gotbreak() and zeke_io_event_loop_gotexit() both perform
 * _destructive_ reads and will only return true one time. They each
 * reset internal respective state after being called.
 *
 * The optional second argument to zeke_io_event_loop_gotexit() will be filled
 * in with the exit code baseed to zeke_io_event_loopexit() if it is
 * not NULL.
 */
ZEKE_EXPORT int zeke_io_event_loop_gotbreak(zeke_io_event_t);
ZEKE_EXPORT int zeke_io_event_loop_gotexit(zeke_io_event_t, int *code);

/* Destroy a libevent-api low-level io eventloop handle. This will
 * also destroy an associated libevent event_base if one was created
 * during the call to zeke_io_event_loopcreate(), removing all pending
 * event structures first.
 */
ZEKE_EXPORT void zeke_io_event_loop_destroy(zeke_io_event_t);

/* Sets the status code associated with an io_event loop handle into
 * the first argument.
 */
ZEKE_EXPORT apr_status_t zeke_io_event_loop_status_get(zeke_io_event_handle_t,
                                                       zk_status_t*);

/* Initiate connection via a low-level event handle created with
 * zeke_io_event_loop_new()/zeke_io_event_loop_create()/etc. Events will be
 * added to the event_base established or passed to these calls, and these
 * events will initiate the actual connection as soon as the event loop starts.
 * The third arg, `setup`, will be called if non-NULL once the connection is
 * fully established (and will be passed app_data).  Callers interested in
 * timeout/timer handling will need to implement this themselves using libevent
 * and zeke_io_event_loopbreak() or zeke_io_event_loopexit().  Applications
 * requiring a shutdown callback should use the setup callback to hook the
 * specific connection structure (zeke_connection_hook_closed(), etc).  The
 * global hook zeke_hook_shutdown() may also be of use here.
 *
 * NOTE: It is the caller's responsibility to manage the event loop including
 * calling zeke_io_event_loop_gotexit() and zeke_io_event_loop_gotbreak() if
 * the loop should terminate.
 *
 * Addl NOTE: Flags is reserved for future use and should always be 0 at this
 * time.
 */
ZEKE_EXPORT zk_status_t zeke_io_event_loop_connect(zeke_io_event_t handle,
                                                   const char *zkhosts,
                                                   zeke_runloop_callback_fn setup,
                                                   apr_uint32_t flags,
                                                   void *app_data);
/* zeke_io_event_loop_restart() should be called after event_base_loop() or
 * event_base_disatch() has returned and the event loop will be restarted.
 * This should NOT be called if a libevent loop function has not yet been
 * called.
 */
ZEKE_EXPORT zk_status_t zeke_io_event_loop_restart(zeke_io_event_t);

/****************************************************************************/
/* libevent-based high-level API. These functions perform almost identically
 * to the non-libevent API zeke_app_run_loop_ex() and friend. The only
 * significant difference is that they do not support interval callbacks
 * directly. If such are needed, use libevent to add a timeout event
 * (possibly with EV_PERSIST set) to the event base before calling
 * one of these functions. These calls ALL use event_base_loop() under the
 * hook so it's quite safe to pass them an event_base that is pre-configured
 * as necessary for an application.
 *
 * NOTE: NEVER EVER EVER mix calls between the low-level API above and
 * the high-level API.
 */
ZEKE_EXPORT zk_status_t zeke_app_event_loop_ex(const char *zkhosts,
                                               struct event_base *base,
                                               apr_interval_time_t max_run_time,
                                               zeke_runloop_callback_fn timeout,
                                               zeke_runloop_callback_fn startup,
                                               zeke_runloop_callback_fn shutdown,
                                               apr_uint32_t flags,
                                               void *app_data,
                                               apr_pool_t *root_pool);
ZEKE_EXPORT zk_status_t zeke_app_event_loop(const char *zkhosts,
                                            struct event_base *base,
                                            zeke_runloop_callback_fn startup,
                                            apr_uint32_t flags,
                                            void *app_data);
#endif /* LIBZEKE_USE_LIBEVENT */

#ifdef __cplusplus
}
#endif

#endif /* _LIBZEKE_EVENT_LOOP_H */
