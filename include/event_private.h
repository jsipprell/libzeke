#ifndef _ZEKE_EVENT_PRIVATE_H
#define _ZEKE_EVENT_PRIVATE_H

#include "internal.h"
#include "libzeke_event.h"

#ifdef LIBZEKE_USE_LIBEVENT

ZEKE_EXTERN void zeke_io_event_reconnect(struct io_event_state *state);

ZEKE_EXTERN int zeke_generic_event_pending(const struct event *ev,
                                           short what,
                                           apr_interval_time_t *expires);
ZEKE_EXTERN int zeke_generic_event_info_get(struct event *ev,
                                            apr_os_sock_t *fd,
                                            apr_uint32_t *flags,
                                            short *persist,
                                            short *signal,
                                            apr_interval_time_t *remaining);
ZEKE_EXTERN apr_status_t zeke_generic_event_type_set(struct event*,
                                                     short what);
ZEKE_EXTERN apr_status_t zeke_generic_event_callback_set(struct event*,
                                                         event_callback_fn fn,
                                                         void *arg);
ZEKE_EXTERN apr_status_t zeke_generic_event_add(struct event*,
                                                apr_interval_time_t timeout);

#endif /* LIBZEKE_USE_LIBEVENT */
#endif /* _ZEKE_EVENT_PRIVATE_H */
