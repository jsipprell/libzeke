#ifndef _ZEKE_UTIL_H
#define _ZEKE_UTIL_H

#include "internal.h"

#define LIBZEKE_RUNLOOP_DELAYED_STARTUP_KEY "libzeke_runloop_delayed_startup_key"

typedef struct {
  zeke_runloop_callback_fn callback;
  void *data;
  const char *registered_name;
} zeke_delayed_startup_t;

ZEKE_EXTERN void delayed_startup(const zeke_watcher_cb_data_t*);

#endif /* _ZEKE_UTIL_H */

