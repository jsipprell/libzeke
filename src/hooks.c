#include "internal.h"

#include <apr_hooks.h>
#include "hooks.h"

#define APR_WANT_MEMFUNC
#define APR_WANT_STRFUNC
#include <apr_want.h>

#define ZEKE_HOOK_USERDATA_KEY "libzeke:hooks_userdata"

#if 0
ZEKE_STATIC_GLOBAL_HOOK_PROTO(new_connection);
ZEKE_STATIC_GLOBAL_HOOK_PROTO(shutdown);
#endif

APR_HOOK_STRUCT(
  APR_HOOK_LINK(new_connection)
  APR_HOOK_LINK(shutdown)
#ifdef LIBZEKE_USE_LIBEVENT
  APR_HOOK_LINK(event_loop_begin)
  APR_HOOK_LINK(event_loop_end)
#endif
)

static int global_hooks_sorted = 0;

typedef struct
{
  void (*dummy)(void*);
  const char *name;
  const char * const *preds;
  const char * const *succs;
  int norder;
} tsort_data_t;

typedef struct tsort_ {
  void *data;
  int npreds;
  struct tsort_ **pp_preds;
  struct tsort_ *p_next;
} tsort_t;

static int crude_order(const void *a, const void *b)
{
  return ((const tsort_data_t*)a)->norder - ((const tsort_data_t*)b)->norder;
}

static tsort_t *prepare(apr_pool_t *p, tsort_data_t *items, int nitems)
{
  int n;
  tsort_t *pdata = apr_palloc(p,nitems * sizeof(*pdata));
  assert(pdata != NULL);

  qsort(items,nitems,sizeof(tsort_data_t),&crude_order);
  for(n = 0; n < nitems; ++n) {
    pdata[n].npreds = 0;
    pdata[n].pp_preds = apr_pcalloc(p,nitems * sizeof(*pdata[n].pp_preds));
    pdata[n].p_next = NULL;
    pdata[n].data = &items[n];
  }

  for(n = 0; n < nitems; ++n) {
    int i,k;

    for(i=0; items[n].preds && items[n].preds[i]; ++i)
      for(k=0; k < nitems; ++k) {
        if(!strcmp(items[k].name,items[n].preds[i])) {
          int l;

          for(l=0; l < pdata[n].npreds; ++l)
            if(pdata[n].pp_preds[l] == &pdata[k])
              goto got_it;
            pdata[n].pp_preds[pdata[n].npreds] = &pdata[k];
            pdata[n].npreds++;
          got_it:
            break;
        }
      }
    for(i=0; items[n].succs && items[n].succs[i]; ++i)
      for(k=0; k < nitems; ++k) {
        if(!strcmp(items[k].name,items[n].succs[i])) {
          int l;

          for(l=0; l < pdata[k].npreds; ++l)
            if(pdata[k].pp_preds[l] == &pdata[n])
              goto got_it2;
          pdata[k].pp_preds[pdata[k].npreds] = &pdata[n];
          pdata[k].npreds++;
        got_it2:
          break;
        }
      }
  }

  return pdata;
}

static tsort_t *tsort(tsort_t *pdata, int nitems)
{
  int total;
  tsort_t *head = NULL;
  tsort_t *tail = NULL;

  for(total = 0; total < nitems; ++total) {
    int n,i,k;

    for(n=0;;++n) {
      assert(n != nitems);
      if(!pdata[n].p_next) {
        if(pdata[n].npreds) {
          for(k=0;;++k) {
            assert(k < nitems);
            if(pdata[n].pp_preds[k])
              break;
          }
          for(i=0;;++i) {
            assert(i < nitems);
            if(&pdata[i] == pdata[n].pp_preds[k]) {
              n = i-1;
              break;
            }
          }
        } else break;
      }
    }

    if(tail)
      tail->p_next = &pdata[n];
    else
      head = &pdata[n];
    tail = &pdata[n];
    tail->p_next = tail;
    for(i = 0; i < nitems; ++i)
      for(k = 0; k < nitems; ++k)
        if(pdata[i].pp_preds[k] == &pdata[n]) {
          --pdata[i].npreds;
          pdata[i].pp_preds[k] = NULL;
          break;
        }
  }
  tail->p_next = NULL;
  return head;
}

static apr_array_header_t *sort_hook(apr_array_header_t *hooks, const char *name)
{
  apr_pool_t *p = NULL;
  tsort_t *sort = NULL;
  char *msg = NULL;
  apr_array_header_t *new = NULL;
  int n;

  if(apr_hook_global_pool == NULL) {
    apr_hook_global_pool = zeke_root_subpool_create();
    assert(apr_hook_global_pool != NULL);
    apr_pool_cleanup_register(apr_hook_global_pool,&apr_hook_global_pool,
                              zk_indirect_wipe,apr_pool_cleanup_null);
  }
  apr_pool_create(&p, apr_hook_global_pool);
  assert(p != NULL);
  assert(hooks->elt_size == sizeof(tsort_data_t));
  sort = prepare(p, (tsort_data_t*)hooks->elts, hooks->nelts);
  sort = tsort(sort,hooks->nelts);
  new = apr_array_make(hooks->pool,hooks->nelts,sizeof(tsort_data_t));
#if 1
  if(apr_hook_debug_enabled)
#endif
    msg = apr_psprintf(p,"libzeke sorting zeke hook: %s",name);
  for(n = 0; sort; sort = sort->p_next) {
    tsort_data_t *hook;
    assert(n < hooks->nelts);
    hook = apr_array_push(new);
    memcpy(hook,sort->data,sizeof(tsort_data_t));
    if(msg)
      msg = apr_pstrcat(p,msg," ",hook->name,NULL);
  }
  if(msg)
    INFO("%s",msg);
  apr_pool_destroy(p);

  return new;
}

typedef struct {
  const char *hook_name;
  apr_array_header_t **hooks;
} hook_sort_entry_t;

typedef struct {
  const void *key;
  char namespace[32];
} hook_hash_key_t;

static apr_hash_t *hook_hash_table(apr_pool_t *pool)
{
  ZEKE_MAY_ALIAS(apr_hash_t) *h = NULL;
  assert(pool != NULL);
  if(apr_pool_userdata_get((void**)&h,ZEKE_HOOK_USERDATA_KEY,pool) != APR_SUCCESS || h == NULL) {
    h = apr_hash_make(pool);
    assert(apr_pool_userdata_setn((const void*)h, ZEKE_HOOK_USERDATA_KEY,NULL,pool) == APR_SUCCESS);
  }
  return h;
}

static void hook_hash_table_delete(apr_pool_t *pool)
{
  ZEKE_MAY_ALIAS(apr_hash_t) *h = NULL;
  assert(pool != NULL);
  if(apr_pool_userdata_get((void**)&h,ZEKE_HOOK_USERDATA_KEY,pool) == APR_SUCCESS && h != NULL)
#ifdef USING_APR_12
  {
    h = apr_hash_make(pool);
    assert(apr_pool_userdata_setn((const void*)h, ZEKE_HOOK_USERDATA_KEY,NULL,pool) == APR_SUCCESS);
  }
#else /* >APR 1.2 */
    apr_hash_clear(h);
#endif /* USING_APR_12 */
}

ZEKE_API(void) zeke_hook_sort_register(apr_pool_t *pool, const void *key,
                                       const char *name, apr_array_header_t **hooks)
{
  hook_hash_key_t hkey = { key, "" };
  hook_sort_entry_t *entry;
  apr_hash_t *h = hook_hash_table(pool);

  memset(hkey.namespace,0,sizeof(hkey.namespace));
  apr_cpystrn(hkey.namespace,name,sizeof(hkey.namespace));

  assert(h != NULL);
  if((entry = apr_hash_get(h,&hkey,sizeof(hook_hash_key_t))) == NULL) {
    hook_hash_key_t *hk = apr_pmemdup(pool,&hkey,sizeof(hook_hash_key_t));
    entry = apr_palloc(pool,sizeof(hook_sort_entry_t));
    entry->hook_name = name;
    entry->hooks = hooks;
    apr_hash_set(h,hk,sizeof(hook_hash_key_t),(const void*)entry);
  }
}

static void zk_hook_sort(apr_pool_t *pool, const void *key, const char *name)
{
  apr_pool_t *p;
  apr_hash_t *h = hook_hash_table(pool);
  apr_hash_index_t *hi = NULL;
  ZEKE_MAY_ALIAS(hook_sort_entry_t) *entry;
  ZEKE_MAY_ALIAS(const hook_hash_key_t) *hkey;
  apr_ssize_t klen;

  assert(apr_pool_create(&p,pool) == APR_SUCCESS);
  assert(h != NULL);
  for(hi = apr_hash_first(p,h); hi; hi = apr_hash_next(hi)) {
    apr_hash_this(hi,(const void**)&hkey,&klen,(void**)&entry);
    assert(hkey != NULL && hkey->key != NULL && hkey->namespace != NULL);
    if(key == hkey->key && (!name || name == hkey->namespace || strcmp(name,hkey->namespace) == 0))
      *entry->hooks = sort_hook(*entry->hooks,entry->hook_name);
  }

  apr_pool_destroy(p);
}

ZEKE_API(void) zeke_hook_sort(apr_pool_t *pool, const void *key, const char *name)
{
  apr_hash_t *h = hook_hash_table(pool);
  ZEKE_MAY_ALIAS(hook_sort_entry_t) *entry;
  hook_hash_key_t hkey = { key, "" };

  memset(hkey.namespace,0,sizeof(hkey.namespace));
  apr_cpystrn(hkey.namespace,name,sizeof(hkey.namespace));

  if(!name) {
    zk_hook_sort(pool,key,name);
    return;
  }
  assert(h != NULL);
  entry = apr_hash_get(h,&hkey,sizeof(hook_hash_key_t));
  if(entry)
    *entry->hooks = sort_hook(*entry->hooks,entry->hook_name);
}

ZEKE_API(void) zeke_hook_sort_all(apr_pool_t *pool, const void *key)
{
  if(key != NULL) {
    zk_hook_sort(pool,key,NULL);
  } else {
    apr_pool_t *p;
    apr_hash_t *h = hook_hash_table(pool);
    apr_hash_index_t *hi = NULL;
    ZEKE_MAY_ALIAS(hook_sort_entry_t) *entry;
    ZEKE_MAY_ALIAS(const hook_hash_key_t) *hkey;
    apr_ssize_t klen = sizeof(hkey);

    assert(apr_pool_create(&p,pool) == APR_SUCCESS);
    for(hi = apr_hash_first(p,h); hi; hi = apr_hash_next(hi)) {
      apr_hash_this(hi,(const void**)&hkey,&klen,(void**)&entry);
      assert(entry != NULL);
      *entry->hooks = sort_hook(*entry->hooks,entry->hook_name);
    }
    apr_pool_destroy(p);
  }
}

ZEKE_API(void) zeke_hook_deregister(apr_pool_t *pool, const void *key, const char *name)
{
  hook_hash_key_t hkey = { key, "" };

  if(key && name) {
    memset(hkey.namespace,0,sizeof(hkey.namespace));
    apr_cpystrn(hkey.namespace,name,sizeof(hkey.namespace));
  }

  if(!key) {
    hook_hash_table_delete(pool);
  } else if(!name) {
    apr_pool_t *p;
    apr_hash_t *h = hook_hash_table(pool);
    apr_hash_index_t *hi = NULL;
    ZEKE_MAY_ALIAS(hook_sort_entry_t) *entry;
    ZEKE_MAY_ALIAS(const hook_hash_key_t) *hk;
    apr_ssize_t klen = sizeof(hook_hash_key_t);

    assert(apr_pool_create(&p,pool) == APR_SUCCESS);
    for(hi = apr_hash_first(p,h); hi;) {
      apr_hash_this(hi,(const void**)&hk,&klen,(void**)&entry);
      hi = apr_hash_next(hi);
      apr_hash_set(h,hk,sizeof(hook_hash_key_t),NULL);
    }
    apr_pool_destroy(p);
  } else {
    apr_hash_t *h = hook_hash_table(pool);
    apr_hash_set(h,(const void**)&hkey,sizeof(hkey),NULL);
  }
}

static void dummy_hook_new_connection(zeke_connection_t *c)
{ /* noop */ }
static int dummy_hook_shutdown(int *c)
{ return ZEKE_HOOK_OKAY; }

#ifdef LIBZEKE_USE_LIBEVENT
static void dummy_hook_event_loop(struct event_base *b, apr_pool_t *p)
{ /* noop */ }
#endif

#ifndef ZEKE_DUMMY_HOOK
#define ZEKE_DUMMY_HOOK NULL,NULL,ZEKE_HOOK_REALLY_LAST+10
#endif

ZEKE_PRIVATE(void) zeke_global_hooks_init(void)
{
  if(apr_hook_global_pool == NULL) {
    apr_hook_global_pool = zeke_root_subpool_create();
    assert(apr_hook_global_pool != NULL);
    global_hooks_sorted = 0;
    zeke_hook_new_connection(dummy_hook_new_connection,ZEKE_DUMMY_HOOK);
    zeke_hook_shutdown(dummy_hook_shutdown,ZEKE_DUMMY_HOOK);
#ifdef LIBZEKE_USE_LIBEVENT
    zeke_hook_event_loop_begin(dummy_hook_event_loop,ZEKE_DUMMY_HOOK);
    zeke_hook_event_loop_end(dummy_hook_event_loop,ZEKE_DUMMY_HOOK);
#endif /* LIBZEKE_USE_LIBEVENT */
  }
}

ZEKE_API(int) zeke_global_hooks_are_modifiable(void)
{
  if(apr_hook_global_pool != NULL) {
    global_hooks_sorted = 0;
    return 1;
  }
  return 0;
}

ZEKE_API(void) zeke_global_hooks_modified(void)
{
  global_hooks_sorted = 0;
}

ZEKE_API(void) zeke_global_hooks_sort(int resort)
{
  zeke_global_hooks_init();
  if(resort || !global_hooks_sorted) {
    apr_hook_sort_all();
    global_hooks_sorted++;
  }
}

#define ZEKE_API_DECLARE ZEKE_API
#define ZEKE_PRIVATE_DECLARE ZEKE_PRIVATE
ZEKE_IMPLEMENT_EXTERNAL_GLOBAL_HOOK_VOID(new_connection,
                                  (zeke_connection_t *conn),
                                  (conn))
ZEKE_IMPLEMENT_EXTERNAL_GLOBAL_HOOK_RUN_ALL(int,shutdown,
                                   (int *exit_code),(exit_code),
                                   ZEKE_HOOK_OKAY,ZEKE_HOOK_DECLINE)

#ifdef LIBZEKE_USE_LIBEVENT
ZEKE_IMPLEMENT_EXTERNAL_GLOBAL_HOOK_VOID(event_loop_begin,
                                  (struct event_base *base,apr_pool_t *tmp),
                                  (base,tmp))
ZEKE_IMPLEMENT_EXTERNAL_GLOBAL_HOOK_VOID(event_loop_end,
                                  (struct event_base *base,apr_pool_t *tmp),
                                  (base,tmp))
#endif /* LIBZEKE_USE_LIBEVENT */
