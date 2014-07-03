/* apr_table hash index abstraction */
#include "tools.h"

#ifndef ZKTOOL_INDEX_POOL_KEY_PREFIX
#define ZKTOOL_INDEX_POOL_KEY_PREFIX "zktool:hash:index:"
#endif

#ifndef ZKTOOL_INDEX_KEY_MAXLEN
#define ZKTOOL_INDEX_KEY_MAXLEN APR_ALIGN_DEFAULT(256-18)
#endif

static int table_index_set_val(void *hp, const char *key, const char *val)
{
  assert(hp != NULL);
  apr_hash_set((apr_hash_t*)hp,key,strlen(key),val);
  return 1;
}

static void table_index_update(apr_hash_t *h, apr_table_t *t)
{
  apr_hash_clear(h);
  apr_table_do(table_index_set_val,h,t,NULL);
}

static apr_hash_t *table_index_get(apr_table_t *t, const char *name)
{
  char namebuf[ZKTOOL_INDEX_KEY_MAXLEN+18];
  const apr_array_header_t *elts;
  apr_status_t st;
  const char *key;
  apr_pool_t *p;
  apr_hash_t *h = NULL;
  assert(t != NULL);

  elts = apr_table_elts(t);
  assert(elts != NULL);
  p = elts->pool;
  assert(p != NULL);
  if(name) {
    char *cp = apr_cpystrn(namebuf,ZKTOOL_INDEX_POOL_KEY_PREFIX,sizeof(namebuf));
    assert(cp != NULL);
    apr_cpystrn(cp,name,ZKTOOL_INDEX_KEY_MAXLEN);
    key = namebuf;
  } else key = ZKTOOL_INDEX_POOL_KEY_PREFIX;

  st = apr_pool_userdata_get((void**)&h,key,p);
  if(st != APR_SUCCESS)
    h = NULL;
  return h;
}

static void table_index_set(apr_pool_t *p, const char *name, apr_hash_t *h)
{
  if(name) {
    char namebuf[ZKTOOL_INDEX_KEY_MAXLEN+18];
    char *cp = apr_cpystrn(namebuf,ZKTOOL_INDEX_POOL_KEY_PREFIX,sizeof(namebuf));
    assert(cp != NULL);
    apr_cpystrn(cp,name,ZKTOOL_INDEX_KEY_MAXLEN);
    apr_pool_userdata_set(h,namebuf,NULL,p);
  } else
    apr_pool_userdata_setn(h,ZKTOOL_INDEX_POOL_KEY_PREFIX,NULL,p);
}

ZEKE_HIDDEN
apr_hash_t *zktool_table_index_get_ex(apr_table_t *t, const char *name)
{
  const apr_array_header_t *elts;
  apr_hash_t *h;

  assert(t != NULL);
  elts = apr_table_elts(t);
  assert(elts != NULL);

  h = table_index_get(t,name);
  if(!h) {
    assert(elts->pool != NULL);
    h = apr_hash_make(elts->pool);
    table_index_set(elts->pool,name,h);
  }

  if(apr_hash_count(h) != elts->nelts)
    table_index_update(h,t);
  return h;
}

ZEKE_HIDDEN
unsigned int zktool_table_index_update_ex(apr_table_t *t, const char *name)
{
  apr_hash_t *h = table_index_get(t,name);
  if(h) {
    table_index_update(h,t);
    return apr_hash_count(h);
  }
  return 0;
}
