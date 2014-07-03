#include "libzeke.h"
#include "libzeke_event_loop.h"
#include "libzeke_nodedir.h"
#include "libzeke_errno.h"
#include "libzeke_regex.h"
#include "libzeke_trans.h"
#include "libzeke_util.h"

#include "tools.h"

#ifndef ZKELECTION_DEFAULT_TIMEOUT
#define ZKELECTION_DEFAULT_TIMEOUT 30
#endif /* ZKELECTION_DEFAULT_TIMEOUT */

#ifndef ZKELECTION_REGEX
#define ZKELECTION_REGEX "^([0-9a-z]+)-([0-9a-z._:]+)-n_(\\d{8,})$"
# ifndef ZKELECTION_REGEX_NAME_GROUP
# define ZKELECTION_REGEX_NAME_GROUP 2
# endif
#endif

#define ZKELECTION_SESSION_PREFIX "zkelection:"
#define ZKELECTION_SESSION_KEY(ctx) ((ctx)->session_key ? (ctx)->session_key : \
  ((ctx)->session_key = apr_pstrcat((ctx)->pool,ZKELECTION_SESSION_PREFIX,ctx->path,NULL)))
#define ZKELECTION_FIND_SESSION_PREFIX "zkelection_find:"
#define ZKELECTION_FIND_SESSION_KEY(ctx) ((ctx)->session_key ? (ctx)->session_key : \
  ((ctx)->session_key = apr_pstrcat((ctx)->pool,ZKELECTION_FIND_SESSION_PREFIX,ctx->path,NULL)))
#define ZKELECTION_NOTE_PATHS "zkelection_clean_paths_array"

#define ZKE_TO_ERROR(s) ((s) & 0xff ? (s) & 0xff : ((s) >> 8) | 1)

#define DECLARE_CONTEXT(type) \
  typedef const type const_context_##type; \
  static inline type *context_to_##type ( const zeke_context_t *ctx ) { \
    return ( type * ) ctx; \
  } \
  static inline const_context_##type *Context_to_##type ( const zeke_context_t *ctx ) { \
    return ( const_context_##type * ) ctx; \
  }

typedef enum {
  zkelection_list = (1 << 0),
#define ZKELECTION_OPTION_LIST zkelection_list
  zkelection_clean = (1 << 1),
#define ZKELECTION_OPTION_CLEAN zkelection_clean
  zkelection_find = (1 << 2),
#define ZKELECTION_OPTION_FIND zkelection_find
  zkelection_logfile,
#define ZKELECTION_OPTION_LOGFILE zkelection_logfile
  zkelection_zkhosts,
#define ZKELECTION_OPTION_ZKHOSTS zkelection_zkhosts
  zkelection_debug = (1 << 17),
#define ZKELECTION_OPTION_DEBUG zkelection_debug
  zkelection_ns = (1 << 18)
#define ZKELECTION_OPTION_NS zkelection_ns
} zkelection_e;

#define ZKELECTION_REQUIRE_ARG (zkelection_clean|zkelection_list|zkelection_find)

#define ISOPTCH(c) ((c) < 0xff && apr_isalnum( (c) ))

typedef enum {
  zkelection_timer_max_run = 0
#define ZKELECTION_TIMER_MAX_RUN zkelection_timer_max_run
} zkelection_timer_e;
#define ZKELECTION_NTIMERS 1
#define zkelection_reset_timers(elec,index,value) \
        zktool_reset_timers((elec)->timers,(elec)->waits,ZKELECTION_NTIMERS,(index),(value))
#define zkelection_update_timers(elec,shortest,updates,max_updates) \
        zktool_update_timers((elec)->timers,(elec)->waits,ZKELECTION_NTIMERS,(shortest),(updates),(max_updates))
#define zkelection_update_timeouts(elec,index,shortest) \
        zktool_update_timeouts((elec)->timers,(elec)->waits,ZKELECTION_NTIMERS,(index),(shortest))

typedef enum {
  filter_all = 1,    /* all expressions must match */
  filter_any,     /* any expressions may match */
  filter_return_matches = (1 << 8)
} zeke_array_filter_how_e;

typedef struct {
  apr_pool_t *pool;
  zkelection_e op;
  const char *path;
  const char *session_key;
  apr_array_header_t *filters;
  zeke_array_filter_how_e filter_how;
  apr_time_t start_end,end_time;
  zktool_timer_t timers[ZKELECTION_NTIMERS];
  zktool_wait_t waits[ZKELECTION_NTIMERS];
  const zeke_connection_t *conn;
  apr_array_header_t *args;
} zkelection_t;

typedef struct {
  apr_pool_t *pool;
  zkelection_t *election;
  apr_array_header_t *results;
  apr_array_header_t *election_filter;
  zeke_array_filter_how_e election_filter_how;
  const zeke_connection_t *conn;
  apr_array_header_t *stack;
  const char *root,*cur_node;
} zkelection_find_t;

DECLARE_CONTEXT(zkelection_t);
DECLARE_CONTEXT(zkelection_find_t);

#define OPT_NS zkelection_ns
#define OPT_DEBUG zkelection_debug
#define OPT_LOGFILE zkelection_logfile
static apr_getopt_option_t options[] = {
 { "list",                'l',      0,
  "display the children of an election node in descending order"},
 { "clean",               'c',      0,
  "clean an election (remove dup nodes)"},
 { "find",                'f',      0,
  "find election nodes recursively from a starting node"},
 { "filter",              'F',      1,
  "add a regular expression to be applied to all node name matches"},
  { "timeout",            'T',      1,
  "limit maximum time for all operations (ex: 30s, 3m, 1.2h). Default is "APR_STRINGIFY(ZKELECTION_DEFAULT_TIMEOUT)"s"},
 { "connect",             'C',      1,
  "override zookeeper connection string (default is from env $ZOOKEEPER_ENSEMBLE)" },
 { "ns",              OPT_NS,       1,
  "additional namespace to include in path" },
 { "logfile",         OPT_LOGFILE,  1,
  "location of zookeeper logfile (default is stderr)" },
 { "debug",           OPT_DEBUG,    0,
  "enable debug mode" },
 { "help",            'h',          0,
  "display this help" },
 { NULL, 0, 0, NULL }
};
#undef OPT_DEBUG
#undef OPT_NS
#undef OPT_LOGFILE

static void usage(int exitcode) 
{
  if(!exitcode || exitcode >= 100) {
    zeke_printf("%szkelection [options] <command-args>\n",(exitcode ? "usage: " : ""));
    if((exitcode & 0xff) != 0)
      zeke_printf("  perform an operation on or interrogate a zookeeper election node\noptions:\n");
    else
      exitcode >>= 8;
  }

  zktool_display_options(options,(!exitcode || exitcode >= 100 ? zstdout : zstderr),NULL);

  exit(exitcode);
}

static void zeke_shutdown_handler(int sig)
{
  zeke_safe_shutdown(sig);
}

/* filter an arrary against a list of regular expressions. Only those expressions that match
   will be returned.

   if the filter_return_matches flag is set the returned array consists of zeke_regex_match_t
   objects. The returned array is allocated from the same pool as the source.

   If no matches are found or there are no filters, an empty array is returned
*/
static apr_array_header_t *zeke_array_filter(const apr_array_header_t *source,
                                             const apr_array_header_t *filters,
                                             zeke_array_filter_how_e filter_how,
                                             apr_size_t *nfiltered)
{
  const zeke_regex_t *re;
  int i,f;
  apr_array_header_t *arr = NULL;

  if(nfiltered)
    *nfiltered = 0;

  if(!source)
    return NULL;

  if(apr_is_empty_array(filters) && (filter_how & 0x0f) == filter_any) {
    if(nfiltered)
      *nfiltered = source->nelts;
    return apr_array_copy_hdr(source->pool,source);
  }

  arr = apr_array_make(source->pool,source->nelts,sizeof(void*));
  assert(arr != NULL);

  if(apr_is_empty_array(source))
    return arr;

  for(i = 0; i < source->nelts; i++) {
    const char *subject = APR_ARRAY_IDX(source,i,const char*);
    zeke_regex_match_t *match = NULL;

    if(!subject)
      continue;

    for(f = 0; f < filters->nelts; f++) {
      re = APR_ARRAY_IDX(filters,f,const zeke_regex_t*);
      match = zeke_regex_exec(re,subject,NULL);
      if(match && (filter_how & 0xff) == filter_any)
        break;
    }

    if(match != NULL) {
      if(filter_how & filter_return_matches) {
        APR_ARRAY_PUSH(arr,zeke_regex_match_t*) = match;
      } else {
        APR_ARRAY_PUSH(arr,const char*) = subject;
      }
    }
  }

  if(nfiltered)
    *nfiltered = arr->nelts;
  return arr;
}

/* identical to zeke_array_filter, but filters a table's keys. Returned array is NOT
   strings but apr_table_entry_t objects.
*/
static apr_array_header_t *zeke_table_filter(const apr_table_t *table,
                                             const apr_array_header_t *filters,
                                             zeke_array_filter_how_e filter_how,
                                             apr_size_t *nfiltered)
{
  const zeke_regex_t *re;
  int i,f;
  apr_array_header_t *arr = NULL;
  const apr_array_header_t *telts = NULL;
  apr_table_entry_t *te = NULL;

  if(nfiltered)
    *nfiltered = 0;

  if(!table)
    return NULL;

  telts = apr_table_elts(table);
  if(apr_is_empty_array(filters) && (filter_how & 0x0f) == filter_any) {
    if(nfiltered)
      *nfiltered = telts->nelts;
    return apr_array_copy_hdr(telts->pool,telts);
  }
  
  arr = apr_array_make(telts->pool,telts->nelts,telts->elt_size);
  assert(arr != NULL);

  if(apr_is_empty_array(telts))
    return arr;

  te = (apr_table_entry_t*)telts->elts;
  for(i = 0; i < telts->nelts; i++) {
    const char *subject = te[i].key;
    zeke_regex_match_t *match = NULL;

    if(!subject)
      continue;

    for(f = 0; f < filters->nelts; f++) {
      re = APR_ARRAY_IDX(filters,f,const zeke_regex_t*);
      match = zeke_regex_exec(re,subject,NULL);
      if(match && (filter_how & 0xff) == filter_any)
        break;
    }

    if(match != NULL) {
      if(filter_how & filter_return_matches) {
        APR_ARRAY_PUSH(arr,zeke_regex_match_t*) = match;
      } else {
        memcpy(&APR_ARRAY_PUSH(arr,apr_table_entry_t),&te[i],telts->elt_size);
      }
    }
  }

  if(nfiltered)
    *nfiltered = arr->nelts;
  return arr;
}


static zeke_cb_t zkelection_finish_clean(const zeke_callback_data_t *cbd)
{
  zk_status_t st = cbd->status;
  const zeke_transaction_t *trans = (zeke_transaction_t*)cbd->ctx;
  zkelection_t *elec = context_to_zkelection_t(zeke_transaction_context(trans));
  ZEKE_MAY_ALIAS(apr_array_header_t) *paths = NULL;
  int rc = 0;

  assert(elec != NULL);

  apr_pool_userdata_get((void**)&paths,ZKELECTION_NOTE_PATHS,zeke_transaction_pool_get(trans));
  assert(paths != NULL);

  if(!ZEKE_STATUS_IS_OKAY(st)) {
    zeke_apr_eprintf(st,"transaction for %s failed (%u) entries",
                     elec->path,(unsigned)zeke_transaction_num_ops(trans));
    zeke_safe_shutdown(99);
  } else {
    const apr_array_header_t *results;
    int i;
    assert((results = zeke_transaction_results(trans)) != NULL);
    for(i = 0; i < results->nelts; i++) {
      const zoo_op_result_t *res = ZEKE_TRANSACTION_IDX(results,i);
      const char *path = APR_ARRAY_IDX(paths,i,const char*);
      st = ZEKE_FROM_ZK_ERROR(res->err);
      zeke_printf("%s: %s\n",path,
        (ZEKE_STATUS_IS_OKAY(st) ? "deleted" : zeke_perrstr(st,cbd->pool)));
      if(!ZEKE_STATUS_IS_OKAY(st))
        rc++;
    }
  }

  zeke_safe_shutdown(rc);
  return ZEKE_CALLBACK_DESTROY;
}

static int zkelection_start_clean(const zeke_nodedir_t *nodedir, apr_table_t *entries, apr_hash_t *dupes,
                                  zeke_regex_t *regex)
{
  zk_status_t st;
  zkelection_t *elec = context_to_zkelection_t(zeke_nodedir_context(nodedir));
  apr_pool_t *pool = apr_hash_pool_get(dupes);
  zeke_transaction_t *trans = NULL;
  apr_array_header_t *paths = NULL;
  const apr_array_header_t *ta = apr_table_elts(entries);
  apr_table_entry_t *te = (apr_table_entry_t*)ta->elts;
  int count = 0;
  int i;

  for(i = 0; i < ta->nelts; i++) {
    zeke_regex_match_t *match;
    apr_size_t matches;

    match = zeke_regex_exec_ex(regex,te[i].key,&matches,30,pool);
    if(match) {
      apr_uint32_t *ref;
      const char *node_name = zeke_regex_match_group(match,ZKELECTION_REGEX_NAME_GROUP,pool);
      const char *path;

      assert((ref = apr_hash_get(dupes,node_name,APR_HASH_KEY_STRING)) != NULL);
      switch(*ref) {
      case 0:
        /* This is a node name which has been previous marked by the default section
        below and thus must be removed.
        */
        if(trans == NULL) {
          st = zeke_transaction_create_ex(&trans,ta->nelts,elec->pool,0);
          ZEKE_ASSERTV(ZEKE_STATUS_IS_OKAY(st),"clear %s, start trans: %s",
                       elec->path,zeke_errstr(st));
          zeke_transaction_buflen_set(trans,ta->nelts*512);
        }
        if(paths == NULL) {
          paths = apr_array_make(zeke_transaction_pool_get(trans),
                                 ta->nelts,sizeof(const char*));
          assert(paths != NULL);
        }
        path = zeke_nodepath_join(zeke_transaction_pool_get(trans),elec->path,te[i].key,NULL);
        st = zeke_trans_op_delete_any_addn(trans,path);
        ZEKE_ASSERTV(ZEKE_STATUS_IS_OKAY(st),"delete %s/%s: %s",
                       elec->path,te[i].key,zeke_errstr(st));
        APR_ARRAY_PUSH(paths,const char*) = path;
        count++;
        INFO("%d: removing %s",count,path);
        break;
      case 1:
        /* There is *exactly* one node with this node name which makes this entry
           completely valid and without dups. Ignore it.
         */
        LOG("ignoring %s (%s)",node_name,te[i].key);
        break;
      default:
        /* there are invalid election winner nodes, but this must be the
           lowest numeric sequence so it must also be valid. Skip it but mark
           all subsequent entries with the same node_name for removal.
        */
        LOG("marking %s (%s) for any future dups",node_name,te[i].key);
        *ref = 0;
        break;
      }
    }
  }

  if(trans) {
    apr_pool_userdata_setn(paths,ZKELECTION_NOTE_PATHS,NULL,zeke_transaction_pool_get(trans));
    zeke_transaction_context_set(trans,elec);
    st = zeke_transaction_run(trans,elec->conn,zkelection_finish_clean);
    if(!ZEKE_STATUS_IS_OKAY(st)) {
      zeke_apr_eprintf(st,"creating transaction for %s (%d entries)",elec->path,count);
      zeke_transaction_destroy(trans);
      zeke_safe_shutdown(99);
      count = 0;
    }
  }

  return count;
}

static int zkelection_find_iter(const zeke_nodedir_t *nodedir, apr_table_t *entries, zeke_context_t *ctx)
{
  zk_status_t st = zeke_nodedir_status(nodedir);
  zkelection_find_t *find = context_to_zkelection_find_t(ctx);
  const apr_array_header_t *nodes = NULL;
  apr_size_t nnodes = 0;
  
  apr_array_pop(find->stack);
  if(!ZEKE_STATUS_IS_OKAY(st)) {
    zeke_apr_eprintf(st,"node %s",find->cur_node);
    if(!ZEKE_STATUS_CAN_RETRY(st)) {
      apr_array_pop(find->stack);
      if(find->stack->nelts == 0 || !zeke_connection_is_active(find->conn))
        zeke_safe_shutdown(ZKE_TO_ERROR(st));
    } else if(!zeke_connection_is_active(find->conn)) {
      apr_array_pop(find->stack);
      zeke_safe_shutdown(ZKE_TO_ERROR(st));
    } else {
      st = zeke_nodedir_restart((zeke_nodedir_t*)nodedir);
      if(!ZEKE_STATUS_IS_OKAY(st)) {
        zeke_apr_eprintf(st,"fatal %s",find->cur_node);
        zeke_safe_shutdown(ZKE_TO_ERROR(st));
        return 0;
      }
      return 1;
    }
    return 0;
  }

  nodes = zeke_table_filter(entries,find->election_filter,find->election_filter_how,&nnodes);

  if(nnodes > 0) {
    /* match */
    APR_ARRAY_PUSH(find->results,const char*) = find->cur_node;
  } else {
    int i;
    apr_table_entry_t *te;

    /* no match, see if we should decend */
    nodes = apr_table_elts(entries);
    te = (apr_table_entry_t*)nodes->elts;
    for(i = 0; i < nodes->nelts; i++) {
      const char *np = zeke_nodepath_join(find->pool,find->cur_node,te[i].key,NULL);
      APR_ARRAY_PUSH(find->stack,const char*) = np;
      LOG("%d: %s %d\n",find->stack->nelts,np,i);
    }
  }

  /* See if there's work left to do */
  while(find->stack->nelts > 0) {
    zeke_nodedir_t *next = NULL;

    find->cur_node = APR_ARRAY_IDX(find->stack,find->stack->nelts-1,const char*);
    LOG("TOP %d: %s\n",find->stack->nelts-1,find->cur_node);

    st = zeke_nodedir_create(&next,find->cur_node,-1,
                             zkelection_find_iter,NULL,
                             find,find->conn);
    if(!ZEKE_STATUS_IS_OKAY(st)) {
      zeke_apr_error(find->cur_node,st);
      if(next)
        zeke_nodedir_destroy(next);
      apr_array_pop(find->stack);
      continue;
    }
    break;
  }

  if(find->stack->nelts == 0) {
    const apr_array_header_t *results = find->results;
    int i;

    if(find->election->filters)
      results = zeke_array_filter(results,find->election->filters,find->election->filter_how,NULL);
    for(i = 0; i < results->nelts; i++)
      zeke_printf("%s\n",APR_ARRAY_IDX(results,i,const char*));
    
    zeke_safe_shutdown(0);
  }
  
  zeke_nodedir_destroy((zeke_nodedir_t*)nodedir);
  return 0;
}

static int zkelection_complete(const zeke_nodedir_t *nodedir, apr_table_t *entries, zeke_context_t *ctx)
{
  zk_status_t st = zeke_nodedir_status(nodedir);
  zkelection_t *elec = context_to_zkelection_t(ctx);
  zeke_regex_t *regex = NULL;
  apr_table_entry_t *te;
  const apr_array_header_t *ta;
  apr_hash_t *dupes = NULL;
  apr_pool_t *pool = NULL;
  int i,rc = 0,count = 0;

  assert(apr_pool_create(&pool,elec->pool) == APR_SUCCESS);
  zeke_pool_tag(pool,"zkelection operational pool");

  if(!ZEKE_STATUS_IS_OKAY(st)) {
    zeke_apr_eprintf(st,"node %s",elec->path);
    zeke_safe_shutdown(ZKE_TO_ERROR(st));
    return 0;
  }

  st = zeke_regex_create_ex(&regex,ZKELECTION_REGEX,
                            ZEKE_RE_CASELESS|ZEKE_RE_DOLLAR_ENDONLY|
                            ZEKE_RE_UTF8|ZEKE_RE_STUDY,pool);
  ZEKE_ASSERTV(ZEKE_STATUS_IS_OKAY(st),"regex: %s",
        (regex ? zeke_regex_error_get(regex,NULL,NULL) : zeke_errstr(st)));
  
  if(elec->filters)
    ta = zeke_table_filter(entries,elec->filters,elec->filter_how,NULL);
  else
    ta = apr_table_elts(entries);
  te = (apr_table_entry_t*)ta->elts;
  for(i = 0; i < ta->nelts; i++) {
    zeke_regex_match_t *match;
    apr_size_t matches;
    match = zeke_regex_exec_ex(regex,te[i].key,&matches,30,pool);
    if(match) {
      ZEKE_ASSERTV(matches > 0,"regex groups %d for %s",(int)matches,te[i].key);
      count++;
      if(elec->op & ZKELECTION_OPTION_CLEAN) {
        apr_uint32_t *ref;
        const char *node_name = zeke_regex_match_group(match,ZKELECTION_REGEX_NAME_GROUP,pool);

        if(dupes == NULL)
          dupes = apr_hash_make(pool);
        if((ref = apr_hash_get(dupes,node_name,APR_HASH_KEY_STRING)) == NULL)
          assert((ref = apr_pcalloc(pool,sizeof(apr_uint32_t))) != NULL);
        (*ref)++;
        apr_hash_set(dupes,node_name,APR_HASH_KEY_STRING,ref);
      } else
        zeke_printf("%s\n",zeke_nodepath_join(pool,elec->path,te[i].key,NULL));

      zeke_regex_match_destroy(match);
    }
  }

  if(count > 0 && dupes != NULL)
    rc = zkelection_start_clean(nodedir,entries,dupes,regex);

  zeke_regex_destroy(regex);
  apr_pool_destroy(pool);
  zeke_nodedir_destroy((zeke_nodedir_t*)nodedir);
  if (rc <= 0)
    zeke_safe_shutdown(count > 0 ? 0 : 1);
  
  return 0;
}

static void zkelection_find_setup(zeke_connection_t *conn, void *data)
{
  zk_status_t st = ZEOK;
  zeke_regex_t *regex = NULL;
  zeke_nodedir_t *nodedir = NULL;
  zkelection_t *ctx = context_to_zkelection_t(data);
  zkelection_find_t *find = NULL;
  
  if(ctx->conn == NULL) {
    ctx->conn = conn;
    apr_pool_cleanup_register(zeke_connection_pool_get(conn),&ctx->conn,zk_indirect_wipe,
                                                                        apr_pool_cleanup_null);
  }
  if(ctx->session_key == NULL) {
    zkelection_find_t *session = NULL;

    assert(ZKELECTION_FIND_SESSION_KEY(ctx) != NULL);
    session = zeke_session_context_create(conn,ZKELECTION_FIND_SESSION_KEY(ctx),sizeof(zkelection_find_t));
    ZEKE_ASSERTV(session->pool == NULL,"session already exists for %s",ctx->session_key);
    find = session;
  }

  assert(find != NULL);
  find->pool = zeke_session_pool(ctx->conn);
  find->election = ctx;
  find->conn = ctx->conn;
  find->root = apr_pstrdup(find->pool,ctx->path);
  find->stack = apr_array_make(find->pool,100,sizeof(const char*));
  find->cur_node = find->root;
  APR_ARRAY_PUSH(find->stack,const char*) = find->cur_node;
  find->election_filter = apr_array_make(find->pool,1,sizeof(zeke_regex_t*));
  st = zeke_regex_create_ex(&regex,ZKELECTION_REGEX,
                            ZEKE_RE_CASELESS|ZEKE_RE_DOLLAR_ENDONLY|
                            ZEKE_RE_UTF8|ZEKE_RE_STUDY,find->pool);
  assert(ZEKE_STATUS_IS_OKAY(st));
  APR_ARRAY_PUSH(find->election_filter,zeke_regex_t*) = regex;
  find->election_filter_how = filter_all;
  find->results = apr_array_make(find->pool,10,sizeof(const char*));

  st = zeke_nodedir_create(&nodedir,find->cur_node,-1,
                           zkelection_find_iter,NULL,
                           find,conn);
  if(!ZEKE_STATUS_IS_OKAY(st))
    zeke_fatal(find->cur_node,st);
}

static void zkelection_list_setup(zeke_connection_t *conn, void *data)
{
  zk_status_t st = ZEOK;
  zkelection_t *ctx = context_to_zkelection_t(data);
  zeke_nodedir_t *nodedir = NULL;
  if(ctx->conn == NULL) {
    ctx->conn = conn;
    apr_pool_cleanup_register(zeke_connection_pool_get(conn),&ctx->conn,zk_indirect_wipe,
                                                                        apr_pool_cleanup_null);
  }
  if(ctx->session_key == NULL) {
    zkelection_t *session = NULL;

    assert(ZKELECTION_SESSION_KEY(ctx) != NULL);
    session = zeke_session_context_create(conn,ZKELECTION_SESSION_KEY(ctx),sizeof(zkelection_t));
    ZEKE_ASSERTV(session->pool == NULL,"session already exists for %s",ctx->session_key);
    memcpy(session,ctx,sizeof(zkelection_t));
    ctx = session;
  }

  st = zeke_nodedir_create(&nodedir,ctx->path,-1,
                           zkelection_complete,NULL,
                           ctx,conn);
  if(!ZEKE_STATUS_IS_OKAY(st))
    zeke_fatal(ctx->path,st);
}

static void zkelection_interval_timer(zeke_connection_t *conn, void *data)
{
  zkelection_t *ctx = (zkelection_t*)data;
  int expired[ZKELECTION_NTIMERS];
  apr_size_t nexpired;

  if(conn && zeke_session_context_exists(conn,ZKELECTION_SESSION_KEY(ctx)))
    ctx = zeke_session_context(conn,ctx->session_key);

  nexpired = zkelection_update_timers(ctx,NULL,expired,ZKELECTION_NTIMERS);

  if(nexpired > 0) {
    int t;
    apr_size_t i;

    for(i = 0; i < nexpired; i++) {
      switch((t = expired[i])) {
      case ZKELECTION_TIMER_MAX_RUN:
        zeke_safe_shutdown_ex(98,"timeout expired (%s)",
              zktool_interval_format(ZKTOOL_STATIC_TIME_ELAPSED(ctx->waits,t),ctx->pool));
        break;
      }
    }
  }
}

static int start_zk(const char *zkhosts, zkelection_t *election)
{
  apr_pool_t *pool = NULL;
  zk_status_t st = ZEOK;
  apr_interval_time_t interval = APR_TIME_C(-1);
  zeke_runloop_callback_fn setup_func = NULL;
  zeke_runloop_callback_fn interval_func = NULL;
  int i,rc = 0;

  assert(apr_pool_create(&pool,election->pool) == APR_SUCCESS);
  zeke_pool_tag(pool,"runtime pool");

  if(zkhosts == NULL) {
    zkhosts = getenv("ZOOKEEPER_CLUSTER");
    if(zkhosts == NULL)
      zkhosts = getenv("ZOOKEEPER_ENSEMBLE");
    if(zkhosts == NULL) {
      zeke_eprintf("cannot determine zookeeper ensemble, set ZOOKEEPER_CLUSTER or ZOOKEEPER_ENSEMBLE env vars\n");
      rc = 101;
      goto start_zk_exit;
    }
  }
  
  if(ZKTOOL_IS_TIMER_SET(election->timers,ZKELECTION_TIMER_MAX_RUN)) {
    zkelection_reset_timers(election,ZKELECTION_TIMER_MAX_RUN,APR_TIME_C(-1));
  }

  for(i = 0; i < ZKELECTION_NTIMERS; i++)
    if(ZKTOOL_IS_TIMER_SET(election->timers,i))
      if(interval == APR_TIME_C(-1) || interval > election->timers[i]) {
        interval = election->timers[i];
        interval_func = &zkelection_interval_timer;
      }

  if(election->op & ZKELECTION_OPTION_LIST)
    setup_func = &zkelection_list_setup;
  else if(election->op & ZKELECTION_OPTION_FIND)
    setup_func = &zkelection_find_setup;
  else {
    zeke_eprintf("nothing to do!\n");
    usage(101);
  }

  st = zeke_app_run_loop_ex(zkhosts,APR_TIME_C(-1),interval,interval_func,
                            NULL,setup_func,NULL,ZEKE_RUNLOOP_DELAY_STARTUP_CALLBACK,
                            election,NULL);

start_zk_exit:
  if(rc == 0) {
    if(!ZEKE_STATUS_IS_OKAY(st)) {
      if(APR_STATUS_IS_TIMEUP(st))
        return 1;
      if(!ZEKE_STATUS_IS_CONNECTION_CLOSED(st)) {
        zeke_eprintf("unknown error code %d\n",st);
        zeke_apr_error("unhandled error",st);
      }
      zeke_safe_shutdown(st);
    }
  }
  if(pool)
    apr_pool_destroy(pool);
  return rc ? rc : st;
}

static inline const char *validate_node(const char *path, const char *msg)
{
  if(path) {
    while(*path == '/') path++;
    if(strlen(path) > 0 && *(path+strlen(path)-1) == '/') {
      if(msg != NULL)
        zeke_eprintf(msg,path);
      path = NULL;
    }
  }

  return path;
}

ZEKE_HIDDEN int main(int argc, const char *const *argv, const char *const *env)
{
  zk_status_t st = zeke_init_cli(&argc,&argv,&env);
  apr_pool_t *pool = NULL;
  apr_pool_t *getopt_pool = NULL;
  apr_getopt_t *getopt = NULL;
  apr_array_header_t *filters = NULL;
  const char *opt_arg = NULL;
  const char *zkhosts = NULL;
  const char *namespace = NULL;
  zkelection_t *election;
  zktool_timer_t timeouts[ZKELECTION_NTIMERS];
  zkelection_e op = 0;
  const char *op_arg = NULL;
  int opt,i;

  assert(ZEKE_STATUS_IS_OKAY(st));
  assert((pool = zeke_root_subpool_create()) != NULL);
  assert(apr_pool_create(&getopt_pool,pool) == APR_SUCCESS);
#ifdef DEBUGGING
  zoo_set_debug_level(ZOO_LOG_LEVEL_INFO);
#elif defined(NDEBUG)
  zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);
#else
  zoo_set_debug_level(ZOO_LOG_LEVEL_WARN);
#endif

  zeke_pool_tag(pool,"zkelection");
  zeke_pool_tag(getopt_pool,"getopt");
  memset(timeouts,0,sizeof(timeouts));
  timeouts[ZKELECTION_TIMER_MAX_RUN] = apr_time_from_sec(ZKELECTION_DEFAULT_TIMEOUT);
  assert(apr_getopt_init(&getopt,getopt_pool,argc,argv) == APR_SUCCESS);

  while((st = apr_getopt_long(getopt,options,&opt,&opt_arg)) != APR_EOF) {
    switch(st) {
    case APR_BADCH:
    case APR_BADARG:
      usage(1);
      break;
    case APR_SUCCESS:
      switch(opt) {
      case 'h':
        usage(0);
        break;
      case 'C':
        for(;apr_isspace(*opt_arg);opt_arg++) ;
        zkhosts = apr_pstrdup(pool,opt_arg);
        if(!*zkhosts) {
          zeke_eprintf("Invalid connection string\n");
          exit(10);
        }
        break;
      case 'T':
        st = zktool_parse_interval_time(&timeouts[ZKELECTION_TIMER_MAX_RUN],opt_arg,getopt_pool);
        if(!ZEKE_STATUS_IS_OKAY(st))
          zeke_fatal(opt_arg,st);
        break;
      case ZKELECTION_OPTION_DEBUG:
        zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
        break;
      case ZKELECTION_OPTION_LOGFILE:
        st = zktool_set_logfile(opt_arg,pool);
        if(!ZEKE_STATUS_IS_OKAY(st))
          zeke_fatal(opt_arg,st);
        break;
      case ZKELECTION_OPTION_NS:
        namespace = validate_node(apr_pstrdup(pool,opt_arg),"%s is not a valid namespace.\n");
        if(namespace == NULL)
          exit(10);
        break;
      case 'F':
        {
          zeke_regex_t *re = NULL;
          st = zeke_regex_create(&re,opt_arg,ZEKE_RE_DOLLAR_ENDONLY|ZEKE_RE_STUDY);
          if(!ZEKE_STATUS_IS_OKAY(st)) {
            if(ZEKE_STATUS_IS_REGEX_ERROR(st)) {
              zeke_eprintf("%s: %s\n",opt_arg,zeke_regex_error_get(re,NULL,NULL));
            } else
              zeke_apr_error(opt_arg,st);
            exit(11);
          }
          if(filters == NULL) {
            filters = apr_array_make(pool,1,sizeof(zeke_regex_t*));
            assert(filters != NULL);
          }
          APR_ARRAY_PUSH(filters,zeke_regex_t*) = re;
        }
        break;
      case 'f':
        op |= ZKELECTION_OPTION_FIND;
        if(opt_arg)
          op_arg = apr_pstrdup(pool,opt_arg);
        break;
      case 'c':
        op |= ZKELECTION_OPTION_CLEAN;
      case 'l':
        opt = ZKELECTION_OPTION_LIST;
      case ZKELECTION_OPTION_LIST:
        op |= opt;
        if(opt_arg)
          op_arg = apr_pstrdup(pool,opt_arg);
        break;
      default:
        zeke_eprintf("this shouldn't happen\n");
        abort();
      }
      break;
    default:
      zeke_fatal("cli options",st);
      break;
    }
  }

  if(!op)
    usage(2);

  election = apr_pcalloc(pool,sizeof(zkelection_t));
  assert(election != NULL);
  election->pool = pool;
  election->op = op;
  election->args = apr_array_make(pool,(getopt->argc-getopt->ind)+1,sizeof(const char *));
  election->filters = filters;
  memcpy(election->timers,timeouts,sizeof(timeouts));

  if(filters)
    election->filter_how = filter_any;

  for(i = getopt->argc-1; i >= getopt->ind; i--) {
    assert((APR_ARRAY_PUSH(election->args,const char*) = apr_pstrdup(pool,getopt->argv[i])) != NULL);
  }
  
  if(op_arg) {
    APR_ARRAY_PUSH(election->args,const char*) = op_arg;
  }
  
  if(getopt_pool) {
    apr_pool_destroy(getopt_pool);
    getopt_pool = NULL;
  }

  if((op & ZKELECTION_REQUIRE_ARG) != 0) {
    if(election->args->nelts < 1) {
      zeke_eprintf("operation requires zookeeper node path\n");
      usage(0x0500);
    }
    op_arg = validate_node(*((const char**)apr_array_pop(election->args)),
                           "%s is not a valid node path.\n");
    if(op_arg) {
      if(namespace)
        election->path = zeke_nodepath_join(pool,namespace,op_arg,NULL);
      else
        election->path = zeke_nodepath_join(pool,op_arg,NULL);
      assert(election->path != NULL);
    }
  }

  apr_signal(SIGINT,zeke_shutdown_handler);
  apr_signal(SIGTERM,zeke_shutdown_handler);
  apr_signal(SIGHUP,zeke_shutdown_handler);

  return start_zk(zkhosts,election);
}
