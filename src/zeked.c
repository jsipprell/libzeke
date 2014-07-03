/* Note: this is currently litte more than a stub */
#include "internal.h"
#include "dispatch.h"
#include "io.h"
#include "connection.h"
#include "transaction.h"

#include <zeke/libzeke_regex.h>
#include <zeke/libzeke_nodedir.h>
#include <zeke/libzeke_uuid.h>

#define RETURN { zeke_callback_status_set(cbd,ZEOK); \
               return ZEKE_CALLBACK_DESTROY; }

#define ZEKED_ROOT_NODE "zeked:root_node"

#define DECLARE_CONTEXT(type) \
  static inline zeke_context_t *cast_##type##_to_context ( type * (arg) ) { \
                return (zeke_context_t*) (arg); \
  } \
  static inline type *cast_context_to_##type ( zeke_context_t *ctx ) { \
                return ( type * )ctx; \
  }

#define CTX_ARG zeke_context_t *_ctx
#define CONTEXT_ARG zeke_context_t *_ctx
#define CONST_CTX_ARG const zeke_context_t *_ctx
#define USE_CONTEXT(type) type *ctx = (type *)((zeke_context_t*)_ctx)
#define CONTEXT_VAR(type,name) type name = (type)((zeke_context_t*)_ctx)

typedef enum  {
  ZLOCK_START = 0,
  ZLOCK_LS_PARENT,
  ZLOCK_MAKE_UUID,
  ZLOCK_FINI
} zlock_e;

typedef struct {
  apr_pool_t *pool;
  zlock_e state;
  apr_uuid_t uuid;
  const char *parent;
  const char *node;
  const char *path;
} zlock_t;

typedef struct {
  apr_pool_t *pool;
  zeke_connection_t *conn;
  const char *name;
  const char *root;
  apr_uuid_t *uuid;
  int flags;
} registry_state_t;

DECLARE_CONTEXT(registry_state_t)

#define ZLOCK_SESSION_KEY "zlock"

static const char *sentinel = "marker";

static void test_node_watch(const zeke_watcher_cb_data_t*);
static zeke_cb_t test_ls_cb(const zeke_callback_data_t*);
static zeke_cb_t test_acreate(const zeke_callback_data_t *cbd);
static void test_ephemeral_watch(const zeke_watcher_cb_data_t *cbd);
static zeke_cb_t get_metadata(const zeke_callback_data_t *cbd);
static void check_lock_state(const zeke_watcher_cb_data_t *cbd);

static void dump_table(apr_table_t *table, apr_pool_t *pool, const char *label, ...)
{
  const apr_array_header_t  *contents;
  apr_table_entry_t *ent;
  int i;
  va_list ap;

  if(label) {
    va_start(ap,label);
    zk_printf("%s\n",apr_pvsprintf(pool,label,ap));
    va_end(ap);
  }

  assert(table != NULL);
  contents = apr_table_elts(table);
  ent = (apr_table_entry_t*)contents->elts;
  for(i = 0; i< contents->nelts; i++, ent++)
    zk_printf("%s  %s\n",ent->key,ent->val);
 
}

static void test_node_watch(const zeke_watcher_cb_data_t *cbd)
{
  apr_status_t st = APR_SUCCESS;
  zk_printf("hmmm, looks like %s is doing something interesting!\n",cbd->path);
  
  if(strcmp(cbd->path,"/") != 0)
    st = zeke_aget_children(NULL,cbd->connection,cbd->path,test_node_watch,
                              test_ls_cb,
                              apr_pstrdup(zeke_session_pool(cbd->connection),cbd->path));
  if(st != APR_SUCCESS)
    zeke_fatal("zeke_aget_children",st);
}

static zeke_cb_t emit_metadata(const zeke_callback_data_t *cbd)
{
  const char *path = cbd->ctx;

  if(cbd->status == ZEOK) {
    assert(cbd->stat != NULL);
    zk_printf("%s: version=%d, cversion=%d, aversion=%d\n",
              path,cbd->stat->version,cbd->stat->cversion,cbd->stat->aversion);
  }
  return ZEKE_CALLBACK_DESTROY;
}

static void recreate_ephemeral(const zeke_watcher_cb_data_t *cbd)
{
  apr_pool_t *pool = zeke_session_pool(cbd->connection);
  char *path = (char*)cbd->ctx;

  if(cbd->type == ZOO_DELETED_EVENT) {
    const char *epath = apr_pstrcat(pool,path,"/xxxx_",NULL);
    zk_printf("recreating ephemeral sequence under under %s\n",path);
    zeke_acreate(NULL,cbd->connection,epath,NULL,0,NULL,ZOO_EPHEMERAL|ZOO_SEQUENCE,
               get_metadata,path);
  }
}

static zeke_cb_t get_metadata(const zeke_callback_data_t *cbd)
{
  const char *path = cbd->value;
  const char *ctxpath = cbd->ctx;
  
  if(ctxpath)
    ctxpath = apr_pstrdup(zeke_session_pool(cbd->connection),ctxpath);
  if(!path)
    path = ctxpath;

  if(cbd->status == ZEOK) {
    zeke_callback_data_t *newcbd = NULL;
    if(cbd->stat)
      return emit_metadata(cbd);
    newcbd = zeke_callback_data_create(cbd->connection,recreate_ephemeral,cbd->pool);
    zeke_set_watcher_context(newcbd,ctxpath);
    zeke_aexists(&newcbd,cbd->connection,path,recreate_ephemeral,emit_metadata,apr_pstrdup(cbd->pool,path));
  } else {
    zeke_fatal(path,cbd->status);
  }
  return ZEKE_CALLBACK_IGNORE;
}

static void test_ephemeral_watch(const zeke_watcher_cb_data_t *cbd)
{
  apr_pool_t *pool = zeke_session_pool(cbd->connection);
  /*char *path = apr_pstrcat(cbd->pool,base,"/",cbd->path,NULL);
  */
  const char *path = cbd->path;
  const char *ctxpath = apr_pstrdup(zeke_session_pool(cbd->connection),path);
  if(cbd->type == ZOO_DELETED_EVENT) {
    zk_printf("Recreating %s\n",path);
    zeke_aexists(NULL,cbd->connection,path,test_ephemeral_watch,NULL,ctxpath);
    zeke_acreate(NULL,cbd->connection,path,NULL,0,NULL,0,test_acreate,ctxpath);
  } else if(cbd->type == ZOO_CREATED_EVENT) {
    zeke_callback_data_t *ecbd = NULL;
    const char *epath = apr_pstrcat(pool,path,"/xxxx_",NULL);
    zk_printf("creating ephemeral sequence under under %s\n",path);
    zeke_acreate(&ecbd,cbd->connection,epath,NULL,0,NULL,ZOO_EPHEMERAL|ZOO_SEQUENCE,
               get_metadata,ctxpath);
    zeke_aget(NULL,cbd->connection,path,test_ephemeral_watch,get_metadata,ctxpath);
  } else
    zeke_aexists(NULL,cbd->connection,path,test_ephemeral_watch,NULL,ctxpath);
}

static zeke_cb_t test_acreate(const zeke_callback_data_t *cbd)
{
  const char *path = cbd->ctx;

  if(cbd->value)
    zk_printf("  %s: %s\n",path,cbd->value);

  if(cbd->status != APR_SUCCESS)
    zk_printf("  %s: %s\n",path,zeke_perrstr(cbd->status,cbd->pool));
  zeke_aexists(NULL,cbd->connection,path,test_ephemeral_watch,NULL,path);
  RETURN;
}

static zeke_cb_t test_contents(const zeke_callback_data_t *cbd)
{
  const char *path = cbd->ctx;

  if(cbd->value) {
    char *value = apr_pcalloc(cbd->pool,cbd->value_len+1);
    memcpy(value,cbd->value,cbd->value_len);
    zk_printf("  %s: %s\n",path,value);
  } else {
    zk_printf("  %s (null)\n",path);
  }
  RETURN;
}

static zeke_cb_t test_ls_cb(const zeke_callback_data_t *cbd)
{
  char *base = "/";
  char *sep = "";
  apr_pool_t *pool = zeke_session_pool(cbd->connection);
  apr_uint16_t get_content = 0;
  int i;
  const struct String_vector *strings = cbd->vectors.strings;

  if(cbd->ctx == sentinel) {
    if(strings->count > 0) {
      char *path = apr_pstrcat(pool,base,sep,strings->data[0],NULL);
      zk_printf("post wossname for %s\n",path);
      zeke_aget_children(NULL,cbd->connection,path,test_node_watch,
                         test_ls_cb,path);
    }
  } else {
    get_content++;
    if(*((char*)cbd->ctx) != '/') {
      base = apr_pstrcat(cbd->pool,base,sep,(char*)cbd->ctx,NULL);
    } else
      base = (char*)cbd->ctx;
    zk_printf("got children for: %s\n",base);
  }

  if(strcmp(base,"/") != 0 && !*sep)
    sep = "/";
  for(i = 0; i < strings->count; i++) {
    if(get_content) {
      char *path = apr_pstrcat(pool,base,sep,strings->data[i],NULL);
      zeke_aget(NULL,cbd->connection,path,NULL,test_contents,path);
    } else
      zk_printf("  %s%s%s\n",base,sep,strings->data[i]);
  }

  RETURN;
}

static void test_notify(const zeke_watcher_cb_data_t *cbd)
{
  if(cbd->type == ZOO_SESSION_EVENT && cbd->state == ZOO_CONNECTED_STATE) {
    const zeke_connection_t *conn = cbd->connection;
    zk_status_t st;

    assert(conn != NULL);

    LOG("   WOOT: CONNECTED: %s",cbd->path);
    st = zeke_aget_children(NULL,conn,"/",test_node_watch,
                            test_ls_cb,sentinel);
    if(st != APR_SUCCESS)
      zeke_fatal("zeke_aget_children",st);

    st = zeke_acreate(NULL,conn,"/bob/you/there",NULL,0,
                      NULL,0,test_acreate,apr_pstrdup(zeke_session_pool(conn),"/bob/you/there"));
    if(st != APR_SUCCESS)
      zeke_fatal("zeke_acreate",st);
  }
}

static zeke_connection_t *conn = NULL;

static void interrupt_cleanly(int sig)
{
  zeke_safe_shutdown(1);
}

static zeke_cb_t finish_trans(const zeke_callback_data_t *cbd)
{
  zk_status_t st = cbd->status;
  const zeke_transaction_t *trans = (zeke_transaction_t*)cbd->ctx;
  zlock_t *lock = zeke_transaction_context(trans);

  if(st != ZEOK)
    zeke_fatal(lock->path,st);

  zk_printf("%s successfully locked! (%s)\n",lock->parent,lock->node);
#if 0
  zeke_connection_close((zeke_connection_t*)conn);
  zeke_safe_shutdown();
#endif
  RETURN;
}

static zeke_cb_t start_trans2(const zeke_callback_data_t *cbd)
{
  zk_status_t st = cbd->status;
  const zeke_connection_t *conn = cbd->connection;
  zeke_transaction_t *trans = NULL;
  zlock_t *lock = (zlock_t*)cbd->ctx;
  const char *root = lock->parent;
  apr_pool_t *pool = conn->pool;
  const struct String_vector *strings = cbd->vectors.strings;
  char *data;
  char hostname[ZEKE_BUFSIZE];

  if(st != ZEOK)
    zeke_fatal(root,st);

  assert(cbd->stat != NULL);
  if(strings->count != 0) { 
    ERR("lock already held, waiting");
    RETURN;
  }

  st = apr_gethostname(hostname,sizeof(hostname),conn->pool);
  if(st != APR_SUCCESS)
    zeke_fatal("apr_gethostname",st);
  st = zeke_transaction_create(&trans,2,pool);
  if(st != ZEOK)
    zeke_fatal("zeke_transaction_create",st);

  ERR("starting transaction on %s, root %s version %d",lock->node,root,cbd->stat->version);
  
  data = apr_psprintf(lock->pool,"%s=%s",lock->node,hostname);
  lock->state++;
  zeke_transaction_context_set(trans,lock);
  zeke_transaction_require_add(trans,root,cbd->stat->version);
  zeke_trans_op_create_add(trans,lock->path,NULL,0,NULL,ZOO_EPHEMERAL,0);
  zeke_trans_op_set_add(trans,lock->parent,data,strlen(data)+1,-1,0);

  apr_pool_userdata_set(root,ZEKED_ROOT_NODE,NULL,zeke_transaction_pool_get(trans));
  st = zeke_transaction_run(trans,conn,finish_trans);
  if(st != ZEOK) {
    zeke_fatal("zeke_transaction_run",st);
    lock->state--;
  }
  return ZEKE_CALLBACK_IGNORE;
}

static void check_lock_state(const zeke_watcher_cb_data_t *cbd)
{
  const zeke_connection_t *conn = cbd->connection;
  zlock_t *lock = zeke_session_context(conn,ZLOCK_SESSION_KEY);
  assert(lock != NULL);

  if(lock->state != 0)
    return;

  if(cbd->type == ZOO_CHILD_EVENT)
    zeke_aget_children(NULL,conn,lock->parent,check_lock_state,start_trans2,lock);
}

static void start_trans(const zeke_watcher_cb_data_t *cbd)
{
  if(cbd->type == ZOO_SESSION_EVENT && cbd->state == ZOO_CONNECTED_STATE) {
    const zeke_connection_t *conn = cbd->connection;
    apr_pool_t *session_pool = zeke_session_pool(conn);
    zlock_t *lock;
    ZEKE_MAY_ALIAS(const char) *node;
    zk_status_t st;
    char uuid_buf[APR_UUID_FORMATTED_LENGTH+1];

    zeke_pool_tag(session_pool,"session");
    assert(session_pool != NULL);
    assert(conn != NULL);
    st = apr_pool_userdata_get((void**)&node,ZEKED_ROOT_NODE,conn->pool);
    assert(st == APR_SUCCESS && node != NULL);
    lock = zeke_session_context_create(conn,ZLOCK_SESSION_KEY,sizeof(zlock_t));
    lock->pool = session_pool;
    node = apr_pstrdup(lock->pool,node);
    lock->parent = node;
    apr_uuid_get(&lock->uuid);
    apr_uuid_format(uuid_buf,&lock->uuid);
    lock->node = apr_pstrdup(lock->pool,uuid_buf);
    lock->path = apr_pstrcat(lock->pool,lock->parent,"/",uuid_buf,NULL);
    st = zeke_aget_children(NULL,conn,lock->parent,check_lock_state,start_trans2,lock);
    assert(st == APR_SUCCESS);
  }
}

static void test_lock(const char *zkhosts, const char *node)
{
  zk_status_t st = APR_SUCCESS;
  apr_interval_time_t interval;
  int c;

  for (c = 0; st == APR_SUCCESS ;c++) {
    apr_pool_t *pool = zeke_root_subpool_create();
    zeke_pool_tag(pool,apr_psprintf(pool,"ROOT i/o loop pool %d",c));
    interval = apr_time_from_sec(20);

    st = zeke_connection_create(&conn,zkhosts,start_trans,pool);
 
    apr_pool_userdata_setn(node,ZEKED_ROOT_NODE,NULL,conn->pool);
    if(st != APR_SUCCESS)
      zeke_fatal(apr_pstrcat(pool,"zeke_connection_create(%s)",zkhosts,NULL),st);
      
    st = zeke_io_loop(conn,&interval,NULL);
    if(st != APR_SUCCESS && !APR_STATUS_IS_TIMEUP(st))
      zeke_fatal(apr_pstrcat(pool,"zeke_io_loop: ",zkhosts,NULL),st);
#ifdef DEBUGGING
    zk_printf("test: %s\n",zeke_perrstr(st,pool));
#endif
    apr_pool_destroy(pool);
    break;
  }
  
  if(APR_STATUS_IS_TIMEUP(st))
    return;
  if(st != APR_SUCCESS)
    zeke_apr_error("unhandled error",st);
  exit(st);
}

typedef struct {
  apr_interval_time_t timer;
  int timeout_count;
  zeke_nodedir_t *nodedir;
  const char *root;
  const char *secondary;
  const zeke_connection_t *connection;
  apr_pool_t *pool;
} test_nodedir_t;


static void uuid_registry_complete(const zeke_uuid_registry_t *reg, CONTEXT_ARG)
{
  CONTEXT_VAR(registry_state_t*,state);
  apr_pool_t *pool = state->pool;
  apr_hash_t *services = zeke_uuid_registry_hash_get(reg,pool);
  const apr_array_header_t *index = zeke_uuid_registry_index_get(reg,1,pool);
  apr_uuid_t myuuid;
  apr_size_t i;
  char uuid_buf[APR_UUID_FORMATTED_LENGTH+1];
  
  assert(zeke_uuid_registry_uuid_get(reg,&myuuid) == ZEOK);

  zk_printf("uuid registration table:\n");
  for(i = 0; i < index->nelts; i++) {
    const char *name = APR_ARRAY_IDX(index,i,const char*);
    apr_uuid_t *uuid = apr_hash_get(services,name,APR_HASH_KEY_STRING);
    assert(uuid != NULL);
    apr_uuid_format(uuid_buf,uuid);
    if(memcmp(&myuuid,uuid,sizeof(apr_uuid_t)) == 0)
      zk_printf("%-40s(ME) %s\n",name,uuid_buf);
    else
      zk_printf("%-44s %s\n",name,uuid_buf);
  }
  zeke_connection_shutdown(state->conn);
}

static int nodedir_completed(const zeke_nodedir_t *nodedir,
                              apr_table_t *list,
                              zeke_context_t *ctx)
{
  test_nodedir_t *app_data = (test_nodedir_t*)ctx;
  const zeke_connection_t *conn = app_data->connection;
  const char *root = app_data->root;
  zk_status_t st = zeke_nodedir_status(nodedir);
  
  if(st != ZEOK) {
    zk_eprintf("%s: %s\n",root,zeke_pstrerr(app_data->pool,st));
    zeke_connection_shutdown((zeke_connection_t*)conn);
    zeke_nodedir_destroy((zeke_nodedir_t*)nodedir);
    return 0;
  }

  dump_table(list,app_data->pool,"listing of %s",root);
 
  if(0) {
    test_nodedir_t *new_app = apr_pcalloc(app_data->pool,sizeof(test_nodedir_t));
    new_app->pool = app_data->pool;
    new_app->root = app_data->secondary;
    new_app->secondary = app_data->root;
    new_app->connection = conn;
    zeke_nodedir_create(&new_app->nodedir,new_app->root,-1,nodedir_completed,
                        NULL,new_app,conn);
    app_data->nodedir = new_app->nodedir;
  }
#if 1
  zeke_connection_shutdown((zeke_connection_t*)conn);
#endif
  return 0;
}

static void test_nodedir_interval_timer(zeke_connection_t *conn,
                                        void *data)
{
  test_nodedir_t *app_data = (test_nodedir_t*)data;

  if(++(app_data->timeout_count) == 2) {
    zk_eprintf("%s --> too many timeouts, aborting! (%d)\n",app_data->root,app_data->timeout_count);
    if(!zeke_nodedir_is_complete(app_data->nodedir)) {
      zk_eprintf("%s nodedir fetch incomplete, cancelling.\n",app_data->root);
      zeke_nodedir_cancel(app_data->nodedir);
    } else {
      zk_eprintf("shutting down connection, nodedir fetch completed.\n");
      /* zeke_connection_shutdown((zeke_connection_t*)conn); */
      zeke_connection_close((zeke_connection_t*)conn);
    }
  } else {
    zk_eprintf("%s --> !! %" APR_INT64_T_FMT "us timeout interval (%d)!\n",
              app_data->root,app_data->timer,app_data->timeout_count);
    assert(app_data->timeout_count < 10);
    if(app_data->timeout_count >= 3) {
      zk_eprintf("Now forcing the zk connection to shutdown.\n");
      zeke_connection_shutdown((zeke_connection_t*)conn);
    }
  }
}

static void test_nodedir_setup(zeke_connection_t *conn, void *data)
{
  zk_status_t st;
  test_nodedir_t *app_data = (test_nodedir_t*)data;

  app_data->connection = conn;
  app_data->timeout_count = 0;
  st = zeke_nodedir_create(&app_data->nodedir,app_data->root,-1,nodedir_completed,
                           NULL,app_data,conn);
  if(st != APR_SUCCESS)
    zeke_fatal(apr_psprintf(app_data->pool,"zeke_nodedir_create(%s)",app_data->root),st);
}

static void test_uuid_registry_setup(zeke_connection_t *conn, void *data)
{
  zk_status_t st;
  registry_state_t *state = (registry_state_t*)data;
  zeke_uuid_registry_t *reg;

  state->conn = conn;
  st = zeke_uuid_registry_create_ex(&reg,state->root,
                                    state->name,
                                    state->uuid,
                                    -1,
                                    uuid_registry_complete,
                                    NULL,
                                    state,
                                    state->flags | ZEKE_UUID_REGISTER_PERMANENT,
                                    conn);
  if(!ZEKE_STATUS_IS_OKAY(st))
    zeke_fatal("zeke_uuid_registry_create",st);
}

static void test_nodedir(const char *zkhosts, const char *root)
{
  zk_status_t st = APR_SUCCESS;
  test_nodedir_t app_data = { 10*1000, 0, NULL, NULL, NULL, NULL };

  app_data.pool = zeke_root_subpool_create();
  app_data.root = root;
  app_data.secondary = "/pubsys/live_nodes";
  st = zeke_app_run_loop_interval(zkhosts,app_data.timer,test_nodedir_interval_timer,
                                  test_nodedir_setup,0,&app_data);

  
  if(st != ZEOK) {
    if(APR_STATUS_IS_TIMEUP(st))
      return;
    if(!ZEKE_STATUS_IS_CONNECTION_CLOSED(st)) {
      zk_eprintf("unknown error code %d\n",st);
      zeke_apr_error("unhandled error",st);
    } else
      zeke_safe_shutdown(st);
  }
  exit(st);
}

static void test_uuid_registry(const char *zkhosts, const char *name, apr_uuid_t *uuid)
{
  registry_state_t *state;
  zk_status_t st = ZEOK;
  apr_pool_t *pool = zeke_root_subpool_create();

  zeke_pool_tag(pool,"global uuid registry test pool");
  state = apr_pcalloc(pool,sizeof(registry_state_t));
  state->pool = pool;
  state->name = apr_pstrdup(pool,name);
  state->uuid = uuid;
  state->flags = ZEKE_UUID_REGISTER_ACCEPT;

  st = zeke_app_run_loop(zkhosts,test_uuid_registry_setup,0,state);

  if(!ZEKE_STATUS_IS_OKAY(st)) {
    if(APR_STATUS_IS_TIMEUP(st))
      return;
    if(!ZEKE_STATUS_IS_CONNECTION_CLOSED(st)) {
      zk_eprintf("unknown error code %d\n",st);
      zeke_apr_error("unhandled error",st);
    } else
      zeke_safe_shutdown(st);
  }
  exit(st);
}

static void test_connect(const char *zkhosts, unsigned loop_time)
{
  zk_status_t st = APR_SUCCESS;
  apr_interval_time_t interval;
  int c;

  for (c = 0; st == APR_SUCCESS ;c++) {
    apr_pool_t *pool = zeke_root_subpool_create();
    interval = apr_time_from_sec(loop_time);

    st = zeke_connection_create(&conn,zkhosts,test_notify,pool);
    if(st != APR_SUCCESS)
      zeke_fatal(apr_pstrcat(pool,"zeke_connection_create(%s)",zkhosts,NULL),st);

#ifdef DEBUGGING
    zk_printf("loop #%d, %lums\n",c+1,(unsigned long)interval);
#endif
#if 0
    while(st == APR_SUCCESS || APR_STATUS_IS_TIMEUP(st)) {
#endif
      st = zeke_io_loop(conn,&interval,NULL);
#if 0
      if(APR_STATUS_IS_TIMEUP(st))
        interval = apr_time_from_sec(loop_time);
    }
#endif
    if(st != APR_SUCCESS && !APR_STATUS_IS_TIMEUP(st))
      zeke_fatal(apr_pstrcat(pool,"zeke_io_loop: ",zkhosts,NULL),st);
#ifdef DEBUGGING
    zk_printf("test: %s\n",zeke_perrstr(st,pool));
#endif
    apr_pool_destroy(pool);
    break;
  }
  
  if(APR_STATUS_IS_TIMEUP(st))
    return;
  if(st != APR_SUCCESS)
    zeke_apr_error("unhandled error",st);
  exit(st);
}

static apr_getopt_option_t options[] = {
  { "lock", 'l', 1, "lock the specified zookeeper node via a transaction" },
  { "dir",  'd', 1, "display a listing of a node's children and contents" },
  { "register", 'r', 0, "test uuid registry" },
  { "name", 1,  1,  "set the name to register a uuid under" },
  { "uuid", 'u',  1, "specify uuid to register" },
  { "debug", 'D', 0, "enable debugging output" },
  { "help" , 'h', 0, "display help" },
  { NULL, 0, 0, NULL }
};

static void usage(int exitcode)
{
  apr_getopt_option_t *opt;
  apr_pool_t *pool = zeke_root_subpool_create();
  char *optfmt;

  const char *optfmt_long_with_arg = "  --%s=VALUE,-%c";
  const char *optfmt_long = "  --%s,-%c";
  const char *optfmt_no_short_with_arg = "  --%s=VALUE";
  const char *optfmt_no_short = "  --%s";

  if(!exitcode)
    zk_printf("zeked [options]\noptions:\n");
  for(opt = options; opt->name != NULL; opt++) {
    if(opt->has_arg && !apr_isalnum(opt->optch))
      optfmt = apr_psprintf(pool,optfmt_no_short_with_arg,opt->name);
    else if(!opt->has_arg && apr_isalnum(opt->optch))
      optfmt = apr_psprintf(pool,optfmt_long,opt->name,(char)opt->optch);
    else if(!opt->has_arg && !apr_isalnum(opt->optch))
      optfmt = apr_psprintf(pool,optfmt_no_short,opt->name);
    else
      optfmt = apr_psprintf(pool,optfmt_long_with_arg,opt->name,(char)opt->optch);

    optfmt = apr_pstrcat(pool,optfmt,apr_psprintf(pool,"%%%ds%%s\n",(int)(30-strlen(optfmt))),NULL);

    zk_printf(optfmt,"",opt->description);
  }

  exit(exitcode);
}

int main(int argc, const char *const *argv, const char *const *env)
{ 
  apr_pool_t *pool,*subpool;
  zk_status_t st = zeke_init_cli(&argc,&argv,&env);
  apr_getopt_t *option_parser = NULL;
  const char *opt_arg = NULL;
  const char *lock_node = NULL;
  const char *dir_node = NULL;
  const char *register_name = NULL;
  apr_uuid_t *register_uuid = NULL;
  int uuid = 0;
  int opt;
  int i;
  int debug_mode = 0;

  apr_signal(SIGINT,interrupt_cleanly);
#ifdef DEBUGGING
  debug_mode = 1;
  zoo_set_debug_level(ZOO_LOG_LEVEL_INFO);
#elif defined(NDEBUG)
  debug_mode = -1;
  zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);
#else
  zoo_set_debug_level(ZOO_LOG_LEVEL_WARN);
#endif
  assert(st == APR_SUCCESS);
  pool = zeke_root_subpool_create();
  zeke_pool_tag(pool,"getopt pool");
  apr_pool_create_ex_debug(&subpool,pool,NULL,NULL,"testing subpool:" __FILE__);

  assert(apr_getopt_init(&option_parser,subpool,argc,argv) == APR_SUCCESS);
  while((st = apr_getopt_long(option_parser,options,&opt,&opt_arg)) != APR_EOF) {
    switch(st) {
    case APR_BADCH:
    case APR_BADARG:
      usage(1);
      break;
    case APR_SUCCESS:
      switch(opt) {
      case 'u':
        {
          apr_uuid_t reg_uuid;

          st = apr_uuid_parse(&reg_uuid,opt_arg);
          if(!ZEKE_STATUS_IS_OKAY(st))
            zeke_fatal(zeke_abort_msg("bad uuid '%s'",opt_arg),st);
          register_uuid = (apr_uuid_t*)apr_palloc(pool,sizeof(apr_uuid_t));
          *register_uuid = reg_uuid;
        }
        break;
      case 1:
        register_name = apr_pstrdup(pool,opt_arg);
        break;
      case 'l':
        lock_node = apr_pstrdup(pool,opt_arg);
        break;
      case 'd':
        dir_node = apr_pstrdup(pool,opt_arg);
        break;
      case 'r':
        uuid++;
        break;
      case 'D':
        if(debug_mode > 0)
          debug_mode--;
        else if(debug_mode <= 0)
          debug_mode = 1;
        if(debug_mode > 0)
          zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
        else
          zoo_set_debug_level(ZOO_LOG_LEVEL_WARN);
        break;
      case 'h':
        usage(0);
      default:
        abort();
      }
      break;
    default:
      abort();
    }
  }

  apr_pool_destroy(subpool);
  if(uuid) {
    test_uuid_registry("rutl020d:2181,rslr007d:2181,rslr002d:2181,rutl001d:2181,rslr019d:2181",
                       register_name,register_uuid);
  }
  else if(dir_node)
    test_nodedir("rutl001d:2181,rslr007d:2181",dir_node);

#if 0
    test_nodedir("rutl020d:2181,rslr007d:2181,rslr002d:2181,rutl001d:2181,rslr019d:2181",
                dir_node);
#endif
  else if(lock_node) {
    if(*lock_node != '/')
      lock_node = apr_pstrcat(pool,"/",lock_node,NULL);

    test_lock("rutl020d:2181,rslr007d:2181,rslr002d:2181,rutl001d:2181,rslr019d:2181",
                lock_node);
  } else {
    for(i = 0; 1; i++)
      test_connect("rutl020d:2181,rslr007d:2181,rslr002d:2181,rutl001d:2181,rslr019d:2181",
                  i+300);
  }
  return 0;
}
