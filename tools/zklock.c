#include "libzeke.h"
#include "libzeke_event_loop.h"
#include "libzeke_nodedir.h"
#include "libzeke_errno.h"
#include "libzeke_trans.h"
#include "libzeke_util.h"

#include "tools.h"

#define ZKLOCK_DEBUG_TIMERS

#ifndef ZKLOCK_MAX_RUN_TIME
#define ZKLOCK_MAX_RUN_TIME 1200
#endif

#ifndef ZKLOCK_ZK_TIMEOUT
#define ZKLOCK_ZK_TIMEOUT 30
#endif

#define RETURN { zeke_callback_status_set(cbd,ZEOK); \
                 return ZEKE_CALLBACK_IGNORE; }

#define DECLARE_CONTEXT(type) \
  static inline zeke_context_t *cast_##type##_to_context ( type * (arg) ) { \
                return (zeke_context_t*) (arg); \
  } \
  static inline type *cast_context_to_##type ( zeke_context_t *ctx ) { \
                return ( type * )ctx; \
  }


#define CONTEXT_ARG zeke_context_t *_ctx
#define CONTEXT_VAR(type,name) type name = (type)((zeke_context_t*)_ctx)

enum {
  zklock_option_debug = 1,
#define ZKLOCK_OPTION_DEBUG zklock_option_debug
#define OPT_DEBUG zklock_option_debug
  zklock_option_ns,
#define ZKLOCK_OPTION_NS zklock_option_ns
#define OPT_NS zklock_option_ns
  zklock_option_connect,
#define ZKLOCK_OPTION_CONNECT zklock_option_connect
#define OPT_ZKHOSTS zklock_option_connect
  zklock_option_maxrun,
#define ZKLOCK_OPTION_MAXRUN zklock_option_maxrun
#define OPT_MAXRUN zklock_option_maxrun
  zklock_option_zk_timeout,
#define ZKLOCK_OPTION_ZK_TIMEOUT zklock_option_zk_timeout
#define OPT_ZKTIME zklock_option_zk_timeout
  zklock_option_xor,
#define ZKLOCK_OPTION_XOR zklock_option_xor
#define OPT_XOR zklock_option_xor
  zklock_option_logfile,
#define ZKLOCK_OPTION_LOGFILE zklock_option_logfile
#define OPT_LOGFILE zklock_option_logfile
  zklock_option_no_wait_exit,
#define ZKLOCK_OPTION_NO_WAIT_EXIT zklock_option_no_wait_exit
#define OPT_NOWAIT_EXIT zklock_option_no_wait_exit
  zklock_option_no_wait = (1 << 9),
#define ZKLOCK_OPTION_NO_WAIT zklock_option_no_wait
#define OPT_NOWAIT zklock_option_no_wait
  zklock_option_make_path = (1 << 10)
#define ZKLOCK_OPTION_MAKE_PATH zklock_option_make_path
};

typedef enum {
  ZKLOCK_START = 0,
  ZKLOCK_CONNECTED,
  ZKLOCK_LS_PARENT,
  ZKLOCK_WAIT,
  ZKLOCK_MKPATH,
  ZKLOCK_TRANS,
  ZKLOCK_CHILD,
  ZKLOCK_FINI
} zklock_e;

typedef enum {
  zklock_timer_lock = 0,
#define ZKLOCK_TIMER_LOCK zklock_timer_lock
  zklock_timer_zk_timeout = 1,
#define ZKLOCK_TIMER_ZK zklock_timer_zk_timeout
  zklock_timer_max_run = 2
#define ZKLOCK_TIMER_MAX_RUN zklock_timer_max_run
} zklock_timer_e;
#define ZKLOCK_NTIMERS 3
#define zklock_reset_timers(lock,index,value) \
        zktool_reset_timers((lock)->timeouts,(lock)->waits,ZKLOCK_NTIMERS,(index),(value))
#define zklock_update_timers(lock,shortest,updates,max_updates) \
        zktool_update_timers((lock)->timeouts,(lock)->waits,ZKLOCK_NTIMERS,(shortest),(updates),(max_updates))
#define zklock_update_timeouts(lock,index,shortest) \
        zktool_update_timeouts((lock)->timeouts,(lock)->waits,ZKLOCK_NTIMERS,(index),(shortest))

typedef struct {
  apr_pool_t *pool;
  zklock_e state;
  const zeke_connection_t *conn;
  const char *session_key;
  const char *parent,*name,*path,*ns;
  const char *content;
  const char *command;
  apr_array_header_t *args;
  apr_procattr_t *procattr;
  zktool_timer_t timeouts[ZKLOCK_NTIMERS];
  zktool_wait_t waits[ZKLOCK_NTIMERS];
  apr_pool_t *procpool;
  apr_proc_t *proc;
  apr_uint32_t flags;
  int exitcode;
  apr_uint16_t exitcode_xor;
} zklock_t;

typedef struct {
  apr_pool_t *pool;
  apr_array_header_t *stack;
  const char *cur_path;
  zklock_t *lock;
} zklock_mkpath_t;

#define ZKLOCK_SESSION_KEY "zklock_session"

/* forward decls */
static void zklock_wait_parent(const zeke_watcher_cb_data_t*);
static zeke_cb_t zklock_check_parent(const zeke_callback_data_t*);

#define WATCHER(f) (((f) & ZKLOCK_OPTION_NO_WAIT) ? NULL : zklock_wait_parent)

static apr_getopt_option_t options[] = {
 { "max-run",       OPT_MAXRUN,     1,
  "Set the maximum time the spawned child process is allowed to run for (default "APR_STRINGIFY(ZKLOCK_MAX_RUN_TIME)"s)"},
 { "lock-timeout",  'T',            1,
  "Set the maximum time to wait for a lock (ex: 250ms, 1.4min, 20s, 3H, 1.2hours)"},
 { "zk-timeout",    OPT_ZKTIME,     1,
  "Set the ZooKeeper general networking timeout (default "APR_STRINGIFY(ZKLOCK_ZK_TIMEOUT)"s)"},
 { "content",       'c',            1,
  "Set the content of the ephemeral locking node (default is command-to-execute)" },
 { "no-wait",       'N',            0,
  "If the lock is already acquired by another host or process, exit immediately without error" },
 { "no-wait-exit",  'E',            1,
  "If the lock is already acquired by another host or process, exit immediately with the specified exit code" },
 { "xor",           OPT_XOR,        1,
  "Perform a bitwise XOR operation on any non-zero return value of a locked process, this can be used "
  "to determine the exitcode of a process as distinct from a zklock failure" },
 { "make-path",     'p',            0,
  "If full path doesn't already exist, make any missing nodes" },
 { "name",          'n',            1,
  "name of ephemeral locking node (default is fqdn)" },
 { "path",          'P',            1,
  "parent path under which to create locking node (default is unused -- see --ns)" },
 { "connect",       OPT_ZKHOSTS,    1,
  "override zookeeper connection string (default is from env $ZOOKEEPER_ENSEMBLE)" },
 { "ns",            OPT_NS,         1,
  "additional namespace to prepend to path (default is /locks). Relative namespaces will be appended to /locks." },
 { "logfile",       OPT_LOGFILE,    1,
  "set the general logging file (default is stderr)" },
 { "debug",         OPT_DEBUG,      0,
  "enable debug mode" },
 { "help",          'h',            0,  "display this help" },
 { NULL,0,0,NULL }
};

#undef OPT_MAXRUN
#undef OPT_ZKHOSTS
#undef OPT_DEBUG
#undef OPT_NS
#undef OPT_NOWAIT
#undef OPT_NOWAIT_EXIT
#undef OPT_LOGFILE
#undef OPT_ZKTIME
#undef OPT_XOR

static apr_pool_t *zklock_pool = NULL;
static apr_proc_t *reflect_proc = NULL;
#ifndef APR_ARRAY_POP
#define APR_ARRAY_POP(ary,type) (*((type*)apr_array_pop(ary)))
#endif

static int raise_sig = 0;

static void zeke_exit_handler(void *arg)
{
  int *sigp = arg;

  if(sigp && *sigp) {
    apr_signal_unblock(*sigp);
    raise(*sigp);
  }
}

static apr_status_t cleanup_proc(void *data)
{
  zklock_t *lock = (zklock_t*)data;

  if(lock) {
    if(reflect_proc == lock->proc)
      reflect_proc = NULL;
    lock->procpool = NULL;
    apr_proc_other_child_unregister(lock);
  }
  return APR_SUCCESS;
}

static inline int zklock_shutdown_xor(apr_uint16_t code, apr_byte_t xor)
{
  if(code && xor)
    code = ((code & 0xff) ^ xor) | (code & (APR_UINT16_MAX ^ 0xff));

  fflush(stdout);

  return (int)code;
}

static void zklock_shutdown(apr_uint16_t code, apr_byte_t xor)
{
  zeke_safe_shutdown(zklock_shutdown_xor(code,xor));
}

static void zklock_shutdown_ex(apr_uint16_t code, apr_byte_t xor, const char *fmt, ...)
{
  int c = zklock_shutdown_xor(code,xor);
  va_list ap;

  va_start(ap,fmt);
  zeke_safe_shutdown_va(c,fmt,ap);
  va_end(ap);
}

static void zklock_maintenance(int reason, void *data, int status)
{
  zklock_t *lock = (zklock_t*)data;
  int shutdown = 0;
  const char *msg = NULL;
  int code = status;
  int unreg = 0;
  apr_byte_t xor = 0;

#if 1
  ERR("maint %d",status);
#endif
  if(lock) {
    if(!lock->proc)
      unreg++;
    if(shutdown || reason == APR_OC_REASON_DEATH) {
      shutdown++;
      unreg++;
      xor = lock->exitcode_xor;
      if(APR_PROC_CHECK_SIGNALED(status) || APR_PROC_CHECK_CORE_DUMP(status)) {
        static int sig = 0;
        sig = code & 0xff;
        code = 0;
        if(sig) {
          LOG("child signaled %d",sig);
          apr_signal(sig,SIG_DFL);
          zeke_atexit(zeke_exit_handler,&sig);
        }
      } else if(APR_PROC_CHECK_EXIT(status) || (status & 0x00ff) == 0) {
        code = (status & 0xff00) >> 8;
        LOG("child exit status=%d",code);
      }
      if(zeke_connection_is_open(lock->conn)) {
        LOG("SHUTDOWN DUE TO CHILD PROC DEATH");
        zeke_connection_shutdown((zeke_connection_t*)lock->conn);
      }
    } else {
      if(reason == APR_OC_REASON_UNREGISTER && lock->procpool) {
        apr_pool_t *pool = lock->procpool;
        apr_pool_cleanup_kill(pool,lock,cleanup_proc);
        if(reflect_proc == lock->proc)
          reflect_proc = NULL;
        lock->procpool = NULL;
        apr_pool_destroy(pool);
        return;
      } else if(zklock_update_timeouts(lock, ZKLOCK_TIMER_MAX_RUN, NULL)) {
        if(!shutdown) {
          
          zeke_connection_shutdown((zeke_connection_t*)lock->conn);
          shutdown = 1;
          code = 128;
          msg = apr_psprintf(lock->pool,"child process killed, max lock time expired (%s)",
                    zktool_interval_format(ZKTOOL_STATIC_TIME_ELAPSED(lock->waits,ZKLOCK_TIMER_MAX_RUN),lock->pool));
        }
      }
    }
  }

  if(unreg && data && lock->procpool) {
    apr_pool_cleanup_kill(lock->procpool,lock,cleanup_proc);
    if(reflect_proc == lock->proc)
      reflect_proc = NULL;
    if(reason != APR_OC_REASON_UNREGISTER)
      apr_proc_other_child_unregister(data);
  }

  if(shutdown) {
    if(msg)
      zklock_shutdown_ex(code,xor,msg);
    else
      zklock_shutdown(code,xor);
  }
}

static void zklock_start_child(zklock_t *lock)
{
  zk_status_t st;
  const char *prog;

  if(lock->procpool)
    apr_pool_destroy(lock->procpool);
  assert(apr_pool_create(&lock->procpool,lock->pool) == APR_SUCCESS);

  assert(apr_env_set("ZKLOCK_PID",
              apr_psprintf(lock->pool,"%" APR_UINT64_T_FMT, (apr_uint64_t)getpid()),
              lock->pool) == APR_SUCCESS);
  if(lock->path)
    apr_env_set("ZKLOCK_NODE",lock->path,lock->pool);
  else
    apr_env_delete("ZKLOCK_NODE",lock->pool);

  prog = APR_ARRAY_IDX(lock->args,0,const char*);
  zklock_reset_timers(lock,ZKLOCK_TIMER_MAX_RUN,
                      (lock->timeouts[ZKLOCK_TIMER_MAX_RUN] > APR_TIME_C(0) ? -1 :
                      apr_time_from_sec(ZKLOCK_MAX_RUN_TIME)));
  lock->state =  ZKLOCK_CHILD;
  lock->proc = (apr_proc_t*)apr_palloc(lock->procpool,sizeof(apr_proc_t));
  assert(lock->proc != NULL);
  st = apr_proc_create(lock->proc,prog,
                       (const char*const*)lock->args->elts,
                       NULL,lock->procattr,lock->procpool);
  if(!ZEKE_STATUS_IS_OKAY(st)) {
    lock->proc = NULL;
    zeke_fatal(prog,st);
  }

  apr_pool_cleanup_register(lock->procpool,lock,cleanup_proc,apr_pool_cleanup_null);
#if 0
  apr_pool_cleanup_register(lock->procpool,lock,kill_proc,apr_pool_cleanup_null);
#endif
  reflect_proc = lock->proc;
  apr_proc_other_child_register(lock->proc,zklock_maintenance,lock,NULL,lock->pool);
  apr_pool_note_subprocess(lock->procpool,lock->proc,APR_KILL_AFTER_TIMEOUT);
}

static zeke_cb_t zklock_finish_trans(const zeke_callback_data_t *cbd)
{
  zk_status_t st = cbd->status;
  const apr_array_header_t *results;
  int i;
  int restart = 0;
  const zeke_transaction_t *trans = (zeke_transaction_t*)cbd->ctx;
  zklock_t *lock = zeke_transaction_context(trans);

  assert(lock != NULL);
  assert(lock->state == ZKLOCK_TRANS);

  if(!ZEKE_STATUS_IS_OKAY(st)) {
    /* Assume some underly transaction element failed (is this true?) */
    /* this is a failure of something besides the actual transaction components */
    zeke_apr_error("transaction failure",st);
    restart++;
  } else {
    assert((results = zeke_transaction_results(trans)) != NULL);
    for(i = 0; i < results->nelts; i++) {
      const zoo_op_result_t *res = &APR_ARRAY_IDX(results,i,const zoo_op_result_t);
      assert(res != NULL);
      st = APR_FROM_ZK_ERROR(res->err);
      if(!ZEKE_STATUS_IS_OKAY(st)) {
        INFO("transaction on %s: %s",lock->path,zeke_perrstr(st,cbd->pool));
        restart++;
      }
    }
  }

  if(restart) {
    /* Someone may have beaten us to it, so now we must restart from scratch */
    lock->state = ZKLOCK_CONNECTED;
    st = zeke_aget_children(NULL,cbd->connection,lock->parent,
                            WATCHER(lock->flags),
                            zklock_check_parent,lock);
    if(!ZEKE_STATUS_IS_OKAY(st))
      zeke_fatal(lock->parent,st);
  } else {
    zklock_start_child(lock);
  }
  return ZEKE_CALLBACK_DESTROY;
}

static void zklock_start_trans(zklock_t *lock, int version)
{
  zk_status_t st;
  zeke_transaction_t *trans = NULL;

  st = zeke_transaction_create(&trans,2,lock->pool);
  if(!ZEKE_STATUS_IS_OKAY(st))
    zeke_fatal(apr_psprintf(lock->pool,"creating transaction for '%s'",lock->path),st);

  INFO("starting transaction for: %s",lock->path);
  zeke_transaction_context_set(trans,lock);
  zeke_transaction_require_add(trans,lock->parent,version);
  zeke_trans_op_create_add(trans,lock->path,lock->content,
                        (lock->content ? strlen(lock->content)+1 : 0),
                        ZEKE_DEFAULT_ACL,ZOO_EPHEMERAL,0);
  zeke_trans_op_set_add(trans,lock->parent,NULL,0,-1,0);

  st = zeke_transaction_run(trans,lock->conn,zklock_finish_trans);
  if(st != ZEOK)
    zeke_fatal(apr_psprintf(lock->pool,"running transaction for '%s'",lock->path),st);
}

static zeke_cb_t zklock_mkpath(const zeke_callback_data_t *cbd)
{
  zk_status_t st = cbd->status;
  zklock_mkpath_t *mkpath = (zklock_mkpath_t*)cbd->ctx;

  assert(mkpath != NULL);
  if(!ZEKE_STATUS_IS_OKAY(st)) {
    /* node exists is silently ignored */
    if(!APR_STATUS_IS_ZEKE_ERROR(st) || APR_TO_ZK_ERROR(st) != ZNODEEXISTS) {
      zeke_apr_error(mkpath->cur_path,st);
      zklock_shutdown(st,0);
      return ZEKE_CALLBACK_DESTROY;
    }
  }

  if(mkpath->stack->nelts == 0) {
    /* all done */
    apr_pool_t *pool = mkpath->pool;
    zklock_t *lock = mkpath->lock;
    mkpath->pool = NULL;
    apr_pool_destroy(pool);
    if(lock->state == ZKLOCK_MKPATH)
      lock->state = ZKLOCK_LS_PARENT;
    /* restart the get child call so we will have the most recent version for
       the lock trans */
    st = zeke_aget_children((zeke_callback_data_t**)&cbd,cbd->connection,lock->parent,
                             WATCHER(lock->flags),
                             zklock_check_parent,lock);
    if(!ZEKE_STATUS_IS_OKAY(st)) {
      zeke_apr_error(lock->parent,st);
      zklock_shutdown(st,0);
      return ZEKE_CALLBACK_DESTROY;
    }
  } else {
    if(mkpath->lock->state > 0)
      mkpath->lock->state = ZKLOCK_MKPATH;

    if(mkpath->cur_path)
      mkpath->cur_path = zeke_nodepath_join(mkpath->pool,mkpath->cur_path,
                                            APR_ARRAY_POP(mkpath->stack,const char*),NULL);
    else
      mkpath->cur_path = zeke_nodepath_join(mkpath->pool,APR_ARRAY_POP(mkpath->stack,const char*),NULL);

    assert(mkpath->cur_path != NULL);
    st = zeke_acreate((zeke_callback_data_t**)&cbd,cbd->connection,mkpath->cur_path,
                       NULL,0,ZEKE_DEFAULT_ACL,0,zklock_mkpath,mkpath);

    if(!ZEKE_STATUS_IS_OKAY(st)) {
      zeke_apr_error(mkpath->cur_path,st);
      zklock_shutdown(st,0);
      return ZEKE_CALLBACK_DESTROY;
    }
  }
  return ZEKE_CALLBACK_IGNORE;
}

static void zklock_wait_parent(const zeke_watcher_cb_data_t *cbd)
{
  const zeke_connection_t *conn = cbd->connection;
  zklock_t *lock = (zklock_t*)zeke_session_context(conn,ZKLOCK_SESSION_KEY);

  LOG("zklock_wait_parent fired lock=%s lock->pool=%s, state=%d",
    (lock ? "YES" : "NO"),(lock && lock->pool ? "YES" : "NO"),
    (int)(lock ? lock->state : -1));

  if(lock == NULL || lock->pool == NULL)
    return;

  if(lock->state >= ZKLOCK_CONNECTED &&
     lock->state <= ZKLOCK_WAIT && cbd->type == ZOO_CHILD_EVENT) {
    zk_status_t st = zeke_aget_children(NULL,conn,lock->parent,
                                          WATCHER(lock->flags),
                                          zklock_check_parent,
                                          lock);
    if(!ZEKE_STATUS_IS_OKAY(st)) {
      zeke_apr_error(apr_psprintf(cbd->pool,"watching: %s",lock->parent),st);
      zklock_shutdown(st,0);
    }
  }
}

static zeke_cb_t zklock_check_parent(const zeke_callback_data_t *cbd)
{
  zk_status_t st = cbd->status;
  zklock_t *lock = (zklock_t*)cbd->ctx;

  if(APR_STATUS_IS_ZEKE_ERROR(st) && ZEKE_TO_ZK_ERROR(st) == ZNONODE) {
    if(lock->flags & ZKLOCK_OPTION_MAKE_PATH) {
      apr_pool_t *pool = NULL;
      apr_array_header_t *temp;
      zeke_callback_data_t *mkpath_cbd = NULL;
      zklock_mkpath_t *mkpath = NULL;
      char *path,*cp;
      char *state = NULL;
      apr_size_t i;

      assert(apr_pool_create(&pool,cbd->pool) == APR_SUCCESS);
      zeke_pool_tag(pool,apr_psprintf(pool,"mkpath: %s",lock->parent));
      assert((mkpath =(zklock_mkpath_t*)apr_pcalloc(pool,sizeof(zklock_mkpath_t))) != NULL);
      assert((path = apr_pstrdup(pool,lock->parent)) != NULL);
      assert((temp = apr_array_make(pool,5,sizeof(const char*))) != NULL);
      mkpath->pool = pool;

      for(cp = apr_strtok(path,"/",&state); cp; cp = apr_strtok(NULL,"/",&state)) {
        for(; *cp && apr_isspace(*cp); cp++) ;
        if(*cp) {
          APR_ARRAY_PUSH(temp,const char*) = cp;
        }
      }

      if(temp->nelts == 0) {
        apr_pool_destroy(pool);
      } else {
        mkpath->lock = lock;
        assert((mkpath->stack = apr_array_make(pool,temp->nelts,temp->elt_size)) != NULL);
        for(i = 0; i < temp->nelts; i++) {
          APR_ARRAY_PUSH(mkpath->stack,const char*) = APR_ARRAY_IDX(temp,(temp->nelts-i)-1,const char*);
        }
        apr_array_clear(temp);

        /* fake a callback to start the path creation process, it's allocated from our callback pool
        so when it's complete it will also destroy our callback context */
        mkpath_cbd = zeke_callback_data_create_ex(lock->conn,NULL,cbd->pool,0);
        assert(mkpath_cbd != NULL);
        mkpath_cbd->ctx = mkpath;
        return zklock_mkpath(mkpath_cbd);
      }
    } else {
      zeke_eprintf("%s: node does not exist\n",lock->parent);
      zklock_shutdown(st,0);
    }
  } else {
    /* Got node contents and stat info, see if it's locked */
    assert(cbd->vectors.strings != NULL);
    if(cbd->vectors.strings->count == 0) {
      lock->state = ZKLOCK_TRANS;
      assert(cbd->stat != NULL);
      zklock_start_trans(lock,cbd->stat->version);
    } else {
      /* if set to skip then we just quietly exit now */
      if(WATCHER(lock->flags) == NULL)
        zeke_safe_shutdown(lock->exitcode);
      else if(lock->state >= ZKLOCK_CONNECTED && lock->state < ZKLOCK_WAIT)
        lock->state = ZKLOCK_WAIT;
    }
  }
  return ZEKE_CALLBACK_DESTROY;
}

static void zklock_setup(zeke_connection_t *conn, void *data)
{
  zk_status_t st;
  zklock_t *lock = (zklock_t*)data;

  lock->conn = conn;
  if(lock->state <= 0)
    lock->state = ZKLOCK_CONNECTED;

  if(!lock->session_key) {
    zklock_t *session = NULL;

    lock->session_key = ZKLOCK_SESSION_KEY;
    assert(lock->session_key != NULL);
    session = zeke_session_context_create(conn,ZKLOCK_SESSION_KEY,sizeof(zklock_t));
    ZEKE_ASSERT(session->pool == NULL, "session already exists for " ZKLOCK_SESSION_KEY);
    memcpy(session,lock,sizeof(zklock_t));
    lock = session;
  }

  if(lock->procattr == NULL) {
    st = apr_procattr_create(&lock->procattr,lock->pool);
    assert(st == APR_SUCCESS && lock->procattr != NULL);
    assert(apr_procattr_io_set(lock->procattr,APR_NO_PIPE,APR_NO_PIPE,APR_NO_PIPE) == APR_SUCCESS);
    assert(apr_procattr_cmdtype_set(lock->procattr,APR_SHELLCMD_ENV) == APR_SUCCESS);
  }

  st = zeke_aget_children(NULL,conn,lock->parent,WATCHER(lock->flags),zklock_check_parent,lock);
  if(!ZEKE_STATUS_IS_OKAY(st))
    zeke_fatal(lock->parent,st);
}

#if defined(ZKLOCK_DEBUG_TIMERS) && !defined(NDEBUG)
inline static void zklock_dump_timers(zktool_timer_t *timeouts, zktool_wait_t *waits)
{
  int i;

  for(i = 0; i < ZKLOCK_NTIMERS; i++) {
    if(ZKTOOL_IS_TIMER_SET(timeouts,i)) {
      zeke_eprintf("interval timer %d start=%"APR_TIME_T_FMT"ms stop=%"APR_TIME_T_FMT"ms delta=%"APR_TIME_T_FMT"ms\n",
                    i,waits[i].start/1000,waits[i].stop/1000,(waits[i].stop-waits[i].start)/1000);
    }
  }
}
#endif /* ZKLOCK_DUMP_TIMERS */

static void zklock_interval_timer(zeke_connection_t *conn, void *data)
{
  zklock_t *lock = (zklock_t*)data;
  int expired[ZKLOCK_NTIMERS];
  apr_size_t nexpired;

  if(conn && zeke_session_context_exists(conn,lock->session_key))
    lock = zeke_session_context(conn,lock->session_key);

#if defined(ZKLOCK_DEBUG_TIMERS) && !defined(NDEBUG)
  zklock_dump_timers(lock->timeouts,lock->waits);
#endif

  nexpired = zklock_update_timers(lock,NULL,expired,ZKLOCK_NTIMERS);

  if(nexpired > 0) {
    int t;
    apr_size_t i;

    for(i = 0; i < nexpired; i++) {
      switch((t = expired[i])) {
      case ZKLOCK_TIMER_LOCK:
        if(lock->state < ZKLOCK_CHILD) {
          zklock_shutdown_ex(127,0,
                             "timeout acquiring lock on %s (%s)",lock->path,
                  zktool_interval_format(ZKTOOL_STATIC_TIME_ELAPSED(lock->waits,t),lock->pool));
          } else if(lock->state > ZKLOCK_MKPATH)
            lock->timeouts[t] = APR_TIME_C(0);
        break;
      case ZKLOCK_TIMER_ZK:
        if(lock->state < ZKLOCK_WAIT) {
          zklock_shutdown_ex(126,0,
                             "zookeeper network timeout while acquiring lock on %s (%s)",lock->path,
                  zktool_interval_format(ZKTOOL_STATIC_TIME_ELAPSED(lock->waits,t),lock->pool));
        }
        break;
      case ZKLOCK_TIMER_MAX_RUN:
        if(lock->state == ZKLOCK_CHILD)
          apr_proc_other_child_refresh_all(APR_OC_REASON_RUNNING);
      }
    }
  }
}

static int start_zk(const char *zkhosts,zklock_t *lock)
{
  apr_pool_t *pool = NULL;
  apr_interval_time_t interval = APR_TIME_C(-1);
  zk_status_t st = ZEOK;
  zeke_runloop_callback_fn interval_timer = NULL;
  int i,rc = 0;

  assert(apr_pool_create(&pool,zklock_pool) == APR_SUCCESS);
  zeke_pool_tag(pool,"top-level i/o pool");

  if(zkhosts == NULL) {
    zkhosts = getenv("ZOOKEEPER_CLUSTER");
    if(zkhosts == NULL)
      zkhosts = getenv("ZOOKEEPER_ENSEMBLE");
    if(zkhosts == NULL) {
      zeke_eprintf("%s","cannot determine zookeeper ensemble, set ZOOKEEPER_CLUSTER or ZOOKEEPER_ENSEMBLE env vars\n");
      rc = 101;
      goto start_zk_exit;
    }
  }
  if(ZKTOOL_IS_TIMER_SET(lock->timeouts,ZKLOCK_TIMER_LOCK)) {
    zklock_reset_timers(lock,ZKLOCK_TIMER_LOCK,APR_TIME_C(-1));
  }

  for(i = 0; i < ZKLOCK_NTIMERS; i++)
    if(ZKTOOL_IS_TIMER_SET(lock->timeouts,i))
      if(interval == APR_TIME_C(-1) || interval > lock->timeouts[i]) {
        interval = lock->timeouts[i];
        interval_timer = &zklock_interval_timer;
      }

  st = zeke_app_run_loop_ex(zkhosts,APR_TIME_C(-1),interval,interval_timer,
                            NULL,&zklock_setup,NULL,ZEKE_RUNLOOP_DELAY_STARTUP_CALLBACK,
                            lock, NULL);
start_zk_exit:
  if(rc == 0) {
    if(!ZEKE_STATUS_IS_OKAY(st)) {
      if(APR_STATUS_IS_TIMEUP(st)) {
        zeke_printf("%s","timeout!\n");
        return 1;
      }
      if(!ZEKE_STATUS_IS_CONNECTION_CLOSED(st)) {
        zeke_apr_error("unhandled error while starting main event loop",st);
      }
      zklock_shutdown(st,0);
    }
  }
  if(pool)
    apr_pool_destroy(pool);
  return rc ? rc : st;
}

static void usage(int exitcode)
{
  if(!exitcode || exitcode >= 100) {
    zeke_printf("%s\n","zklock [options] [--] command-to-run <command-args>");
    zeke_printf("%s\n","  run a command under the control of a zookeeper lock, so that only one in a cluster");
    zeke_printf("%s\n%s\n","  may run at one time.","options:");
  }

  zktool_display_options(options,(!exitcode || exitcode >= 100 ? zstdout : zstderr),NULL);

  exit(exitcode);
}

static void zeke_sigchld_handler(int sig)
{
  /* NOOP, only exists so that apr_pollset_poll() will awaken */
}

static int zeke_interrupt(zeke_connection_t *conn)
{
  static apr_pool_t *p = NULL;
  apr_exit_why_e why;
  apr_proc_t proc;
  apr_status_t st;
  int code;

  if(!p)
    apr_pool_create(&p,zklock_pool);

  for(st = APR_CHILD_DONE; st == APR_CHILD_DONE; ) {
    if((st = apr_proc_wait_all_procs(&proc,&code,&why,APR_NOWAIT,p)) == APR_CHILD_DONE)
      apr_proc_other_child_alert(&proc, APR_OC_REASON_DEATH, ZKTOOL_MKSTATUS_OC(code,why));
  }

  apr_pool_clear(p);
  return 0;
}

static void zeke_shutdown_handler(int sig)
{
  raise_sig = sig;
  if(sig) {
    apr_signal(sig, SIG_DFL);
    zeke_atexit(zeke_exit_handler,&raise_sig);
    zeke_safe_shutdown_r(0);
  }
}

static void zeke_reflect_handler(int sig)
{
  if(sig && reflect_proc)
    apr_proc_kill(reflect_proc,sig);
}

ZEKE_HIDDEN int main(int argc, const char *const *argv, const char *const *env)
{
  zk_status_t st = zeke_init_cli(&argc,&argv,&env);
  apr_pool_t *pool = NULL;
  apr_getopt_t *option_parser = NULL;
  const char *zkhosts = NULL;
  const char *name = NULL;
  const char *namespace = "locks";
  const char *content = NULL;
  const char *path = NULL;
  const char *opt_arg = NULL;
  zktool_timer_t timeouts[ZKLOCK_NTIMERS];
  apr_uint32_t flags = 0;
  apr_array_header_t *args = NULL;
  zklock_t *lock = NULL;
  int opt,i;
  int exitcode = 0;
  apr_byte_t exitcode_xor = 0;

  assert(ZEKE_STATUS_IS_OKAY(st));
  assert((zklock_pool = zeke_root_subpool_create()) != NULL);
  assert(apr_pool_create(&pool,zklock_pool) == APR_SUCCESS);
#ifdef DEBUGGING
  zoo_set_debug_level(ZOO_LOG_LEVEL_INFO);
#elif defined(NDEBUG)
  zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);
#else
  zoo_set_debug_level(ZOO_LOG_LEVEL_WARN);
#endif

  zeke_pool_tag(pool,"getopt pool");
  memset(timeouts,0,sizeof(timeouts));
  timeouts[ZKLOCK_TIMER_ZK] = apr_time_from_sec(ZKLOCK_ZK_TIMEOUT);
  assert(apr_getopt_init(&option_parser,pool,argc,argv) == APR_SUCCESS);
  while((st = apr_getopt_long(option_parser,options,&opt,&opt_arg)) != APR_EOF) {
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
      case ZKLOCK_OPTION_DEBUG:
        zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
        LOG("foo");
        break;
      case ZKLOCK_OPTION_LOGFILE:
        st = zktool_set_logfile(opt_arg,zklock_pool);
        if(!ZEKE_STATUS_IS_OKAY(st))
          zeke_fatal(opt_arg,st);
        break;
      case ZKLOCK_OPTION_CONNECT:
        for(;apr_isspace(*opt_arg);opt_arg++) ;
        zkhosts = apr_pstrdup(zklock_pool,opt_arg);
        if(!*zkhosts) {
          zeke_eprintf("%s\n","Invalid connection string");
          exit(10);
        }
        break;
      case ZKLOCK_OPTION_ZK_TIMEOUT:
        st = zktool_parse_interval_time(&timeouts[ZKLOCK_TIMER_ZK],opt_arg,pool);
        if(!ZEKE_STATUS_IS_OKAY(st))
          zeke_fatal(opt_arg,st);
        break;
      case ZKLOCK_OPTION_MAXRUN:
        st = zktool_parse_interval_time(&timeouts[ZKLOCK_TIMER_MAX_RUN],opt_arg,pool);
        if(!ZEKE_STATUS_IS_OKAY(st))
          zeke_fatal(opt_arg,st);
        break;
      case 'T':
        st = zktool_parse_interval_time(&timeouts[ZKLOCK_TIMER_LOCK],opt_arg,pool);
        if(!ZEKE_STATUS_IS_OKAY(st))
          zeke_fatal(opt_arg,st);
        break;
      case 'c':
        content = apr_pstrdup(zklock_pool,opt_arg);
        break;
      case 'n':
        name = apr_pstrdup(zklock_pool,opt_arg);
        if(strchr(name,'/')) {
          zeke_eprintf("'%s' is an invalid name.\n",name);
          exit(10);
        }
        break;
      case ZKLOCK_OPTION_NS:
        do {
          const char *cp = opt_arg;
          while(*cp && apr_isspace(*cp)) cp++;
          if(!*cp) {
            zeke_eprintf("'%s' is an invalid namespace.\n",opt_arg);
            exit(10);
          }
          if(*cp == '/') {
            while(*cp && *cp == '/') cp++;
            namespace = apr_pstrdup(zklock_pool,cp);
          } else
            namespace = zeke_nodepath_join(zklock_pool,namespace,cp,NULL);
        } while(0);
        if(namespace == NULL) {
          zeke_eprintf("'%s' is an invalid namespace.\n",opt_arg);
          exit(10);
        }
        break;
      case 'P':
        do {
          const char *cp = opt_arg;
          while(*cp && (apr_isspace(*cp) || *cp == '/')) cp++;
          if(!*cp) {
            zeke_eprintf("'%s' is an invalid path.\n",opt_arg);
            exit(11);
          }
          path = apr_pstrdup(zklock_pool,cp);
          assert(path != NULL);
        } while(0);
        break;
      case 'p':
        opt = ZKLOCK_OPTION_MAKE_PATH;
      case ZKLOCK_OPTION_NO_WAIT:
        flags |= (apr_uint32_t)opt;
        break;
      case 'N':
        flags |= (apr_uint32_t)ZKLOCK_OPTION_NO_WAIT;
        break;
      case 'E':
      case ZKLOCK_OPTION_NO_WAIT_EXIT:
        do {
          char *end = NULL;
          apr_int64_t val;
          flags |= (apr_uint32_t)ZKLOCK_OPTION_NO_WAIT;
          val = apr_strtoi64(opt_arg,&end,0);
          if((!end || *end) || val < 0 || val > 255) {
            zeke_eprintf("'%s' is not a valid exit code.\n",opt_arg);
            exit(12);
          }
          exitcode = (int)val;
        } while(0);
        break;
      case ZKLOCK_OPTION_XOR:
        do {
          char *end = NULL;
          apr_int64_t val;
          val = apr_strtoi64(opt_arg,&end,0);
          if((!end || *end) || val < 0 || val > 255) {
            zeke_eprintf("'%s' is not a valid exit code xor value (range: 0-255)\n",opt_arg);
            exit(13);
          }
          exitcode_xor = (apr_byte_t)val;
        } while(0);
        break;
      default:
        zeke_eprintf("%s\n","this should not occur");
        abort();
      }
      break;
    default:
      zeke_fatal("cli options",st);
      break;
    }
  }

  if(name == NULL) {
    name = zeke_get_fqdn(zklock_pool);
    ZEKE_ASSERT(name != NULL,"cannot determined fully-qualified domain name");
  }

  if(path)
    path = zeke_nodepath_join(zklock_pool,namespace,path,NULL);
  else
    path = namespace;

  assert(path != NULL);
  args = apr_array_make(zklock_pool,(option_parser->argc-option_parser->ind)+1
                                                       ,sizeof(const char*));
  for(i = option_parser->ind; i < option_parser->argc; i++) {
    APR_ARRAY_PUSH(args,const char*) = apr_pstrdup(zklock_pool,option_parser->argv[i]);
  }

  if(args->nelts == 0)
    usage(100);
  if(content == NULL) {
    content = apr_array_pstrcat(zklock_pool,args,' ');
    assert(content != NULL && *content);
  }
  APR_ARRAY_PUSH(args,const char*) = NULL;

  apr_pool_clear(pool);
  lock = (zklock_t*)apr_pcalloc(pool,sizeof(zklock_t));
  assert(lock != NULL);

  lock->pool = pool;
  lock->name = name;

  if(path && *path != '/')
    lock->parent = zeke_nodepath_join(pool,path,NULL);
  else
    lock->parent = path;
  lock->ns = namespace;
  lock->path = zeke_nodepath_join(pool,path,name,NULL);
  lock->content = content;
  lock->command = APR_ARRAY_IDX(args,0,const char*);
  lock->args = args;
  lock->flags = flags;
  lock->exitcode = exitcode;
  lock->exitcode_xor = exitcode_xor;
  memcpy(lock->timeouts,timeouts,sizeof(lock->timeouts));

#if defined(ZLOCK_DEBUG_TIMERS) && !defined(NDEBUG)
  if(ZKLOCK_IS_TIMER_SET(lock->timeouts,ZKLOCK_TIMER_LOCK)) {
    zeke_printf("lock timeout is %"APR_TIME_T_FMT" usec\n",lock->timeouts[ZKLOCK_TIMER_LOCK]);
    return 0;
  }
#endif

  assert(apr_pool_userdata_setn(lock,ZKLOCK_SESSION_KEY,NULL,pool) == APR_SUCCESS);
  apr_signal(SIGCHLD,zeke_sigchld_handler);
  apr_signal(SIGTERM,zeke_shutdown_handler);
  apr_signal(SIGHUP,zeke_reflect_handler);
  apr_signal(SIGINT,zeke_reflect_handler);
  apr_signal(SIGQUIT,zeke_reflect_handler);
  apr_signal(SIGUSR1,zeke_reflect_handler);
  apr_signal(SIGUSR2,zeke_reflect_handler);
  apr_signal(SIGPIPE,SIG_IGN);
  apr_signal(SIGALRM,SIG_DFL);
  apr_signal_unblock(SIGCHLD);
  apr_signal_unblock(SIGINT);
  apr_signal_unblock(SIGTERM);
  apr_signal_unblock(SIGHUP);
  apr_signal_unblock(SIGUSR1);
  apr_signal_unblock(SIGUSR2);
  apr_signal_unblock(SIGPIPE);
  apr_signal_unblock(SIGQUIT);
  apr_signal_unblock(SIGALRM);
  zeke_interrupt_handler_set(zeke_interrupt);
  return start_zk(zkhosts,lock);
}

/* vi: :set sts=2 sw=2 ai et tw=0: */

