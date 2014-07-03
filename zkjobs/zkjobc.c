/* client for submitting/querying a zkjobq zookeeper node */
#include "zkjobs.h"
#include <apr_user.h>

#define ZKJOBQ_SESSION_KEY "zkjobq_session"

#ifndef APR_ARRAY_POP
#define APR_ARRAY_POP(ary,type) (*((type*)apr_array_pop(ary)))
#endif

#ifndef SUBNODE_ELECTION
#define SUBNODE_ELECTION "election"
#endif

#ifndef SUBNODE_QUEUE
#define SUBNODE_QUEUE "queue"
#endif

enum zkjobopt {
  OPT_DEBUG = 1,
  OPT_NS = 2,
  OPT_FLAGS = 'F',
  OPT_HELP = 'h',
  OPT_COMMAND = 'c',
  OPT_SHELL = 's',
  OPT_FILE = 'f',
  OPT_USER = 'u',
  OPT_GROUP = 'g',
  OPT_INTERP = 'I',
  OPT_ZKHOSTS = 'C',
  OPT_TIMEOUT = 'T',
  OPT_WRAPPER = 'w',
};

typedef struct zkjobq {
  apr_pool_t *pool;
  zeke_connection_t *conn;
  const char *session_key;
  const char *cmdline;
  const char *node, *enode, *qnode;
  const char *srcfile;
  const char *interp,*wrapper;
  const char *user,*group;
  apr_table_t *header;
  struct zkws *ws;
  zkqueue_t *q;
  apr_interval_time_t timeout;
  apr_uint32_t flags;
} zkjobq_t;

#define ZKJOBQ_WS_SIZE 4
struct zkjobq_ws {
  apr_uint32_t magic;
#define ZKJOBQ_WS_MAGIC 0x99efab01
  struct zkws *ws;
  struct zkjobq *jobq;
  void *a[ZKJOBQ_WS_SIZE];
};

static apr_pool_t *zkjob_pool = NULL;

static apr_getopt_option_t options[] = {
  { "ns",            OPT_NS,              1,
    "Set the zookeeper namespace of the job dispatch node"},
  { "command",       OPT_COMMAND,         1,
    "Schedule a one-line command to be executed by a shell, may included shell syntax."},
  { "shell",         OPT_SHELL,           0,
    "For the use of a shell irrespective of shell meta character detection"},
  { "timeout",       OPT_TIMEOUT,         1,
    "Specify maximum amount of time this job will be allowed to run (ex: -T5m or -T120s)"},
  { "file",          OPT_FILE,            1,
    "Read job from a file, use - for stdin"},
  { NULL,            OPT_USER,            1,
    "Run command as a specified user (requires root)"},
  { NULL,            OPT_GROUP,           1,
    "Run command as a specified group (requires root)"},
  { NULL,            OPT_INTERP,          1,
    "Specifies absolute path to an alterate shell or script interpreter"},
  { NULL,            OPT_WRAPPER,         1,
    "Pipe job error output through a wrapper script"},
  { "flags",         OPT_FLAGS,           1,
    "Specifies specific job flags as a string of single characters (use --flags=: for a list)"},
  { "connect",       OPT_ZKHOSTS,         1,
    "Override zookeeper connection string (default is from $ZOOKEEPER_ENSEMBLE)"},
  { "debug",         OPT_DEBUG,           0,
    "Enabled debug mode."},
  { "help",          OPT_HELP,            0,
    "Display this help"},
  { NULL, 0, 0, NULL }
};

DECLARE_CONTEXT(zkjobq_t)

#define ZKJOBQ(cbd) cast_context_to_zkjobq_t((zeke_context_t*)(cbd)->ctx)

static const struct zkjobq_ws *zkjobq_make_arg(struct zkjobq *jobq,
                                               struct zkws *ws, ...)
{
  struct zkjobq_ws *arg;
  int i;
  va_list ap;

  arg = zk_workspace_alloc(ws,sizeof(*arg));
  AN(arg);
  ZK_CHECK_OBJ_NOTNULL(ws, ZK_WS_MAGIC);
  arg->magic = ZKJOBQ_WS_MAGIC;
  arg->jobq = jobq;
  arg->ws = ws;
  for(i = 0; i < ZKJOBQ_WS_SIZE; i++)
    arg->a[i] = 0;
  va_start(ap,ws);
  for(i = 0; i < ZKJOBQ_WS_SIZE; i++) {
    void *v = va_arg(ap,void*);
    if(v != NULL)
      arg->a[i] = v;
    else
      break;
  }
  va_end(ap);
  return arg;
}

static zeke_cb_t job_submitted(const zeke_callback_data_t *cbd)
{
  zk_status_t st = zeke_callback_status_get(cbd);
  const struct zkjobq_ws *arg = (struct zkjobq_ws*)cbd->ctx;
  const char *node;

  AN(arg); AN(arg->a[0]);
  ZK_CHECK_OBJ_NOTNULL(arg,ZKJOBQ_WS_MAGIC);
  node = (const char*)arg->a[0];

  AN(arg->jobq);
  if(!ZEKE_STATUS_IS_OKAY(st))
    zeke_fatal(node,st);
  if(cbd->value)
    node = cbd->value;
  zeke_printf("%s submitted\n",node);
  ZK_CHECK_OBJ_NOTNULL(arg->jobq->ws, ZK_WS_MAGIC);
  zk_workspace_clear(&arg->jobq->ws);
  zeke_safe_shutdown(0);
  DESTROY;
}

static int zk_hdr_join(void *wsv, const char *key, const char *val)
{
  apr_off_t *op;
  struct zkws *ws = (struct zkws*)wsv;
  AN(key); AN(val);
  while(apr_isspace(*key)) key++;
  while(apr_isspace(*val)) val++;
  if(*key && *val) {
    char *b,*cp;
    apr_size_t u,sz;

    ZK_CHECK_OBJ_NOTNULL(ws, ZK_WS_MAGIC);
    op = (apr_off_t*)ws->data;
    AN(op);
    sz = strlen(key) + strlen(val) + 4;
    u = zk_workspace_reserve(ws,sz);
    assert(u >= sz);
    b = ws->b + *op;
    cp = apr_cpystrn(b,key,sz);
    cp = apr_cpystrn(cp,": ",3);
    cp = apr_cpystrn(cp,val,sz);
    *cp++ = '\n';
    *cp = '\0';
    assert(cp < ws->e);
    *op = (cp - ws->b);
    zk_workspace_release(ws,cp - ws->f);
  }
  return 1;
}

static void zkj_submit_job(struct zkjobq *jobq, apr_pool_t *p)
{
  zk_status_t st;
  apr_file_t *f = NULL;
  apr_array_header_t *buffer = NULL;
  char *hdr,*b;
  apr_ssize_t bsz,sz;
  apr_off_t bst,o = 0;
  const char *s;
  char datestr[APR_CTIME_LEN+1];
  const struct zkjobq_ws *arg;

  ZK_CHECK_OBJ_NOTNULL(jobq->ws,ZK_WS_MAGIC);
  zk_workspace_clear(&jobq->ws);
  if(jobq->header == NULL)
    jobq->header = apr_table_make(jobq->pool,5);

  AAOK(apr_ctime(datestr,apr_time_now()));
  if(jobq->cmdline)
    apr_table_setn(jobq->header,"command",jobq->cmdline);
  if(jobq->interp)
    apr_table_setn(jobq->header,"shell",jobq->interp);
  if(jobq->wrapper)
    apr_table_setn(jobq->header,"wrapper",jobq->wrapper);

  apr_table_setn(jobq->header,"date",datestr);

  s = zkjobq_flags_unparse(jobq->flags, jobq->ws);
  if(s != NULL)
    apr_table_setn(jobq->header,"flags",apr_pstrdup(jobq->pool,s));
  if(jobq->user != NULL)
    apr_table_setn(jobq->header,"user",jobq->user);
  if(jobq->group != NULL)
    apr_table_setn(jobq->header,"group",jobq->group);
  if(jobq->timeout > APR_TIME_C(-1))
    apr_table_setn(jobq->header,"timeout",
                  apr_psprintf(jobq->pool,"%"APR_TIME_T_FMT,jobq->timeout));
  zk_workspace_reset(jobq->ws);

  buffer = apr_array_make(jobq->pool,100,2048);
  if(jobq->srcfile) {
    if(strcmp(jobq->srcfile,"-") == 0)
      f = zstdin;
    else {
      st = apr_file_open(&f,jobq->srcfile,APR_READ,APR_OS_DEFAULT,p);
      if(st != APR_SUCCESS) {
        zeke_apr_error(jobq->srcfile,st);
        return;
      }
    }
  }
  jobq->ws->data = &o;
  zk_workspace_reserve(jobq->ws,0);
  o = bst = jobq->ws->f - jobq->ws->b;
  hdr = jobq->ws->b + bst;
  zk_workspace_release(jobq->ws,0);
  apr_table_do(zk_hdr_join,jobq->ws,jobq->header,NULL);
  assert(jobq->ws->data == (void*)&o);
  zk_workspace_reserve(jobq->ws,2);
  *(jobq->ws->b+o) = '\n';
  *(jobq->ws->b+o+1) = '\0';
  zk_workspace_release(jobq->ws,2);
  hdr = jobq->ws->b + bst;
  b = NULL; /* compiler warning meh */


  for(sz = strlen(hdr),bsz = 0; sz > 0; sz -= 2048) {
    b = apr_array_push(buffer);
    memcpy(b,hdr,(sz > 2048 ? 2048 : sz));
    bsz += (sz > 2048 ? 2048 : sz);
    if(sz < 2048) {
      b += (sz > 2048 ? 2048 : sz);
    } else if(sz == 2048) {
      b = apr_array_push(buffer);
      sz = 1;
    }
  }
  zk_workspace_clear(&jobq->ws);
  if(f) {
    apr_size_t bread,max;
    for(st = APR_SUCCESS; st != APR_EOF; ) {
      if(sz == 0)
        b = apr_array_push(buffer);
      max = (sz < 0 ? (-1 * sz) : 2048);
      st = apr_file_read_full(f,b,max,&bread);
      LOG("READ: max=%d sz=%d bread=%d",(int)max,(int)sz,(int)bread);
      sz = 0;
      bsz += bread;
      b += bread;
    }
    if(bread == max)
      b = apr_array_push(buffer);
  }
  *b++ = '\0';
  bsz++;
  s = zk_workspace_nodecat(jobq->ws,jobq->qnode,"job_",NULL);
  AN(s);
  arg = zkjobq_make_arg(jobq,jobq->ws,s,NULL);
  AN(arg);
  LOG("BUFFER=%s, SIZE=%lu",(char*)buffer->elts,(unsigned long)bsz);
  st = zeke_acreate(NULL,jobq->conn,s,(char*)buffer->elts,
                    bsz,NULL,ZOO_SEQUENCE,job_submitted,arg);
  if(!ZEKE_STATUS_IS_OKAY(st))
    zeke_fatal(s,st);
}

static zeke_cb_t zkj_check_election_exists(const zeke_callback_data_t *cbd)
{
  zk_status_t st = zeke_callback_status_get(cbd);
  const struct zkjobq_ws *arg = (struct zkjobq_ws*)cbd->ctx;

  ZK_CHECK_OBJ_NOTNULL(arg,ZKJOBQ_WS_MAGIC);
  if(!ZEKE_STATUS_IS_OKAY(st)) {
    AN(arg->jobq);
    zeke_apr_eprintf(st,"election node %s",(const char*)arg->a[0]);
    zeke_safe_shutdown(ZKE_TO_ERROR(st));
    DESTROY;
  }
  AZ(arg->jobq->enode);
  arg->jobq->enode = apr_pstrdup(arg->jobq->pool,(const char*)arg->a[0]);
  if(arg->jobq->enode && arg->jobq->qnode)
    zkj_submit_job(arg->jobq,cbd->pool);
  DESTROY;
}

static zeke_cb_t zkj_check_queue_exists(const zeke_callback_data_t *cbd)
{
  zk_status_t st = zeke_callback_status_get(cbd);
  const struct zkjobq_ws *arg = (struct zkjobq_ws*)cbd->ctx;

  ZK_CHECK_OBJ_NOTNULL(arg,ZKJOBQ_WS_MAGIC);
  if(!ZEKE_STATUS_IS_OKAY(st)) {
    AN(arg->jobq);
    zeke_apr_eprintf(st,"queue node %s",(const char*)arg->a[0]);
    zeke_safe_shutdown(ZKE_TO_ERROR(st));
    DESTROY;
  }
  AZ(arg->jobq->qnode);
  arg->jobq->qnode = apr_pstrdup(arg->jobq->pool,(const char*)arg->a[0]);
  if(arg->jobq->qnode && arg->jobq->enode)
    zkj_submit_job(arg->jobq,cbd->pool);
  DESTROY;
}

static zeke_cb_t zkj_check_exists(const zeke_callback_data_t *cbd)
{
  zk_status_t st = zeke_callback_status_get(cbd);
  struct zkjobq *jobq = ZKJOBQ(cbd);
  const char *node;

  AN(jobq);
  ZK_CHECK_OBJ_NOTNULL(jobq->ws, ZK_WS_MAGIC);
  node = (char*)jobq->ws->data;
  jobq->ws->data = NULL;
  if(ZEKE_STATUS_IS_OKAY(st)) {
    const struct zkjobq_ws *arg;
    char *enode, *qnode;
    jobq->node = apr_pstrdup(jobq->pool,node);
    node = jobq->node;
    zk_workspace_clear(&jobq->ws);

    enode = zk_workspace_nodecat(jobq->ws,jobq->node,SUBNODE_ELECTION,NULL);
    qnode = zk_workspace_nodecat(jobq->ws,jobq->node,SUBNODE_QUEUE,NULL);
    AN(enode); AN(qnode);
    INFO("election node should be: %s",enode);
    INFO("queue node should be: %s",qnode);
    arg = zkjobq_make_arg(jobq,jobq->ws,enode,NULL);
    ZK_CHECK_OBJ_NOTNULL(arg,ZKJOBQ_WS_MAGIC);
    st = zeke_aexists(NULL,jobq->conn,enode,NULL,zkj_check_election_exists,arg);
    if(!ZEKE_STATUS_IS_OKAY(st)) {
      zeke_apr_error(enode,st);
      zeke_safe_shutdown(st);
      DESTROY;
    }
    arg = zkjobq_make_arg(jobq,jobq->ws,qnode,NULL);
    ZK_CHECK_OBJ_NOTNULL(arg,ZKJOBQ_WS_MAGIC);
    st = zeke_aexists(NULL,jobq->conn,enode,NULL,zkj_check_queue_exists,arg);
    if(!ZEKE_STATUS_IS_OKAY(st)) {
      zeke_apr_error(qnode,st);
      zeke_safe_shutdown(st);
      DESTROY;
    }
    DESTROY;
  } else
    zeke_apr_error(node,st);
  zeke_safe_shutdown(st);
  DESTROY;
}

static void zkj_setup(zeke_connection_t *conn, void *data)
{
  struct zkjobq *jobq = cast_context_to_zkjobq_t(data);
  zk_status_t st = ZEOK;
  const char *node;

  AN(jobq);
  if(jobq->conn == NULL) {
    jobq->conn = conn;
    zeke_pool_cleanup_indirect(zeke_connection_pool_get(conn),jobq->conn);
  }
  if(jobq->session_key == NULL) {
    zkjobq_t *session = NULL;
    session = zeke_session_context_create(conn,ZKJOBQ_SESSION_KEY,sizeof(*session));
    ZEKE_ASSERTV(session->pool == NULL,"session already exists for %s",ZKJOBQ_SESSION_KEY);
    jobq->session_key = ZKJOBQ_SESSION_KEY;
    memcpy(session,jobq,sizeof(*session));
    zeke_pool_cleanup_indirect(zeke_connection_pool_get(conn),session->conn);
    jobq = session;
  }
  ZK_CHECK_OBJ_NOTNULL(jobq->ws, ZK_WS_MAGIC);
  AN(jobq->ws->data);
  node = (char*)jobq->ws->data;
  st = zeke_aexists(NULL,jobq->conn,node,NULL,zkj_check_exists,jobq);
  if(!ZEKE_STATUS_IS_OKAY(st)) {
    zeke_fatal(zk_workspace_strcat(jobq->ws,"zeke_aexists ",node,NULL),st);
  }
}

static void zkj_abort(zeke_connection_t *conn, void *data)
{
  struct zkjobq *jobq = cast_context_to_zkjobq_t(data);
  const char *node;

  AN(jobq);
  ZK_CHECK_OBJ_NOTNULL(jobq->ws, ZK_WS_MAGIC);
  node = (char*)jobq->ws->data;
  if(!node) node = jobq->node;

  zeke_eprintf("Timeout communicating with zookeeper for job queue at %s\n",node);
  zeke_safe_shutdown(2);
}

static
int start_zk(const char *zkhosts, struct zkjobq *jobq, const char *node)
{
  apr_uid_t uid;
  apr_gid_t gid;
  apr_pool_t *p = jobq->pool;
  zk_status_t st = ZEOK;
  zeke_runloop_callback_fn setup_func = NULL;
  zeke_runloop_callback_fn interval_func = NULL;
  int rc = 0;

  zeke_pool_tag(p,"runtime pool");
  ZK_CHECK_OBJ_NOTNULL(jobq->ws,ZK_WS_MAGIC);
  AN(node);
  setup_func = zkj_setup;
  interval_func = zkj_abort;

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

  if(jobq->user == NULL) {
    char *username = NULL,*groupname = NULL;
    st = apr_uid_current(&uid,&gid,jobq->pool);
    if(st == APR_SUCCESS)
      st = apr_uid_name_get(&username, uid, jobq->pool);
    if(st != APR_SUCCESS || !username || !*username) {
      zeke_apr_eprintf(st,"cannot determine current user and group");
      jobq->user = "nobody";
    } else
      jobq->user = username;
    if(jobq->group == NULL) {
      if(st == APR_SUCCESS) {
        st = apr_gid_name_get(&groupname,gid,jobq->pool);
        if(st != APR_SUCCESS)
          zeke_apr_eprintf(st,"cannot determine groupname");
        else
          jobq->group = groupname;
      }
    }
  }

  jobq->ws->data = (void*)node;
  st = zeke_app_run_loop_interval(zkhosts,
                                  apr_time_from_sec(20),
                                  interval_func,
                                  setup_func,
                                  ZEKE_RUNLOOP_DELAY_STARTUP_CALLBACK,jobq);

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
  return rc;
}

static
void usage(int exitcode)
{
  if(!exitcode || exitcode >= 100) {
    zeke_printf("%s\n","zkjobc [options] <zkjobq-zookeeper-nodepath> [job]");
    zeke_printf("%s\n","  Submits jobs for distributed processing by hosts running zkjobd");
    zeke_printf("%s\n","options:");
  }

  zktool_display_options(options,(!exitcode || exitcode >= 100 ? zstdout : zstderr),NULL);

  exit(exitcode);
}

static inline const char *validate_node(const char *path, const char *msg)
{
  if(path) {
    apr_size_t sz;
    while(*path == '/') path++;
    sz = strlen(path);
    if(sz > 0 && *(path+sz-1) == '/') {
      if(msg != NULL) {
        zeke_eprintf(msg,path);
        zeke_eprintf("\n");
      }
      path = NULL;
    }
  }

  return path;
}

static inline const char *validate_nodep(apr_pool_t *p,const char *path, const char *msg)
{
  path = validate_node(path,msg);
  if(path)
    return apr_pstrdup(p,path);
  return path;
}

ZEKE_HIDDEN
int main(int argc, const char * const *argv, const char * const *env)
{
  zk_status_t st = zeke_init_cli(&argc, &argv, &env);
  apr_pool_t *pool = NULL;
  apr_pool_t *getopt_pool = NULL;
  apr_getopt_t *getopt = NULL;
  int i,opt;
  struct zkjobq jobq;
  const char *ns = NULL;
  const char *zkhosts = NULL;
  const char *opt_arg;

  AZOK(st);
  AN(zkjob_pool = zeke_root_subpool_create());
  AAOK(apr_pool_create(&pool,zkjob_pool));
  AN(pool);
  AAOK(apr_pool_create(&getopt_pool,pool));
  AAOK(apr_getopt_init(&getopt,getopt_pool,argc,argv));

  memset(&jobq,0,sizeof(jobq));

  AAOK(apr_pool_create(&jobq.pool,zkjob_pool));
  jobq.timeout = APR_TIME_C(-1);

#ifdef DEBUGGING
  zoo_set_debug_level(ZOO_LOG_LEVEL_INFO);
#elif defined(NDEBUG)
  zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);
#else
  zoo_set_debug_level(ZOO_LOG_LEVEL_WARN);
#endif

  while((st = apr_getopt_long(getopt,options,&opt,&opt_arg)) != APR_EOF) {
    switch(st) {
    case APR_BADCH:
    case APR_BADARG:
      usage(1);
      break;
    case APR_SUCCESS:
      switch(opt) {
      case OPT_HELP:
        usage(0);
        break;
      case OPT_ZKHOSTS:
        for(;apr_isspace(*opt_arg); opt_arg++)
          ;
        zkhosts = apr_pstrdup(pool,opt_arg);
        if(!*zkhosts) {
          zeke_eprintf("Invalid connection string: %s\n",opt_arg);
          exit(10);
        }
        break;
      case OPT_DEBUG:
        zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
        break;
      case OPT_NS:
        ns = validate_nodep(pool,opt_arg,"%s is not a valid namespace.");
        break;
      case OPT_COMMAND:
        for(;apr_isspace(*opt_arg); opt_arg++)
          ;
        if(!*opt_arg) {
          zeke_eprintf("Empty command line.");
          exit(11);
        }
        jobq.cmdline = apr_pstrdup(pool,opt_arg);
        break;
      case OPT_WRAPPER:
        for(;apr_isspace(*opt_arg); opt_arg++)
          ;
        if(*opt_arg)
          jobq.wrapper = apr_pstrdup(pool,opt_arg);
        break;
      case OPT_SHELL:
        jobq.flags |= ZKPROCF_USE_SHELL;
        break;
      case OPT_FLAGS:
        do {
          apr_uint32_t extra_flags = 0;
          st = zkjobq_flags_parse(opt_arg,&extra_flags);
          if(st != APR_SUCCESS) {
            zeke_apr_error(opt_arg,st);
            exit(11);
          }
          if (extra_flags == 0) {
            zeke_eprintf("No valid flags in '%s'. Flags are:\n",opt_arg);
            zkjobq_flags_print(1);
            exit(11);
          }
          jobq.flags |= extra_flags;
        } while(0);
        break;
      case OPT_INTERP:
        for(;apr_isspace(*opt_arg); opt_arg++)
          ;
        if(!*opt_arg || *opt_arg != '/') {
          zeke_eprintf("Invalid interpreter: %s",opt_arg);
          exit(15);
        }
        jobq.interp = apr_pstrdup(pool,opt_arg);
        break;
      case OPT_FILE:
        do {
          char *s = apr_pstrdup(getopt_pool,opt_arg);
          apr_collapse_spaces(s,s);
          if(strcmp(s,"-") != 0) {
            apr_finfo_t finfo;
            st = apr_stat(&finfo, opt_arg, APR_FINFO_TYPE|APR_FINFO_PROT, getopt_pool);
            if(st != APR_SUCCESS) {
              zeke_apr_error(opt_arg,st);
              exit(16);
            }
            if(finfo.filetype != APR_REG) {
              zeke_eprintf("%s: not a regular file\n",opt_arg);
              exit(16);
            }
            jobq.srcfile = apr_pstrdup(pool,opt_arg);
          } else
            jobq.srcfile = apr_pstrdup(pool,s);
        } while(0);
        break;
      case OPT_TIMEOUT:
        st = zktool_parse_interval_time(&jobq.timeout, opt_arg, getopt_pool);
        if(!ZEKE_STATUS_IS_OKAY(st)) {
          zeke_apr_eprintf(st,"'%s' is an invalid timeout",opt_arg);
          exit(17);
        }
        break;
      case OPT_USER:
        AN(jobq.pool);
        if(jobq.header == NULL)
          jobq.header = apr_table_make(jobq.pool,5);
        for(;apr_isspace(*opt_arg); opt_arg++)
          ;
        if(!apr_isalnum(*opt_arg)) {
          zeke_eprintf("%s: invalid username.\n",opt_arg);
          exit(20);
        }
        if(geteuid() != 0) {
          zeke_eprintf("Must be running as root to submit jobs as a different user.\n");
          exit(20);
        }
        apr_table_set(jobq.header,"user",opt_arg);
        break;
      case OPT_GROUP:
        AN(jobq.pool);
        if(jobq.header == NULL)
          jobq.header = apr_table_make(jobq.pool,5);
        for(;apr_isspace(*opt_arg); opt_arg++)
          ;
        if(!apr_isalnum(*opt_arg)) {
          zeke_eprintf("%s: invalid groupname.\n",opt_arg);
          exit(21);
        }
        if(geteuid() != 0) {
          zeke_eprintf("Must be running as root to submit jobs as a different group.\n");
          exit(21);
        }
        apr_table_set(jobq.header,"group",opt_arg);
        break;
      default:
        zeke_eprintf("unsupported cli option %d\n",(int)opt);
        abort();
      }
      break;
    default:
      zeke_fatal("cli options",st);
      break;
    }
  }

  for(i = getopt->argc-1; i >= getopt->ind; i--)
    if(getopt->argv[i] != NULL) {
      if(i > getopt->ind && jobq.cmdline == NULL)
        jobq.cmdline = apr_pstrdup(pool,getopt->argv[i]);
      else if(jobq.node == NULL)
        jobq.node = validate_nodep(pool,getopt->argv[i],"%s is not a valid node");

      if(jobq.cmdline && jobq.node)
        break;
    }

  if(!jobq.node || (!jobq.cmdline && !jobq.srcfile)) {
    zeke_eprintf("no job specified or not jobq zookeeper node\n");
    usage(5);
  }
  jobq.ws = zk_workspace_new(jobq.pool);
  if(ns)
    AAOK(zeke_nodepath_chroot_set(ns));

  jobq.node = zk_workspace_nodecat(jobq.ws,jobq.node,NULL);
  AN(jobq.node);
  i = start_zk(zkhosts,&jobq,jobq.node);
  apr_pool_destroy(zkjob_pool);
  return i;
}
