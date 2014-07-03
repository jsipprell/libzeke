#include "zkjobs.h"
#include "libzeke_log.h"

#ifdef DEBUGGING
#include "internal.h"
#endif

#include <apr_atomic.h>

#if defined(ZEKE_USE_THREADS) && defined(HAVE_EVENT2_THREAD_H)
#include <event2/thread.h>
#endif

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
  OPT_DISABLE_SHMLOG = 3,
  OPT_CONCURRENT = 'N',
  OPT_HELP = 'h',
  OPT_ZKHOSTS = 'C',
  OPT_IDENT = 'i',
  OPT_MAX_RUN = 'R',
  OPT_FOREGROUND = 'F',
  OPT_DAEMONIZE = 'D',
};

typedef struct zkjobq {
  apr_pool_t *pool;
  const char *ident; /* NULL = use fqdn */
  zeke_connection_t *conn;
  const char *session_key;
  const char *ns;
  const char *node, *enode, *qnode;
  struct zkws *ws;
  zkelection_t *e;
  zkqueue_t *q;
  apr_interval_time_t etimeout,max_run;
  apr_uint32_t flags;
  unsigned max_concurrent_jobs;
  int njobs;
  int daemonize;
  unsigned debug, no_shmlog;
} zkjobq_t;

#ifdef LIBZEKE_USE_LIBEVENT
struct sigevent_callback {
  apr_pool_t *pool;
  const zkjobq_t *jobq;
  struct event *ev;
};
#endif

#define ZKJ_F_WINNER (1 << 0)

#define ZKJOBQ_WS_SIZE 4
struct zkjobq_ws {
  apr_uint32_t magic;
#define ZKJOBQ_WS_MAGIC 0x99efab01
  struct zkws *ws;
  struct zkjobq *jobq;
  void *a[ZKJOBQ_WS_SIZE];
};

#ifndef EXEC_TMPDIR
#define EXEC_TMPDIR "/var/tmp"
#endif

static int raise_sig = 0;
static apr_pool_t *zkjob_pool = NULL;
static const char *exec_temp_dir = EXEC_TMPDIR;

static apr_getopt_option_t options[] = {
  { "ns",            OPT_NS,              1,
    "Set the zookeeper namespace of the job dispatch node"},
  { "concurrency",   OPT_CONCURRENT,      1,
    "Set the maximum number of jobs that can run simultaneously on this host"},
  { "ident",         OPT_IDENT,           1,
    "Specific host identity (default is hostname in FQDN format)"},
  { "connect",       OPT_ZKHOSTS,         1,
    "override zookeeper connection string (default is from env $ZOOKEEPER_ENSEMBLE)"},
  { "max-run",       OPT_MAX_RUN,         1,
    "set the maximum number of seconds a job may run (default is unlimited)"},
  { NULL,            OPT_FOREGROUND,      0,
    "Run zkjobd in the foreground (default with debug off is to run in bg)"},
  { NULL,            OPT_DAEMONIZE,       0,
    "Run zkjobd in the background, this is the default unless --debug is used"},
  { "debug",         OPT_DEBUG,           0,
    "Enable debug mode (also switches to foreground without -D)" },
  { "disable-shm",   OPT_DISABLE_SHMLOG,  0,
    "Disable shared memory logging ring buffer (usually combined with -F)"},
  { "help",          OPT_HELP,            0,
    "Display this help" },
  { NULL,            0,                   0, NULL }
};

DECLARE_CONTEXT(zkjobq_t)

#define ZKJOBQ(cbd) cast_context_to_zkjobq_t((zeke_context_t*)(cbd)->ctx)

static void zeke_sigchld_handler(int sig)
{
  /* NOOP, only exists so that apr_pollset_poll() will awaken */
}

static void zeke_exit_handler(void *arg)
{
  int *sigp = arg;
  if(sigp && *sigp) {
    apr_signal_unblock(*sigp);
    raise(*sigp);
  }
}

static void zeke_shutdown_handler(int sig)
{
  raise_sig = sig;
  if(sig) {
    apr_signal(sig, SIG_DFL);
    zeke_atexit(zeke_exit_handler,&raise_sig);
    zeke_safe_shutdown(0);
  }
}

static void zeke_reflect_handler(int sig)
{
  if(sig) {
    zkproc_initialize();
    if(zkproc_get_total_job_count() > 0)
      zkproc_signal_all(sig);
  }
}

#ifdef LIBZEKE_USE_LIBEVENT
static void zeke_event_sigchld(int sig, short flags, void *arg)
{
  apr_exit_why_e why;
  apr_status_t st;
  apr_proc_t proc;
  int code;

  struct sigevent_callback *info = arg;
  AN(info); AN(info->jobq); AN(info->ev);
  LOG("SIGCHLD");
  if(info->jobq->conn) {
    zkproc_initialize();  /* safety */
    for(st = APR_CHILD_DONE; st == APR_CHILD_DONE; ) {
      code = 0;
      if((st = apr_proc_wait_all_procs(&proc,&code,&why,APR_NOWAIT,info->pool)) == APR_CHILD_DONE) {

        LOG("apr_proc_wait_all_procs (libevent signal handler): %d 0x%04x code=0x%04x",proc.pid,(int)why,code);
        apr_proc_other_child_alert(&proc,APR_OC_REASON_DEATH,ZKTOOL_MKSTATUS_OC(code,why));
      }
    }
  }
  event_add(info->ev,NULL);
}

static void zeke_event_shutdown(int sig, short flags, void *arg)
{
  if(sig) {
    struct sigevent_callback *info = arg;
    fprintf(stderr,"signal %d\n",sig);
    AN(info); AN(info->jobq); AN(info->ev);
    raise_sig = sig;
    apr_signal(sig, SIG_DFL);
    zeke_atexit(zeke_exit_handler, &raise_sig);
    zeke_safe_shutdown(0);
    event_base_loopexit(event_get_base(info->ev),NULL);
  }
}

static void zeke_event_report(int sig, short flags, void *arg)
{
  static apr_time_t last = APR_TIME_C(0);
  apr_time_t now = apr_time_now();
  struct sigevent_callback *info = arg;

  if(sig == SIGINT) {
    if(last > APR_TIME_C(0)) {
      if (now - last < (APR_TIME_C(1200) * APR_TIME_C(1000)) ) {
        last = now;
        zeke_safe_shutdown(0);
        return;
      }
    }
    last = now;
  }
  do {
    int njobs,nalive,nkill,nreap;
    njobs = nalive = nkill = nreap = 0;
    zkproc_maintenance(info->pool,&njobs,&nalive,&nkill,&nreap);
    if(info->jobq->daemonize > 0) {
      ERR("MAINT: valid jobs=%u - nalive=%u - nkill=%u - nreap=%u",
          njobs,nalive,nkill,nreap);
    } else {
      zeke_eprintf("MAINT: valid jobs=%u - nalive=%u - nkill=%u - nreap=%u\n",
          njobs,nalive,nkill,nreap);
    }
#ifdef DEBUGGING
    zeke_indirect_debug_banner("INDIRECT TABLE");
#endif
  } while(0);
  event_add(info->ev,NULL);
}

static void zeke_event_reflect(int sig, short flags, void *arg)
{
  struct sigevent_callback *info = arg;
  AN(info); AN(info->jobq); AN(info->ev);

  if(sig) {
    zkproc_initialize();
    fprintf(stderr,"signal reflect: %d\n",sig);
    if(zkproc_get_total_job_count() > 0)
      zkproc_signal_all(sig);
  }
  event_add(info->ev,NULL);
}

#endif /* LIBZEKE_USE_LIBEVENT */

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

static zk_status_t zkj_make_script(const char **sp,
                                    struct zkjobq *jobq,
                                    apr_uint64_t seq,
                                    const unsigned char *b,
                                    apr_size_t blen,
                                    apr_pool_t *p)
{
  zk_status_t st;
  const char *fname;
  apr_file_t *f;

  ZK_CHECK_OBJ_NOTNULL(jobq->ws, ZK_WS_MAGIC);
  if(!b || blen == 0) {
    ERR("cannot create script for job #%" APR_UINT64_T_FMT ", no content provided",seq);
    return APR_ENOENT;
  }
  AN(exec_temp_dir);
  fname = zk_workspace_printf(jobq->ws,"%s/zkjobd.%016" APR_UINT64_T_HEX_FMT,
                              exec_temp_dir,seq);
  INFO("SCRIPT JOB %" APR_UINT64_T_FMT ": %s", seq, fname);

  st = apr_file_open(&f, fname, APR_WRITE|APR_CREATE|APR_EXCL|APR_DELONCLOSE,
                     APR_OS_DEFAULT,p);
  if(st != APR_SUCCESS) {
    zeke_apr_error(fname,st);
    return st;
  }
  st = apr_file_write_full(f,b,blen,NULL);
  if(st == APR_SUCCESS)
    st = apr_file_flush(f);
  if(st != APR_SUCCESS) {
    zeke_apr_eprintf(st,"cannot write to %s",fname);
    apr_file_close(f);
  } else if(sp)
    *sp = fname;
  if(st == APR_SUCCESS) {
    st = apr_file_perms_set(fname,
                     APR_FPROT_UREAD|APR_FPROT_UEXECUTE|
                     APR_FPROT_GREAD|APR_FPROT_GEXECUTE|
                     APR_FPROT_WREAD|APR_FPROT_WEXECUTE);
    if(st != APR_SUCCESS && st != APR_INCOMPLETE && st != APR_ENOTIMPL)
      zeke_apr_eprintf(st,"chmod %s",fname);
    else
      st = APR_SUCCESS;
  }
  return st;
}

static zkproc_t *zkj_initiate_job_pipeline(struct zkjobq *jobq,
                                           const char *subcmd,
                                           zkproc_t *parent,
                                           zkjob_t *job,
                                           apr_pool_t *p)
{
  zk_status_t st;
  zkproc_t *child = NULL;
  AN(jobq); AN(subcmd); AN(parent); AN(job);

  st = zkproc_create_pipeline(&child,parent,subcmd,ZKPROC_COPY_PARENT_ENV,0);
  assert(ZEKE_STATUS_IS_OKAY(st));
  AN(child);
  return child;
}

#ifdef LIBZEKE_USE_LIBEVENT
struct zkj_pipe {
  apr_uint32_t magic;
#define ZKJ_PIPE_MAGIC 0x2396cfa0
  apr_pool_t *p;
  struct zkjobq *jobq;
  zkproc_t *proc;
  zkjob_t *job;
  apr_file_t *f;
  struct zkws *ws;
  struct event *ev;
  apr_off_t i;
};

static void zkj_stderr_output_event(int fd, short events, void *piv)
{
  struct zkj_pipe *pinfo = (struct zkj_pipe*)piv;
  char *cp;
  apr_status_t st;
  apr_pool_t *p;
  struct zkws *ws;
  apr_size_t b,u = 0;
  ZK_CHECK_OBJ_NOTNULL(pinfo, ZKJ_PIPE_MAGIC);
  p = pinfo->p;
  AN(p);
  ws = pinfo->ws;
  ZK_CHECK_OBJ_NOTNULL(ws, ZK_WS_MAGIC);
  for(u = (ws->e - (ws->f+pinfo->i)); u < 1024; zk_workspace_release(ws,0),
                                              u = (ws->e - (ws->f+pinfo->i))) {
    u = zk_workspace_reserve(ws,0);
  }

  b = u;
  st = apr_file_read(pinfo->f, ws->f + pinfo->i, &b);
  if(((st == APR_SUCCESS) || (st == APR_EOF)) && b > 0) {
    char *s = ws->f + pinfo->i;
    *(s+b) = '\0';
    for(cp = s; *cp; cp++) {
      assert(cp < (s+b));
      if(*cp == '\r' || *cp == '\n') {
        if(*cp == '\r') {
          *cp++ = '\0';
          if(*cp == '\n')
            *cp++ = '\0';
        } else if(*cp == '\n') {
          *cp++ = '\0';
          if(*cp == '\r')
            *cp++ = '\0';
        }
        INFO("STDERR: %s",ws->f);
        memmove(ws->f,cp,((s+b) - cp)+1);
        cp = ws->f + ((s+b) - cp);
        s = ws->f;
      }
    }
    if(cp >= (s+b))
      apr_file_flush(pinfo->f);
    pinfo->i = s - ws->f;
  } else if(st == APR_EOF) {
    apr_file_close(pinfo->f);
    *(ws->f + pinfo->i++) = '\0';
    INFO("STDERR: %s",ws->f);
    return;
  } else if(st != APR_SUCCESS) {
    zeke_apr_error("job #%" APR_UINT64_T_FMT, pinfo->job->seq);
    return;
  } else if(b == 0)
    apr_file_flush(pinfo->f);

  event_add(pinfo->ev,NULL);
}
static void zkj_start_stderr_pipe(struct zkjobq *jobq, zkproc_t *c,
                                  zkjob_t *job, apr_pool_t *p)
{
  zk_status_t st;
  struct event *ev;
  zkproc_pipeset_t *pset;
  apr_file_t *pipe;
  struct zkj_pipe *pinfo;

  AN(c); AN(job);
  assert(job->seq = zkproc_seq_get(c));
  st = zkproc_pipeset_create(&pset,ZKPROC_PIPESET_STDERR,
                             zkproc_pool_get(c));
  AZOK(st); AN(pset);
  AZOK(zkproc_pipeset_assign(pset,zkproc_pipeset_write,&pipe,0,c));
/*                           ZKPROC_PIPESET_OPT_NO_STDOUT,c)); */
  AN(pipe);
  AAOK(apr_file_inherit_unset(pipe));
  st = zeke_file_event_create_ex(&ev,pipe,zkj_stderr_output_event,
                                  0,APR_TIME_C(-1),jobq->pool);
  if(!ZEKE_STATUS_IS_OKAY(st)) {
    zeke_apr_eprintf(st,"seq #%" APR_UINT64_T_FMT, job->seq);
    zeke_safe_shutdown(ZKE_TO_ERROR(st));
  }

  AN(ev);
  pinfo = apr_palloc(apr_file_pool_get(pipe),sizeof(*pinfo));
  AN(pinfo);
  pinfo->magic = ZKJ_PIPE_MAGIC;
  pinfo->p = apr_file_pool_get(pipe);
  AN(pinfo->p);
  pinfo->f = pipe;
  pinfo->proc = c;
  pinfo->job = job;
  pinfo->jobq = jobq;
  pinfo->ev = ev;
  pinfo->ws = zk_workspace_new(pinfo->p);
  ZK_CHECK_OBJ_NOTNULL(pinfo->ws,ZK_WS_MAGIC);
  assert(pinfo->ws->p != pinfo->p);
  AN(pinfo->ws->p);
  AN(pinfo->p);
  /* apr_pool_join(pinfo->ws->p,pinfo->p); */
  pinfo->i = 0;
  event_del(ev);

  if(event_assign(ev,event_get_base(ev),event_get_fd(ev),
                  EV_READ,event_get_callback(ev),
                  pinfo) == -11)
    zeke_fatal("event_assign",apr_get_os_error());
  if(event_add(ev,NULL) == -1)
    zeke_fatal("event_assign",apr_get_os_error());
}
#endif

static zk_status_t zkj_initiate_job(zkproc_t **pp,
                                    struct zkjobq *jobq,
                                    zkjob_t *job,
                                    apr_pool_t *p)
{
  zk_status_t st = APR_ENOENT;
  zkproc_t *child = NULL, *pipeline = NULL;
  apr_uint32_t flags = 0;
  const char *cmd = NULL;
  const char *c = NULL;
  apr_array_header_t *args = NULL;

  if(job->headers) {
    const char *flagstr = apr_table_get(job->headers,"flags");
    if(flagstr && !ZEKE_STATUS_IS_OKAY(zkjobq_flags_parse(flagstr,&flags)))
      flags = 0;
  }
  zkproc_maintenance(p,&jobq->njobs,NULL,NULL,NULL);
  if(flags & ZKPROCF_TEMP_FILE) {
    zk_status_t r;
    const char *s = NULL;

    r = zkj_make_script(&s,jobq,job->seq,job->content,job->len-1,job->pool);
    if(!ZEKE_STATUS_IS_OKAY(r))
      return r;
    AN(s);
    flags &= ~ZKPROCF_STDIN_PIPE;
    if(job->headers)
      apr_table_unset(job->headers,"command");
    if (flags & ZKPROCF_USE_SHELL) {
      cmd = (job->headers ? apr_table_get(job->headers,"shell") : NULL);
      if(!cmd)
        cmd = "/bin/sh";
    } else if(job->headers)
      cmd = apr_table_get(job->headers,"interp");
    if(!cmd) cmd = s;
  } else if((flags & ZKPROCF_USE_SHELL) != 0 && job->headers) {
    cmd = apr_table_get(job->headers,"shell");
    if(!cmd)
      cmd = apr_table_get(job->headers,"interp");
    if(cmd && !apr_table_get(job->headers,"arguments")) {
      ERR("CMD=%s",cmd);
      if(zkproc_is_valid_shell(cmd)) {
        const char *s = apr_table_get(job->headers,"command");
        ERR("CMD=%s",s);
        if(s && (!job->content || !job->len == 0 || (apr_isspace(*job->content) && job->len <= 1))) {
          s = apr_psprintf(job->pool,"%s\n",s);
          job->content = (unsigned char*)s;
          job->len = strlen(s);
          apr_table_unset(job->headers,"command");
          flags &= ~ZKPROCF_USE_SHELL;
          flags |= ZKPROCF_STDIN_PIPE;
        }
      } else  {
        ERR("WTF, '%s' is not a valid shell?",cmd);
        cmd = NULL;
      }
    }
  }

  if(!job->headers || apr_table_elts(job->headers)->nelts == 0) {
    ERR("job #%" APR_UINT64_T_FMT " has no headers!",job->seq);
    zkqueue_job_delete(job);
    return APR_EINVAL;
  } else if (cmd) {
    if(!args) {
      apr_status_t ast;
      char **argv = NULL;
      const char *s;
      args = apr_array_make(job->pool,2,sizeof(const char*));
      AN(args);
      APR_ARRAY_PUSH(args,const char*) = cmd;
      if(job->headers) {
        s = apr_table_get(job->headers,"arguments");
        if(s && (ast = apr_tokenize_to_argv(s,&argv,p)) != APR_SUCCESS) {
          zeke_apr_eprintf(ast,"cannot tokenize arguments: %s",s);
          argv = NULL;
        }
      }
      for(; argv && *argv; argv++)
        APR_ARRAY_PUSH(args,const char*) = apr_pstrdup(job->pool,*argv);
    }
  }

  c = (cmd ? cmd : apr_table_get(job->headers,"command"));
  if (c) {
    const char *user = apr_table_get(job->headers,"user");
    if(!(flags & ZKPROCF_TEMP_FILE)) {
      flags |= ZKPROCF_STDIN_PIPE;
      if(!(flags & ZKPROCF_USE_SHELL) && strpbrk(c,"~><()$*\\\"'&|;[]") != NULL) {
        flags |= ZKPROCF_USE_SHELL;
      }
    }
    if (user && strchr(c,'~')) {
      apr_off_t snap;
      apr_size_t u,len,namelen,sz;
      int repl = 0;
      char *tmp,*cp,*state;
      ZK_CHECK_OBJ_NOTNULL(job->ws, ZK_WS_MAGIC);
      snap = zk_workspace_snapshot(job->ws);
      tmp = zk_workspace_strdup(job->ws,c);
      AN(tmp);
      namelen = strlen(user);
      u = zk_workspace_reserve(job->ws,0);
      for(cp = apr_strtok(tmp,"~",&state); cp; cp = apr_strtok(NULL,"~",&state), repl++) {
        if(repl && (!*cp || *cp == '/' || *cp == '\\')) {
          if(u <= namelen+2) {
            sz = job->ws->r - job->ws->b;
            zk_workspace_release(job->ws, 0);
            u = zk_workspace_reserve(job->ws, sz*2);
            assert(u >= sz*2);
            job->ws->r = job->ws->b + sz;
          }
          *job->ws->r++ = '~';
          job->ws->r = apr_cpystrn(job->ws->r,user,namelen+1);
        }
        len = strlen(cp);
        if(u <= len+1) {
          sz = job->ws->r - job->ws->b;
          zk_workspace_release(job->ws, 0);
          u = zk_workspace_reserve(job->ws, sz*2);
          assert(u >= sz*2);
          job->ws->r = job->ws->b + sz;
        }
        job->ws->r = apr_cpystrn(job->ws->r,cp,len+1);
      }
      if(*job->ws->r)
        *job->ws->r++ = '\0';
      else
        job->ws->r++;
      apr_table_set(job->headers,"command",zk_workspace_releasep(job->ws,job->ws->r));
      zk_workspace_restore(job->ws,snap);
    }
    st = zkproc_create_ex(&child,job,cmd,args,NULL,flags,NULL);
    if(ZEKE_STATUS_IS_OKAY(st)) {
      apr_interval_time_t max_run = jobq->max_run;
      apr_file_t *pipe = NULL;
      AN(child);
      if(pp)
        *pp = child;
      if(job->headers) {
        apr_int64_t t;
        char *end = NULL;
        const char *job_timeout = apr_table_get(job->headers,"timeout");
        if(job_timeout) {
          t = apr_strtoi64(job_timeout,&end,10);
          if(t > APR_TIME_C(-1) && end && !*end)
            if(jobq->max_run <= APR_TIME_C(0) || (apr_interval_time_t)t < jobq->max_run)
              max_run = (apr_interval_time_t)t;
        }
#if 0
        if(apr_table_get(job->headers,"wrapper"))
          pipeline = zkj_initiate_job_pipeline(jobq, apr_table_get(job->headers,"wrapper"),
                                    child,job,p);
#endif
      }
#ifdef LIBZEKE_USE_LIBEVENT
#if 1
      if(pipeline || child)
        zkj_start_stderr_pipe(jobq,(pipeline ? pipeline : child),
                              job,p);
#endif
#endif
      if(pipeline != NULL) {
        st = zkproc_start_pipeline(pipeline,child);
        if(!ZEKE_STATUS_IS_OKAY(st))
          zeke_apr_eprintf(st,"cannot start pipelined child proc for job #%" APR_UINT64_T_FMT,
                           job->seq);
      }
      st = zkproc_start(child,&pipe,max_run);
      if(ZEKE_STATUS_IS_OKAY(st)) {
        if(pipe) {
          apr_status_t ast = APR_SUCCESS;
          if(job->len > 1 || *job->content)
            ast = apr_file_write_full(pipe,job->content,job->len-1,NULL);
          if(ast != APR_SUCCESS)
            zeke_apr_eprintf(ast,"writing to job #%" APR_UINT64_T_FMT, job->seq);
          apr_file_close(pipe);
        }
        if(jobq->njobs+1 >= jobq->max_concurrent_jobs && zkelection_member_count(jobq->e) > 1) {
          INFO("maximum job count reached (%d), relinquishing queue.",jobq->max_concurrent_jobs);
          st = zkqueue_clear(jobq->q);
          if(!ZEKE_STATUS_IS_OKAY(st))
            zeke_apr_error("queue",st);
          if(jobq->flags & ZKJ_F_WINNER) {
            st = zkelection_relinquish_leader(jobq->e);
            if(ZEKE_STATUS_IS_OKAY(st))
              jobq->flags &= ~ZKJ_F_WINNER;
            else
              zeke_apr_error("unable to reqlinquish election",st);
          }
          st = ZEOK;
        }
#if 0
        zkproc_close_pipeline(child,NULL);
#endif
      } else if(child) {
        AZOK(zkproc_destroy(child));
      }

    } else zkqueue_job_unlock(job);
  } else if(job->len == 0) {
    ERR("job #%" APR_UINT64_T_FMT " has no content!",job->seq);
    st = APR_ENOENT;
  } else {
    ERR("job #%" APR_UINT64_T_FMT " cannot determine command",job->seq);
    st = APR_ENOENT;
  }
  return st;
}

static zeke_cb_t zkj_jobq_handler(zkqueue_t *q, enum zkq_event ev, zkjob_t *job,
                        zeke_context_t *ctx)
{
  zk_status_t st;
  struct zkjobq *jobq = (struct zkjobq*)ctx;
  apr_pool_t *p;
  zkproc_t *proc;
  zeke_cb_t r = ZEKE_CALLBACK_IGNORE;
  int pending = 0;

  AN(jobq);
  AN(zkjob_pool);
  AAOK(apr_pool_create(&p,zkjob_pool));

  switch(ev) {
  case zkq_ev_indexed:
    LOG("queue at %s was indexed",jobq->qnode);
    break;
  case zkq_ev_job_new:
    AN(job);
    INFO("NEW: job sequenece #%" APR_UINT64_T_FMT ", njobs=%u" ,job->seq, (unsigned)jobq->njobs);
    if(!zkqueue_job_is_loaded(job)) {
      if(jobq->njobs < jobq->max_concurrent_jobs || zkelection_member_count(jobq->e) > 1) {
        LOG("Initiating load on job #%" APR_UINT64_T_FMT, job->seq);
        st = zkqueue_job_load(job);
        if(!ZEKE_STATUS_IS_OKAY(st)) {
          zeke_apr_eprintf(st,"cannot load job seq #%" APR_UINT64_T_FMT,job->seq);
          apr_pool_destroy(p);
          return ZEKE_CALLBACK_DESTROY;
        }
      } else {
        assert(!zkqueue_job_is_pending(job));
        st = zkqueue_job_pending_push(jobq->q,job);
        if(!ZEKE_STATUS_IS_OKAY(st)) {
          zeke_apr_eprintf(st,"unable to set job seq #%" APR_UINT64_T_FMT " pending",job->seq);
          return ZEKE_CALLBACK_DESTROY;
        }
      }
    }
    break;
  case zkq_ev_job_loaded:
    r = ZEKE_CALLBACK_DESTROY;
    AN(job);
    INFO("LOADED: job sequence #%" APR_UINT64_T_FMT ", len=%lu",job->seq,job->len);
    proc = NULL;
    st = zkj_initiate_job(&proc,jobq,job,p);
    if(proc && (zkproc_flags_get(proc) & ZKPROCF_RUNNING))
       zkproc_register(proc);
    if(!ZEKE_STATUS_IS_OKAY(st)) {
      zeke_apr_eprintf(st,"job #%" APR_UINT64_T_FMT,job->seq);
      goto zkj_jobq_handler_exit;
    } else {
      st = zkqueue_job_delete(job);
      if(!ZEKE_STATUS_IS_OKAY(st))
        zeke_apr_eprintf(st,"cannot delete %s",zkqueue_job_node_get(job));
    }
    if(jobq->njobs+1 < jobq->max_concurrent_jobs &&
       zkelection_member_count(jobq->e) == 1 && (jobq->flags & ZKJ_F_WINNER)) {
      st = zkqueue_job_pending_pop(jobq->q,&job);
      if(st == APR_EINCOMPLETE)
        st = APR_SUCCESS;
      else if(st == APR_SUCCESS) {
        zeke_eprintf("LOADING deferred job #%" APR_UINT64_T_FMT "\n",job->seq);
        st = zkqueue_job_load(job);
        if(ZEKE_STATUS_IS_OKAY(st)) {
          pending++;
        } else
          zeke_apr_eprintf(st,"cannot load job seq (in orphan mode) #%" APR_UINT64_T_FMT,job->seq);
        zkqueue_job_unlock(job);
      } else
        zeke_apr_error("zkqueue_job_pending_pop",st);
    }
    break;
  default:
    ERR("unsupported zkqueue event %d",(int)ev);
    zeke_safe_shutdown(1);
    break;
  }

zkj_jobq_handler_exit:
  apr_pool_destroy(p);
  if(!pending) {
    if(!(jobq->flags & ZKJ_F_WINNER) && jobq->e) {
      apr_uint32_t state = zkelection_state(jobq->e);
      if(!(state & ZKELEC_STATE_REGISTERED) || (state & ZKELEC_STATE_RELINQUISH)) {
        zkelection_restart(jobq->e);
      }
    }
  }
  return r;
}

static void zkj_start_queue(struct zkjobq *jobq)
{
  zk_status_t st;
  if(jobq->q == NULL) {
    AN(jobq->qnode);
    st = zkqueue_create(&jobq->q,jobq->qnode,zkj_jobq_handler,jobq->conn,jobq->pool);
    if(!ZEKE_STATUS_IS_OKAY(st)) {
      zeke_apr_eprintf(st,"cannot init queue at %s",jobq->qnode);
      return;
    }
    zeke_pool_cleanup_indirect(zkqueue_pool_get(jobq->q),jobq->q);
    zkqueue_context_set(jobq->q,jobq);
    st = zkqueue_start(jobq->q,NULL,NULL);
    if(!ZEKE_STATUS_IS_OKAY(st)) {
      zeke_apr_eprintf(st,"cannot start queue at %s",jobq->qnode);
      return;
    }
  } else {
    assert(zkqueue_context_get(jobq->q) == (zeke_context_t*)jobq);
    st = zkqueue_start(jobq->q,zkj_jobq_handler,NULL);
    if(!ZEKE_STATUS_IS_OKAY(st)) {
      zeke_apr_eprintf(st,"cannot restart queue at %s",jobq->qnode);
      return;
    }
  }
}

static zeke_cb_t zkj_election_update(zkelection_t *e, enum zkelec what, void *data)
{
  struct zkjobq *jobq = (struct zkjobq*)data;

  ZK_CHECK_OBJ_NOTNULL(jobq->ws, ZK_WS_MAGIC);
  switch(what) {
    case ZKELEC_WIN:
      if((jobq->flags & ZKJ_F_WINNER) == 0) {
        LOG("won the election, state %d/%d!",zkelection_state(jobq->e),
            zkelection_state(e));
        jobq->flags |= ZKJ_F_WINNER;
        zkj_start_queue(jobq);
      }
      break;
    case ZKELEC_CHANGE:
      if(jobq->flags & ZKJ_F_WINNER) {
        jobq->flags &= ~ZKJ_F_WINNER;
        INFO("%s: I lost the election, winner is %s",jobq->ident,zkelection_winner(e));
      }
      break;
    case ZKELEC_INVAL:
      ERR("%s: election became invalid",jobq->ident);
      zeke_safe_shutdown(1);
    default:
      break;
  }
  return ZEKE_CALLBACK_DESTROY;
}

static void zkj_start_election(struct zkjobq *jobq)
{
  zk_status_t st;
  ZK_CHECK_OBJ_NOTNULL(jobq->ws, ZK_WS_MAGIC);
  zk_workspace_clear(&jobq->ws);
  if(jobq->e == NULL) {
    LOG("%s: Initiating election at %s...",jobq->ident,jobq->enode);
    st = zkelection_create(&jobq->e,jobq->enode,jobq->ident,
                           zkj_election_update,
                           jobq->conn,jobq->pool);
    if(!ZEKE_STATUS_IS_OKAY(st)) {
      zeke_apr_eprintf(st,"Cannot create election at %s",jobq->enode);
      zeke_safe_shutdown(ZKE_TO_ERROR(st));
      return;
    } else {
      const char *n;
      if(ZEKE_STATUS_IS_OKAY(zkelection_name_get(jobq->e,&n,NULL)) && n) {
        jobq->ident = apr_pstrdup(jobq->pool,n);
        zeke_shmlog_ident_set(n);
      }
    }
    zeke_pool_cleanup_indirect(zkelection_pool_get(jobq->e),jobq->e);
    zkelection_context_set(jobq->e,jobq);
    st = zkelection_start(jobq->e,(jobq->etimeout > APR_TIME_C(0) ? &jobq->etimeout : NULL));
    if(!ZEKE_STATUS_IS_OKAY(st)) {
      zeke_apr_eprintf(st,"Cannot start election at %s",jobq->enode);
      zeke_safe_shutdown(ZKE_TO_ERROR(st));
    }
  /* zeke_safe_shutdown(0); */
  }
}

static zeke_cb_t zkj_check_election_exists(const zeke_callback_data_t *cbd)
{
  zk_status_t st = zeke_callback_status_get(cbd);
  const struct zkjobq_ws *arg = (struct zkjobq_ws*)cbd->ctx;

  ZK_CHECK_OBJ_NOTNULL(arg,ZKJOBQ_WS_MAGIC);
  if(ZEKE_STATUS_IS_OKAY(st)) {
    struct zkjobq *jobq = arg->jobq;
    AN(arg->a[0]);
    jobq->enode = apr_pstrdup(jobq->pool,arg->a[0]);
    LOG("%s: Got election node: %s",jobq->ident,jobq->enode);
    if(jobq->enode != NULL && jobq->qnode != NULL)
      zkj_start_election(jobq);
  } else if(!arg->a[1] && APR_STATUS_IS_ZK_ERROR(st) && ZEKE_TO_ZK_ERROR(st) == ZNONODE) {
    struct zkjobq *jobq = arg->jobq;
    const char *node = arg->a[0];
    AN(node);
    ((struct zkjobq_ws*)arg)->a[1] = (void*)1;
    st = zeke_acreate(NULL,jobq->conn,node,NULL,0,NULL,0,
                      zkj_check_election_exists,arg);
    if(!ZEKE_STATUS_IS_OKAY(st))
      zeke_apr_eprintf(st,"cannot create %s",node);
  } else {
    zeke_apr_error(arg->a[0],st);
  }
  DESTROY;
}

static zeke_cb_t zkj_check_queue_exists(const zeke_callback_data_t *cbd)
{
  zk_status_t st = zeke_callback_status_get(cbd);
  const struct zkjobq_ws *arg = (struct zkjobq_ws*)cbd->ctx;

  ZK_CHECK_OBJ_NOTNULL(arg, ZKJOBQ_WS_MAGIC);
  if(ZEKE_STATUS_IS_OKAY(st)) {
    struct zkjobq *jobq = arg->jobq;
    AN(arg->a[0]);
    jobq->qnode = apr_pstrdup(jobq->pool,arg->a[0]);
    LOG("%s: Got queue node: %s",jobq->ident,jobq->qnode);
    if(jobq->enode != NULL && jobq->qnode != NULL)
      zkj_start_election(jobq);
  } else if(!arg->a[1] && APR_STATUS_IS_ZK_ERROR(st) && ZEKE_TO_ZK_ERROR(st) == ZNONODE) {
    struct zkjobq *jobq = arg->jobq;
    const char *node = arg->a[0];
    AN(node);
    ((struct zkjobq_ws*)arg)->a[1] = (void*)1;
    st = zeke_acreate(NULL,jobq->conn,node,NULL,0,NULL,0,
                      zkj_check_queue_exists,arg);
    if(!ZEKE_STATUS_IS_OKAY(st))
      zeke_apr_eprintf(st,"cannot create %s",node);
  } else {
    zeke_apr_error(arg->a[0],st);
  }
  DESTROY;
}

static zeke_cb_t zkj_check_subnodes(const zeke_callback_data_t *cbd, const char *node)
{
  zk_status_t st;
  struct zkjobq *jobq = ZKJOBQ(cbd);
  const struct zkjobq_ws *arg;
  char *enode, *qnode;

  AN(jobq);
  ZK_CHECK_OBJ_NOTNULL(jobq->ws, ZK_WS_MAGIC);
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
}

static zeke_cb_t zkj_create(const zeke_callback_data_t *cbd)
{
  zk_status_t st = zeke_callback_status_get(cbd);
  struct zkjobq *jobq = ZKJOBQ(cbd);
  const char *node;
  AN(jobq);
  ZK_CHECK_OBJ_NOTNULL(jobq->ws, ZK_WS_MAGIC);
  node = jobq->ws->data;
  jobq->ws->data = NULL;
  if(ZEKE_STATUS_IS_OKAY(st))
    return zkj_check_subnodes(cbd,node);
  else
    zeke_apr_eprintf(st,"creating %s",node);
  zeke_safe_shutdown(ZKE_TO_ERROR(st));
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
  if(ZEKE_STATUS_IS_OKAY(st))
    return zkj_check_subnodes(cbd,node);
  else if(APR_STATUS_IS_ZK_ERROR(st) && ZEKE_TO_ZK_ERROR(st) == ZNONODE) {
    jobq->ws->data = (char*)node;
    st = zeke_acreate(NULL,jobq->conn,node,NULL,0,ZEKE_DEFAULT_ACL,0,
                      zkj_create,jobq);
    if(ZEKE_STATUS_IS_OKAY(st))
      DESTROY;

    zeke_apr_eprintf(st,"creating %s",node);
  } else {
    zeke_apr_error(node,st);
  }
  zeke_safe_shutdown(ZKE_TO_ERROR(st));
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

static void zkj_interval_timer(zeke_connection_t *conn, void *data)
{
  struct zkjobq *jobq = cast_context_to_zkjobq_t(data);
  const char *node;
  static unsigned short count = 0;

  apr_pool_t *p;
  int nkill = 0, nreap = 0, nalive = 0;
  AN(jobq);
  if(jobq->session_key && jobq->conn) {
    data = zeke_session_context(jobq->conn,jobq->session_key);
    if(data)
      jobq = cast_context_to_zkjobq_t(data);
    else
      return;
  }
  if(!jobq->conn && !conn)
    return;
  if(!conn)
    conn = jobq->conn;
  ZK_CHECK_OBJ_NOTNULL(jobq->ws,ZK_WS_MAGIC);
  node = jobq->node;
  if(!node)
    node = (char*)jobq->ws->data;

  AAOK(apr_pool_create(&p,jobq->pool));
  zkproc_maintenance(p,&jobq->njobs,&nalive,&nkill,&nreap);
  apr_pool_destroy(p);
  if(jobq->njobs || nalive || nkill || nreap)
    INFO("TICK: total jobs=%d, procs=%d, procs killed=%d, procs reaped=%d",jobq->njobs,
          nalive,nkill,nreap);

  if(jobq->njobs < jobq->max_concurrent_jobs && jobq->e != NULL) {
    apr_uint32_t es = zkelection_state(jobq->e);
    if(!(es & ZKELEC_STATE_REGISTERED) &&
       !(es & ZKELEC_STATE_STARTING) && (es != ZKELEC_STATE_INIT)) {
      zk_status_t st = zkelection_start(jobq->e,(jobq->etimeout > APR_TIME_C(0) ?
                                                        &jobq->etimeout : NULL));
      if(!ZEKE_STATUS_IS_OKAY(st)) {
        zeke_apr_eprintf(st,"Cannot start election at %s",jobq->enode);
        zeke_safe_shutdown(ZKE_TO_ERROR(st));
      }
    } else if(jobq->njobs < jobq->max_concurrent_jobs &&
       zkelection_member_count(jobq->e) == 1 && (jobq->flags & ZKJ_F_WINNER)) {
      zkjob_t *job;
      zk_status_t st = zkqueue_job_pending_pop(jobq->q,&job);
      if(st == APR_EINCOMPLETE)
        st = APR_SUCCESS;
      else if(st == APR_SUCCESS) {
        zeke_eprintf("LOADING deferred job #%" APR_UINT64_T_FMT " (maint timer)\n",job->seq);
        st = zkqueue_job_load(job);
        if(!ZEKE_STATUS_IS_OKAY(st))
          zeke_apr_eprintf(st,"cannot load job seq (in orphan mode) #%" APR_UINT64_T_FMT,job->seq);
        zkqueue_job_unlock(job);
      } else
        zeke_apr_error("zkqueue_job_load",st);
    }
  }
  if(++count % 5 == 0)
    count = 0;

  if(count == 0 && jobq->e && zkelection_member_count(jobq->e) == 1)
    zkelection_refresh(jobq->e);
}

#ifdef LIBZEKE_USE_LIBEVENT
static void zkj_signal_event_set(int sig, event_callback_fn cb,
                                 const zkjobq_t *jobq, apr_pool_t *p)
{
  zk_status_t st;
  struct sigevent_callback *info;
  apr_pool_t *pool;

  AAOK(apr_pool_create(&pool,p));
  zeke_pool_tag(pool,"signal pool");
  AZOK(zeke_event_base_set(zeke_event_base_get(p),0,pool));
  info = apr_palloc(pool,sizeof(*info));
  info->pool = pool;
  info->jobq = jobq;
  st = zeke_event_create(&info->ev,info->pool);
  assert(ZEKE_STATUS_IS_OKAY(st));
  AN(info->ev);
  evsignal_assign(info->ev,event_get_base(info->ev),sig,cb,info);
  event_add(info->ev,NULL);
}
#endif

static void zkj_daemonize(zkjobq_t *jobq)
{
  apr_status_t st;
  int daemonized = 0;
  if(jobq->daemonize >= 0) {
    st = apr_proc_detach(jobq->daemonize > 0 ? 1 : 0);
    if(st != APR_SUCCESS) {
      zeke_fatal("cannot run in backgrond",st);
      jobq->daemonize = -10;
    } else if(jobq->daemonize != 0) daemonized++;
  };
  if(!jobq->no_shmlog) {
    zeke_shmlog_ident_set(jobq->ident ? jobq->ident : "zkjobd");
    st = zktool_enable_shmlog(jobq->pool);
    if(st != APR_SUCCESS)
      zeke_fatal("enabling shared memory log",st);
  }
  if(geteuid() == 0 || daemonized) {
    if(chdir("/") == -1)
      zeke_fatal("cannot 'chdir /'",apr_get_os_error());
  }

#ifndef LIBZEKE_USE_LIBEVENT
  apr_signal(SIGCHLD, zeke_sigchld_handler);
  apr_signal(SIGTERM, zeke_shutdown_handler);
  apr_signal(SIGHUP, zeke_reflect_handler);
  apr_signal(SIGINT, zeke_shutdown_handler);
  apr_signal(SIGQUIT, zeke_reflect_handler);
  apr_signal(SIGUSR1, zeke_reflect_handler);
  apr_signal(SIGUSR2, zeke_reflect_handler);
#endif
  apr_signal(SIGPIPE, SIG_IGN);
  apr_signal(SIGALRM, SIG_DFL);

#ifdef LIBZEKE_USE_LIBEVENT
  zkj_signal_event_set(SIGCHLD, zeke_event_sigchld, jobq, jobq->pool);
  zkj_signal_event_set(SIGTERM, zeke_event_shutdown, jobq, jobq->pool);
  zkj_signal_event_set(SIGHUP, zeke_event_reflect, jobq, jobq->pool);
#if 0
  zkj_signal_event_set(SIGINT, zeke_event_shutdown, jobq, jobq->pool);
#else
  zkj_signal_event_set(SIGINT, zeke_event_report, jobq, jobq->pool);
#endif
  zkj_signal_event_set(SIGQUIT, zeke_event_reflect, jobq, jobq->pool);
  zkj_signal_event_set(SIGUSR1, zeke_event_report, jobq, jobq->pool);
  zkj_signal_event_set(SIGUSR2, zeke_event_reflect, jobq, jobq->pool);
  return;
#endif
  apr_signal_unblock(SIGCHLD);
  apr_signal_unblock(SIGINT);
  apr_signal_unblock(SIGTERM);
  apr_signal_unblock(SIGHUP);
  apr_signal_unblock(SIGUSR1);
  apr_signal_unblock(SIGUSR2);
  apr_signal_unblock(SIGPIPE);
  apr_signal_unblock(SIGQUIT);
  apr_signal_unblock(SIGALRM);
}

#ifdef LIBZEKE_USE_LIBEVENT
struct interval_wrapper {
  struct event *ev;
  zeke_runloop_callback_fn fp;
  struct zkjobq *jobq;
  struct timeval *tv;
};


static void interval_event(int ignored, short event, void *data)
{
  struct interval_wrapper *i = (struct interval_wrapper*)data;

  AN(i); AN(i->ev); AN(i->fp); AN(i->jobq);
  i->fp(i->jobq->conn,i->jobq);
  if(!zeke_is_safe_shutdown(NULL))
    event_add(i->ev,i->tv);
}
#endif

static int start_zk(const char *zkhosts, struct zkjobq *jobq, const char *node)
{
  apr_pool_t *pool = jobq->pool;
  zk_status_t st = ZEOK;
  apr_interval_time_t interval = apr_time_from_sec(1);
  zeke_runloop_callback_fn setup_func = NULL;
  zeke_runloop_callback_fn interval_func = NULL;
  char *temp = NULL;
  int rc = 0;
#if defined(EVTHREAD_USE_PTHREADS_IMPLEMENTED) && defined(ZEKE_USE_THREADS)
  evthread_use_pthreads();
#endif

  zeke_pool_tag(pool,"runtime pool");
  ZK_CHECK_OBJ_NOTNULL(jobq->ws, ZK_WS_MAGIC);

  setup_func = zkj_setup;
  interval_func = zkj_interval_timer;
  if(apr_env_get(&temp,"ZKJOBD_TMPDIR",zkjob_pool) == APR_SUCCESS && temp) {
    apr_dir_t *d = NULL;
    st = APR_ENOENT;
    if(*temp != '/' || (st = apr_dir_open(&d,temp,pool)) != APR_SUCCESS)
      zeke_apr_eprintf(st,"Ignoring ZKJOBD_TMPDIR, %s is an invalid directory.",temp);
    if(d != NULL && st == APR_SUCCESS) {
      apr_dir_close(d);
      exec_temp_dir = temp;
      zeke_pool_cleanup_indirect(pool,exec_temp_dir);
    }
  }

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

  jobq->ws->data = (void*)node;

  zkj_daemonize(jobq);
#ifdef LIBZEKE_USE_LIBEVENT
  do {
    struct timeval interval_tv;
    struct interval_wrapper wrapper;
    struct event_base *base;
    zeke_io_event_t loop;
    int i = -1;

  (void)zeke_event_base_get(pool);
    loop = zeke_io_event_loop_make(pool);
    AN(loop);


    wrapper.fp = interval_func;
    wrapper.jobq = jobq;
    wrapper.tv = &interval_tv;

    interval_tv.tv_sec = apr_time_sec(interval);
    interval_tv.tv_usec = apr_time_usec(interval);
    assert(zeke_event_create(&wrapper.ev,pool) == ZEOK);
    AN(wrapper.ev);
    evtimer_assign(wrapper.ev, zeke_io_event_loop_base_get(loop),
                                        interval_event, &wrapper);
    event_add(wrapper.ev,&interval_tv);

    assert(zeke_io_event_loop_connect(loop,zkhosts,setup_func,0,jobq) == APR_SUCCESS);
    assert(zeke_io_event_loop_base_get(loop) != NULL);
    assert(zeke_io_event_loop_base_get(loop) == zeke_event_base_get(pool));

    for(base = zeke_io_event_loop_base_get(loop);
            base != NULL && !zeke_io_event_loop_gotexit(loop,&rc);)
    {
      if(i != -1)
        assert(zeke_io_event_loop_restart(loop) == APR_SUCCESS);
      i = event_base_loop(base,0);
      zeke_eprintf("\n\n  LOOP  \n\n");
      switch(i) {
      case -1:
        st = apr_get_os_error();
        zeke_fatal("event_base_loop",st);
        exit(1);
        break;
      case 0:
      case 1:
        if(zeke_io_event_loop_gotbreak(loop)) {
          assert(zeke_io_event_loop_status_get(loop,&st) == APR_SUCCESS);
          if(!ZEKE_STATUS_IS_OKAY(st))
            zeke_fatal("io event loop error",st);
        }
        break;
      default:
        zeke_eprintf("hmm... got %d",i);
        base = NULL;
        rc = i;
        break;
      }
    }
  } while(0);
#else /* !LIBZEKE_USE_LIBEVENT */
  st = zeke_app_run_loop_interval(zkhosts,interval,
                                  interval_func,setup_func,
                                  ZEKE_RUNLOOP_DELAY_STARTUP_CALLBACK,
                                  jobq);
#endif

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

static inline const char *validate_node(const char *path, const char *msg)
{
  if(path) {
    apr_size_t sz;
    while(*path == '/') path++;
    sz = strlen(path);
    if(sz > 0 && *(path+sz-1) == '/') {
      if(msg != NULL)
        zeke_eprintf(msg,path);
      path = NULL;
    }
  }

  return path;
}

static
void usage(int exitcode)
{
  if(!exitcode || exitcode >= 100) {
    zeke_printf("%s\n","zkjobd [options]");
    zeke_printf("%s\n","  Processes jobs queued by zkjobq, typically run on a distributed farm");
    zeke_printf("%s\n%s\n","  of hosts.","options:");
  }

  zktool_display_options(options,(!exitcode || exitcode >= 100 ? zstdout : zstderr),NULL);

  exit(exitcode);
}

ZEKE_HIDDEN
int main(int argc, const char *const *argv,
                               const char *const *env)
{
  zk_status_t st = zeke_init_cli(&argc, &argv, &env);
  apr_pool_t *pool = NULL;
  apr_pool_t *getopt_pool = NULL;
  apr_getopt_t *getopt = NULL;
  int i,opt;
  struct zkjobq jobq;
  const char *ns = NULL;
  const char *node = NULL;
  const char *zkhosts = NULL;
  const char *opt_arg;

  AZOK(st);
  AN(zkjob_pool = zeke_root_subpool_create());
  AAOK(apr_pool_create(&pool,zkjob_pool));
  AN(pool);
  AAOK(apr_pool_create(&getopt_pool,pool));
  AAOK(apr_getopt_init(&getopt,getopt_pool,argc,argv));

  zkproc_init_default_environment(env);
  jobq.node = jobq.enode = jobq.qnode = NULL;
  jobq.ident = NULL;
  jobq.session_key = NULL;
  jobq.conn = NULL;
  jobq.e = NULL; jobq.q = NULL;
  jobq.etimeout = apr_time_from_sec(5);
  jobq.max_run = APR_TIME_C(-1);
  jobq.flags = 0;
  jobq.njobs = 0;
  jobq.max_concurrent_jobs = 1;
  jobq.daemonize = 0;
  jobq.debug = 0;
  jobq.no_shmlog = 0;

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
      case OPT_NS:
        ns = validate_node(apr_pstrdup(pool,opt_arg),"%s is not a valid namespace.\n");
        break;
      case OPT_FOREGROUND:
        jobq.daemonize = -1;
        break;
      case OPT_DAEMONIZE:
        jobq.daemonize = 1;
        break;
      case OPT_DISABLE_SHMLOG:
        jobq.no_shmlog++;
        break;
      case OPT_DEBUG:
        zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
        if (jobq.daemonize == 0)
          jobq.daemonize--;
        jobq.debug++;
        break;
      case OPT_CONCURRENT:
        for(;apr_isspace(*opt_arg);opt_arg++) ;
        do {
          char *end = NULL;
          apr_int64_t val = apr_strtoi64(opt_arg,&end,0);
          if((!end || *end) || val < 1 || val > 1000) {
            zeke_eprintf("Concurrency option must be between 1 and 1000 (got %s)\n",opt_arg);
            exit(13);
          }
          jobq.max_concurrent_jobs = (unsigned)val;
        } while(0);
        break;
      case OPT_IDENT:
        jobq.ident = validate_node(apr_pstrdup(pool,opt_arg),"%s is not a valid node identifier.\n");
        break;
      case OPT_ZKHOSTS:
        for(;apr_isspace(*opt_arg);opt_arg++)
          ;
        zkhosts = apr_pstrdup(pool,opt_arg);
        if(!*zkhosts) {
          zeke_eprintf("Invalid connection string: %s\n",opt_arg);
          exit(10);
        }
        break;
      case OPT_MAX_RUN:
        st = zktool_parse_interval_time(&jobq.max_run,opt_arg,getopt_pool);
        if(!ZEKE_STATUS_IS_OKAY(st))
          zeke_fatal(opt_arg,st);
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

  AAOK(apr_pool_create(&jobq.pool,zkjob_pool));
  jobq.ns = ns;
  for(i = getopt->argc-1; i >= getopt->ind; i--)
    if(getopt->argv[i] != NULL && node == NULL) {
      node = validate_node(getopt->argv[i],"%s is not a valid node.\n");
      break;
    }

  if(node == NULL) {
    zeke_eprintf("No jobq zookeeper node specified.\n");
    usage(1);
  }

  jobq.ws = zk_workspace_new(jobq.pool);
  if(ns)
    AAOK(zeke_nodepath_chroot_set(ns));

  node = zk_workspace_nodecat(jobq.ws,node,NULL);
  AN(node);
  if(getopt_pool) {
    apr_pool_destroy(getopt_pool);
    getopt_pool = NULL;
  }

  if(jobq.no_shmlog && jobq.daemonize == 0)
    jobq.daemonize--;

#ifdef DEBUGGING
  if(jobq.daemonize == 0)
    jobq.daemonize = -1;
#else
  if(jobq.daemonize == 0 && jobq.debug < 2)
    jobq.daemonize = 1;
#endif

  i = start_zk(zkhosts,&jobq,node);
  apr_pool_destroy(zkjob_pool);
  return i;
}
