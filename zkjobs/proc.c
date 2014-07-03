#include "zkjobs.h"

#include "libzeke_log.h"
#include "libzeke_indirect.h"
#include <apr_atomic.h>
#include <apr_ring.h>

#ifndef ZKJOBD_DEFAULT_PATH
#define ZKJOBD_DEFAULT_PATH "/bin:/usr/bin:/usr/local/bin"
#endif

#ifndef DEFAULT_USER
#define DEFAULT_USER "nobody"
#endif

#ifndef DEFAULT_GROUP
#define DEFAULT_GROUP "nobody"
#endif

#define ZKPROC_POOL_PREFIX "zkjobq:"
#define ZKPROC_POOL_KEY ZKPROC_POOL_PREFIX "child process"

#define ZKPROC_RING_ENTRY APR_RING_ENTRY(zkproc) link
#define ZKPROC_RING_HEAD APR_RING_HEAD(zkproc_head,zkproc)
#define ZKPROC_RING_SENTINEL(hp) APR_RING_SENTINEL((hp),zkproc,link)
#define ZKPROC_RING_FIRST(hp) APR_RING_FIRST(hp)
#define ZKPROC_RING_LAST(hp) APR_RING_LAST(hp)
#define ZKPROC_RING_NEXT(ep) APR_RING_NEXT((ep),link)
#define ZKPROC_RING_PREV(ep) APR_RING_PREV((ep),link)
#define ZKPROC_RING_INIT(hp) APR_RING_INIT((hp),zkproc,link)
#define ZKPROC_RING_EMPTY(hp) APR_RING_EMPTY((hp),zkproc,link)
#define ZKPROC_RING_ELEM_INIT(ep) APR_RING_ELEM_INIT((ep),link)
#define ZKPROC_RING_INSERT_BEFORE(lep,nep) APR_RING_INSERT_BEFORE((lep),(nep),link)
#define ZKPROC_RING_INSERT_AFTER(lep,nep) APR_RING_INSERT_AFTER((lep),(nep),link)
#define ZKPROC_RING_INSERT_HEAD(hp,ep) APR_RING_INSERT_HEAD((hp),(ep),zkproc,link)
#define ZKPROC_RING_INSERT_TAIL(hp,ep) APR_RING_INSERT_TAIL((hp),(ep),zkproc,link)
#define ZKPROC_RING_REMOVE(ep) do { \
  APR_RING_REMOVE((ep),link); \
  APR_RING_ELEM_INIT((ep),link); \
} while(0)
#define ZKPROC_RING_FOREACH(ep,hp) APR_RING_FOREACH((ep),(hp),zkproc,link)
#define ZKPROC_RING_FOREACH_SAFE(ep,state,hp) APR_RING_FOREACH_SAFE((ep),(state),(hp),zkproc,link)
#define ZKPROC_RING_CHECK_CONSISTENCY(hp) APR_RING_CHECK_CONSISTENCY((hp),zkproc,link)
#define ZKPROC_RING_ELEM_IS_ORPHAN(ep) (APR_RING_NEXT((ep),link) == (ep) && \
                                         APR_RING_PREV((ep),link) == (ep))

#define ZKPROC_NUM_PIPESETS 3

typedef apr_status_t (*procattr_pipe_fn)(struct apr_procattr_t*,
                                         apr_file_t*, apr_file_t*);
struct zkproc_pipeset {
  apr_uint32_t magic;
#define ZKPROC_PIPESET_MAGIC 0xbbab101e
  unsigned short index;
  apr_pool_t *pool;
  apr_file_t *pipes[2];
  unsigned char slots[2];
};

struct zkproc {
  apr_uint32_t magic;
#define ZKPROC_MAGIC 0x409ba01f
  apr_uint32_t vxid;
  apr_uint64_t seq;
  apr_pool_t *pool;
  struct zkws *ws;
  zkjob_ref_t *job;
  apr_uint32_t flags;
  const char *cmd,*shell;
  apr_array_header_t *args;
  apr_table_t *env;
  const char *user,*group;
  const char *cwd;
  apr_proc_t *proc;
  apr_procattr_t *procattr;
  apr_interval_time_t timeout;
  struct {
    apr_exit_why_e why;
    int sig, code;
  } status;
  apr_time_t start;
  unsigned short termcnt;
  zkproc_pipeset_t *pipesets[ZKPROC_NUM_PIPESETS];

  ZKPROC_RING_ENTRY;
};

static ZKPROC_RING_HEAD procs;
static apr_pool_t *maintenance_pool = NULL;
static volatile apr_uint32_t initialized = 0;
static apr_table_t *default_env = NULL;
static apr_hash_t *valid_shells = NULL;
static char *sentinel_marker = "SENTINEL";
static zktool_vxid_pool_t *vxid_pool = NULL;
ZEKE_HIDDEN void *zkproc_sentinel = NULL;

static int zkproc_interrupt_handler(zeke_connection_t*);

static inline void zkjob_assert(const zkjob_ref_t *jr)
{
  const zkjob_t *j = zkqueue_job_ref_get(jr);
  ZK_CHECK_OBJ_NOTNULL(j, ZKJOB_MAGIC);
  ZK_CHECK_OBJ_NOTNULL(j->ws, ZK_WS_MAGIC);
}

static inline void zkproc_assert(const zkproc_t *c)
{
  ZK_CHECK_OBJ_NOTNULL(c, ZKPROC_MAGIC);
  ZK_CHECK_OBJ_NOTNULL(c->ws, ZK_WS_MAGIC);
}

static inline char *pstrdup_iif(apr_pool_t *p, const char *s)
{
  if(s != NULL) {
    char *sp = apr_pstrdup(p,s);
    AN(sp);
    return sp;
  }
  return NULL;
}

static void zkproc_ring_tryinit(void)
{
  if(apr_atomic_read32(&initialized) == 0) {
    static volatile apr_uint32_t spinlock = 0;
    while(apr_atomic_cas32(&spinlock,1,0) != 0) ;
    if(apr_atomic_read32(&initialized) == 0) {
      ZKPROC_RING_INIT(&procs);
      apr_atomic_inc32(&initialized);
    }
    AZ(maintenance_pool);
    AAOK(apr_pool_create_unmanaged(&maintenance_pool));
    zeke_pool_tag(maintenance_pool,"global zkproc unmanaged maintenance pool");
    AZ(zkproc_sentinel);
    zkproc_sentinel = sentinel_marker;
    AZ(vxid_pool);
    vxid_pool = zktool_vxid_factory(maintenance_pool);
    zeke_pool_cleanup_indirect(maintenance_pool,vxid_pool);
#ifndef LIBZEKE_USE_LIBEVENT
    zeke_interrupt_handler_set(zkproc_interrupt_handler);
#endif
    assert(apr_atomic_dec32(&spinlock) == 0);
  }
}

ZEKE_HIDDEN
void zkproc_initialize(void)
{
  if(!initialized) zkproc_ring_tryinit();
}

static
apr_status_t zkp_file_close(void *fv)
{
  apr_file_close((apr_file_t*)fv);
  return APR_SUCCESS;
}

static
apr_status_t zkp_child_file_close(void*fv)
{
  return zkp_file_close(fv);
}

static
apr_file_t *devnull(void)
{
  static apr_file_t *f = NULL;
  if(!initialized) zkproc_ring_tryinit();

  if(f == NULL) {
    assert(apr_file_open(&f,"/dev/null",APR_READ|APR_WRITE,APR_OS_DEFAULT,maintenance_pool) == APR_SUCCESS);
    apr_file_inherit_unset(f);
    apr_pool_cleanup_register(maintenance_pool,f,zkp_file_close,zkp_child_file_close);
  }
  AN(f);
  return f;
}

static
apr_hash_t *env_blacklist(apr_pool_t *p)
{
  apr_hash_t *bl = NULL;

  if(apr_pool_userdata_get((void**)&bl,"ZKJOBQ:ENV:BLACKLIST",p) != APR_SUCCESS ||
     bl == NULL) {
    bl = apr_hash_make(p);
    apr_hash_set(bl,"PATH",APR_HASH_KEY_STRING,env_blacklist);
    apr_hash_set(bl,"LD_PRELOAD",APR_HASH_KEY_STRING,env_blacklist);
    apr_hash_set(bl,"LD_DEBUG",APR_HASH_KEY_STRING,env_blacklist);
    apr_hash_set(bl,"HOME",APR_HASH_KEY_STRING,env_blacklist);
    apr_hash_set(bl,"MAIL",APR_HASH_KEY_STRING,env_blacklist);
    apr_hash_set(bl,"USER",APR_HASH_KEY_STRING,env_blacklist);
    apr_hash_set(bl,"LOGNAME",APR_HASH_KEY_STRING,env_blacklist);
    apr_hash_set(bl,"USERNAME",APR_HASH_KEY_STRING,env_blacklist);
    apr_hash_set(bl,"SHELL",APR_HASH_KEY_STRING,env_blacklist);
    apr_pool_userdata_setn(bl,"ZKJOBQ:ENV:BLACKLIST",NULL,p);
  }
  return bl;
}

ZEKE_HIDDEN
void zkproc_init_default_environment(const char *const *envp)
{
  apr_pool_t *p;
  const char * const*ep;
  struct zkws *ws;
  apr_hash_t *blacklist;

  if(!initialized) zkproc_ring_tryinit();
  AN(maintenance_pool);
  if(default_env)
    apr_pool_destroy(apr_table_elts(default_env)->pool);
  AAOK(apr_pool_create(&p,maintenance_pool));
  default_env = apr_table_make(p,20);
  AN(default_env);
  zeke_pool_cleanup_indirect(p,default_env);
  ws = zk_workspace_new_ex(p,"global default environment workspace");
  ZK_CHECK_OBJ_NOTNULL(ws, ZK_WS_MAGIC);
  blacklist = env_blacklist(maintenance_pool);

  for(ep = envp; *ep != NULL; ep++) {
    char *s,*ev = zk_workspace_strdup(ws,*ep);
    AN(ev);
    for(; apr_isspace(*ev); ev++)
      ;
    if(!*ev || *ev == '=')
      continue;
    s = strchr(ev,'=');
    if(s) {
      for(*s++ = '\0'; apr_isspace(*s); s++)
        ;
      if(*s && *ev && !apr_hash_get(blacklist,ev,APR_HASH_KEY_STRING))
        apr_table_setn(default_env,ev,s);
    }
  }

  AZ(valid_shells);
  zeke_pool_cleanup_indirect(p,valid_shells);
  valid_shells = apr_hash_make(p);
  do {
    apr_status_t st;
    apr_size_t u,sz,blk = 1024;
    apr_file_t *f;
    apr_off_t save,rest = 0;
    char *cp,*r;
    if((st = apr_file_open(&f,"/etc/shells",APR_READ,APR_OS_DEFAULT,p)) == APR_SUCCESS) {
      save = zk_workspace_snapshot(ws);
      u = zk_workspace_reserve(ws,blk+1);
      assert(u > blk);
      cp = r = ws->f;
      st = apr_file_read_full(f,cp,u,&sz);
      while(st == APR_SUCCESS || (st == APR_EOF && sz > 0)) {
        sz += rest;
        for(; apr_isspace(*r); r++)
          ;
        for(cp = r; cp < ws->f+sz; cp++) {
          if(*cp == '\r' || *cp == '\n') {
            *cp++ = '\0';
            if(sz > 0 && *r == '/') {
              char *ros = cp;
              const char *ss;
              rest = (ws->f+sz)-cp;
              r = zk_workspace_releasep(ws,cp);
              ss = apr_pstrdup(p,r);
              apr_hash_set(valid_shells,ss,APR_HASH_KEY_STRING,ss);
              u = zk_workspace_reserve(ws,blk+1);
              assert(u > blk);
              r = ws->f;
              if(rest > 0)
                memmove(ws->f,ros,rest);
              r = ws->f;
              cp = r-1;
            } else {
              for(r = cp; apr_isspace(*r) && r < ws->f+sz; r++)
                cp = r-1;
            }
          }
        }
        rest = ((ws->b + u) - cp);
        if(rest <= 1) {
          apr_size_t off = (cp - ws->b);
          rest = (r - ws->b);
          blk *= 2;
          zk_workspace_release(ws,0);
          u = zk_workspace_reserve(ws,blk+1);
          assert(u > blk);
          r = ws->b + rest;
          cp = ws->b + off;
          rest = 0;
        }
        st = apr_file_read_full(f,cp,u-(cp-ws->f),&sz);
      }
      apr_file_close(f);
      zk_workspace_release(ws,0);
      zk_workspace_restore(ws,save);
    } else {
      zeke_apr_error("/etc/shells",st);
    }
  } while(0);
  /* set default sensible path */
  apr_table_setn(default_env,"PATH",ZKJOBD_DEFAULT_PATH);
}

ZEKE_HIDDEN
int zkproc_is_valid_shell(const char *s)
{
  if(s && valid_shells && apr_hash_get(valid_shells,s,APR_HASH_KEY_STRING))
    return 1;
  return 0;
}

static void zk_proc_maintenance(int reason, void *ctx, int status)
{
  zkproc_t *c = (zkproc_t*)ctx;
  static apr_pool_t *p = NULL;
  pid_t pid = 0;

  int unreg = 0;
  AN(maintenance_pool);
  if(p == NULL)
    AAOK(apr_pool_create(&p,maintenance_pool));

#if 0
  INFO("zk_trigger_maint: reason=%d/%d/%d status=0x%04x",reason,APR_OC_REASON_UNREGISTER,
       APR_OC_REASON_DEATH,status);
#endif
  if(c && (c->flags & ZKPROCF_RUNNING) && reason != APR_OC_REASON_RUNNING) {
    zkjob_t *job = (c->job ? zkqueue_job_ref_get(c->job) : NULL);

    if(job) {
      INFO("zk_trigger_maint: " VXID_FMT "/%u is running, job lock=%d, c->ws->magic=0x%08lx, status=0x%04x, reason=0x%04x",
            VXID_C(c->vxid),(unsigned)c->proc->pid,(job ? zkqueue_is_job_locked(job) : -1),
            (unsigned long)(c->ws->magic),status,reason);
    }
    zkproc_assert(c);
    AN(c->proc);
    if(reason != APR_OC_REASON_RUNNING && reason != APR_OC_REASON_UNREGISTER) {
      unreg++;
      c->flags &= ~ZKPROCF_RUNNING;
      c->flags |= ZKPROCF_DEAD;
      INFO("PROCESS " VXID_FMT "/%u death reason=0x%04x status=0x%04x",
           VXID_C(c->vxid),(c->proc ? c->proc->pid : 0),
           (unsigned)reason,(unsigned)status);
      c->status.why = status;
      if(APR_PROC_CHECK_SIGNALED(status) || APR_PROC_CHECK_CORE_DUMP(status)) {
        c->status.why = APR_PROC_SIGNAL;
        c->status.sig = ZKTOOL_STATUS_SIGNAL(status);
        c->status.code = status = 0;
        if (c->status.sig) {
          WARN("child %d signaled %d",c->proc->pid,c->status.sig);
          c->flags |= ZKPROCF_SIGNALED;
        }
      } else {
        c->status.sig = 0;
        c->status.code = ZKTOOL_STATUS_EXITCODE(status);
        WARN("child %d exited with code %d",c->proc->pid,c->status.code);
      }
    }
  }

  if(c->proc)
    pid = c->proc->pid;

  if(reason != APR_OC_REASON_UNREGISTER) {
    if(unreg && ctx)
      apr_proc_other_child_unregister(ctx);
    else if(c && (c->flags & ZKPROCF_DEAD)) {
      apr_proc_other_child_unregister(c);
    }
  } else if(c->flags & ZKPROCF_DEAD)
    c->proc = NULL;
  if(reason == APR_OC_REASON_UNREGISTER) {
    int shut = zeke_is_safe_shutdown(NULL);
    INFO("UNREGISTER: vxid=" VXID_FMT " pid=%d",
        VXID_C(c->vxid),(pid ? pid : 666));
    assert((c->flags & ZKPROCF_REGISTERED));
    if(!(c->flags & ZKPROCF_REGISTERED) && !(c->flags & ZKPROCF_PIPELINE))
      WARN("Attempt to unregister job #%" APR_UINT64_T_FMT,c->seq);
    if(!shut)
      assert((c->flags & ZKPROCF_DEAD) != 0 || (c->flags & ZKPROCF_PIPELINE));
    c->flags &= ~ZKPROCF_REGISTERED;
    if(!shut && c->vxid && (c->flags & ZKPROCF_PIPELINE) == 0) {
      unsigned short cnt = 0;
      apr_time_t now = apr_time_now();
      zkproc_t *rel;
      ZKPROC_RING_FOREACH(rel,&procs) {
        if(rel != c && rel->timeout == APR_TIME_C(-1) &&
                        rel->vxid == c->vxid && (rel->flags & ZKPROCF_DEAD) == 0 &&
                       (rel->flags & (ZKPROCF_REGISTERED|ZKPROCF_RUNNING))) {
          rel->timeout = APR_TIME_C(100 * 1000);
          rel->start = now;
          cnt++;
        }
      }
      if(cnt > 0)
        INFO("UNREGISTER: selected %u related procs with vxid " VXID_FMT " for killing in 100ms",
            (unsigned)cnt,VXID_C(c->vxid));
    }
  }
  apr_pool_clear(p);
}

static void child_error(apr_pool_t *p, apr_status_t st, const char *desc)
{
  zkproc_t *c;
  AN(p);
  apr_pool_userdata_get((void**)&c,ZKPROC_POOL_KEY,p);
  assert(c != NULL && c == NULL);
  if(c) {
    zkproc_assert(c);
    zkjob_assert(c->job);
    ERR("Job #%" APR_UINT64_T_FMT "/%s could not start: %s",
        c->seq,c->cmd,desc);
    c->flags &= ~ZKPROCF_RUNNING;
    c->flags |= (ZKPROCF_DEAD|ZKPROCF_START_ERROR);
    c->start = APR_TIME_C(0);
  }
}

static
apr_status_t child_cleanup(void *ind)
{
  zkproc_t *c = (zkproc_t*)zeke_indirect_consume((zeke_indirect_t*)ind);

  if(c) {
    zkproc_assert(c);

    if(!ZKPROC_RING_ELEM_IS_ORPHAN(c))
      ZKPROC_RING_REMOVE(c);
    if(c->proc && (c->flags & ZKPROCF_REGISTERED))
      apr_proc_other_child_unregister(c);
    c->job = NULL;
  }
  return APR_SUCCESS;
}

static int dump_header(void *jv, const char *key, const char *val)
{
  zkjob_t *j = (zkjob_t*)jv;
  AN(j);
  zeke_eprintf("job #%" APR_UINT64_T_FMT " %s: %s\n",
               j->seq,key,val);
  return 1;
}

static void dump_job(zkjob_ref_t *job, apr_pool_t *p)
{
  zkjob_t *j = zkqueue_job_ref_get(job);
  AN(j);
  if(!p)
    p = j->pool;

  if(j->headers) {
    apr_table_do(dump_header,j,j->headers,j,NULL);
  } else {
    zeke_eprintf("job #%" APR_UINT64_T_FMT " has no headers.\n",j->seq);
  }
}

ZEKE_HIDDEN
zk_status_t zkproc_pipeset_create(zkproc_pipeset_t **psp, unsigned short index,
                                  apr_pool_t *p)
{
  apr_file_t *rpipe,*wpipe;
  apr_status_t st = apr_file_pipe_create_ex(&rpipe,&wpipe,APR_FULL_BLOCK,p);

  if(st != APR_SUCCESS)
    return st;
  apr_file_inherit_unset(rpipe);
  apr_pool_cleanup_register(p,rpipe,zkp_file_close,zkp_child_file_close);
  apr_pool_cleanup_register(p,wpipe,zkp_file_close,zkp_child_file_close);
  apr_file_inherit_unset(wpipe);
  AN(psp);
  *psp = apr_pcalloc(p,sizeof(**psp));
  AN(*psp);
  (*psp)->magic = ZKPROC_PIPESET_MAGIC;
  (*psp)->pool = p;
  (*psp)->index = index;
  (*psp)->pipes[zkproc_pipeset_read] = rpipe;
  (*psp)->pipes[zkproc_pipeset_write] = wpipe;
  apr_pool_pre_cleanup_register(p,psp,zeke_indirect_wipe);
  return st;
}

ZEKE_HIDDEN
unsigned short zkproc_pipeset_index_get(const zkproc_pipeset_t *pset)
{
  ZK_CHECK_OBJ_NOTNULL(pset, ZKPROC_PIPESET_MAGIC);
  assert(pset->index >= 0);
  assert(pset->index < ZKPROC_NUM_PIPESETS);
  return pset->index;
}

ZEKE_HIDDEN
int zkproc_pipeset_inuse(const zkproc_pipeset_t *pset,
                                zkproc_pipeset_type t)
{
  ZK_CHECK_OBJ_NOTNULL(pset, ZKPROC_PIPESET_MAGIC);
  return pset->slots[t] > 0 ? 1 : 0;
}

ZEKE_HIDDEN
apr_pool_t *zkproc_pipeset_pool_get(const zkproc_pipeset_t *pset)
{
  ZK_CHECK_OBJ_NOTNULL(pset, ZKPROC_PIPESET_MAGIC);
  return pset->pool;
}

ZEKE_HIDDEN
zk_status_t zkproc_pipeset_assign(zkproc_pipeset_t *pset,
                                  zkproc_pipeset_type t,
                                  apr_file_t **end,
                                  apr_uint32_t opts,
                                  zkproc_t *c)
{
  zk_status_t st = APR_SUCCESS;
  procattr_pipe_fn set_in,set_out,set_err;
  zkproc_pipeset_type other;

  zkproc_assert(c);
  ZK_CHECK_OBJ_NOTNULL(pset, ZKPROC_PIPESET_MAGIC);
  assert(t >= zkproc_pipeset_read && t <= zkproc_pipeset_write);
  assert(pset->index >= 0);
  assert(pset->index < ZKPROC_NUM_PIPESETS);
  other = (t == zkproc_pipeset_read ? zkproc_pipeset_write : zkproc_pipeset_read);

  if(pset->slots[t] || (end && pset->slots[other]))
    return APR_EBUSY;
  if(end)
    other = (t == zkproc_pipeset_read ? zkproc_pipeset_write : zkproc_pipeset_read);
  if (c->flags & (ZKPROCF_DEAD|ZKPROCF_REGISTERED|ZKPROCF_RUNNING|ZKPROCF_START_ERROR))
    return APR_EINVAL;
  if(c->pipesets[pset->index] == NULL) {
    c->pipesets[pset->index] = pset;
#if 0
    if(c->pool != pset->pool) {
      apr_pool_cleanup_register(c->pool,pset->pipes[t],zkp_file_close,zkp_child_file_close);
    }
#endif
  } else if(c->pipesets[pset->index] != pset)
    return APR_EGENERAL;

  pset->slots[t]++;
  if(end) {
    assert(pset->slots[other] == 0);
    pset->slots[other]++;
    *end = pset->pipes[other];
  }

  AN(c->procattr);
  set_in = apr_procattr_child_in_set;
  set_out = ((opts & ZKPROC_PIPESET_OPT_NO_STDOUT) == 0 ? apr_procattr_child_out_set : NULL);
  set_err = ((opts & ZKPROC_PIPESET_OPT_NO_STDERR) == 0 ? apr_procattr_child_err_set : NULL);

 #if 0
  switch(pset->index) {
  case ZKPROC_PIPESET_STDIN:
    set_in = apr_procattr_child_in_set;
    set_out = ((opts & ZKPROC_PIPESET_OPT_NO_STDOUT) == 0 ? NULL : apr_procattr_child_out_set);
    set_err = ((opts & ZKPROC_PIPESET_OPT_NO_STDERR) == 0 ? NULL : apr_procattr_child_err_set);
    break;
  case ZKPROC_PIPESET_STDOUT:
    set_in = apr_procattr_child_out_set;
    set_out = apr_procattr_child_in_set;
    set_err = ((opts & ZKPROC_PIPESET_OPT_MAPPING) ==
                       ZKPROC_PIPESET_OPT_STDOUT_TO_STDERR) ? apr_procattr_child_in_set : NULL;
    break;
  case ZKPROC_PIPESET_STDERR:
    set_in = apr_procattr_child_out_set;
    set_out = ((opts & ZKPROC_PIPESET_OPT_MAPPING) ==
                       ZKPROC_PIPESET_OPT_STDERR_TO_STDOUT) ? apr_procattr_child_in_set : NULL;
    set_err = apr_procattr_child_in_set;
    break;
  }
#endif
  switch(t) {
  case zkproc_pipeset_read:
    st = set_in(c->procattr,pset->pipes[t],NULL);
    break;
  case zkproc_pipeset_write:
    if(set_out)
      st = set_out(c->procattr,pset->pipes[t],NULL);
    if(st == APR_SUCCESS && set_err != NULL)
      st = set_err(c->procattr,pset->pipes[t],NULL);
    break;
  default:
    assert("not possible" == NULL);
  }
  return st;
}

ZEKE_HIDDEN
zk_status_t zkproc_pipeset_get(apr_file_t **fp,
                               zkproc_pipeset_type t,
                               zkproc_pipeset_t *pset)
{
  ZK_CHECK_OBJ_NOTNULL(pset, ZKPROC_PIPESET_MAGIC);
  AN(fp); assert(t >= zkproc_pipeset_read && t <= zkproc_pipeset_write);
  if(pset->slots[t])
    return APR_EBUSY;
  pset->slots[t]++;
  *fp = pset->pipes[t];
  return APR_SUCCESS;
}

ZEKE_HIDDEN
zk_status_t zkproc_create_pipeline(zkproc_t **cp, zkproc_t *parent,
                                   const char *cmd,
                                   apr_table_t *env,
                                   apr_uint32_t flags)
{
  apr_pool_t *p;
  zk_status_t st = ZEOK;
  zkproc_t *c, *top;
  zkjob_ref_t *job;
  zkjob_t *j;
  zkproc_pipeset_t *pipeset = NULL;

  if(!initialized) zkproc_ring_tryinit();

  ERR("CREATE PIPELINE!!!");
  zkproc_assert(parent);
  job = parent->job;
  zkjob_assert(job);
  j = zkqueue_job_ref_get(job);

  AAOK(apr_pool_create(&p,parent->pool));

  if(!cmd) {
    if(j->headers) {
      cmd = apr_table_get(j->headers,"command");
      if(cmd) {
        while(apr_isspace(*cmd)) cmd++;
        if(!*cmd)
          cmd = NULL;
      }
    }
    if(!cmd)
      return APR_EINVAL;
  } else {
    while(apr_isspace(*cmd)) cmd++;
    cmd = apr_pstrdup(p,cmd);
  }
  for(top = parent; (top->flags & ZKPROCF_PIPELINE) != 0;) {
    ZKPROC_RING_FOREACH(c, &procs) {
      if((c->flags & ZKPROCF_PIPELINE) == 0 && c->vxid == top->vxid) {
        top = c;
        break;
      }
    }
    if(!top || !zkqueue_job_ref_eq(top->job,job) ||top->seq != parent->seq || top == parent) {
      ERR("cannot find starting process for pipeline of job #%" APR_UINT64_T_FMT" (%s/%s)",
          parent->seq,parent->cmd,cmd);
      st = APR_EGENERAL;
      goto zkproc_create_pipeline_error;
    }
  }

  c = apr_pcalloc(p,sizeof(*c));
  c->magic = ZKPROC_MAGIC;
  c->pool = p;
  c->vxid = top->vxid;
  zkjob_assert(top->job);
  j = zkqueue_job_ref_get(top->job);
  AN(j);
  c->job = job = zkqueue_job_ref_make(j,p);
  zkjob_assert(c->job);
  ZKPROC_RING_ELEM_INIT(c);
  c->ws = zk_workspace_new_ex(p,"zkjobd pipeline process");
  flags &= ~ZKPROCF_STDIN_PIPE;
  c->flags = flags | ZKPROCF_PIPELINE;
  c->cwd = parent->cwd;
  c->timeout = APR_TIME_C(-1);
  if(parent->pipesets[ZKPROC_PIPESET_STDOUT])
    pipeset = parent->pipesets[ZKPROC_PIPESET_STDOUT];
  else if(parent->pipesets[ZKPROC_PIPESET_STDERR])
    pipeset = parent->pipesets[ZKPROC_PIPESET_STDERR];
  else {
    st = zkproc_pipeset_create(&parent->pipesets[ZKPROC_PIPESET_STDERR],
                               ZKPROC_PIPESET_STDERR,
                               parent->pool);
    if(!ZEKE_STATUS_IS_OKAY(st)) {
      zeke_apr_eprintf(st,"cannot create output pipeset for '%s'",parent->cmd);
      goto zkproc_create_pipeline_error;
    }
    pipeset = parent->pipesets[ZKPROC_PIPESET_STDERR];
    AN(pipeset);
    apr_pool_pre_cleanup_register(p,&parent->pipesets[ZKPROC_PIPESET_STDERR],zeke_indirect_wipe);
  }
  AN(pipeset);
  c->pipesets[ZKPROC_PIPESET_STDIN] = pipeset;
  apr_pool_pre_cleanup_register(p,&c->pipesets[ZKPROC_PIPESET_STDIN],zeke_indirect_wipe);
  c->seq = top->seq;
  if((c->flags & ZKPROCF_USE_SHELL) == 0)
    if(!c->cmd || *c->cmd != '/' || strpbrk(c->cmd,"~><()$*\\\"'&|;[]") != NULL)
      c->flags |= ZKPROCF_USE_SHELL;
  if(c->flags & ZKPROCF_USE_SHELL) {
    c->args = apr_array_make(p,1,sizeof(const char*));
    APR_ARRAY_PUSH(c->args,const char*) = cmd;
    c->cmd = "/bin/sh";
  } else {
    char **argv = NULL;
    st = apr_tokenize_to_argv(cmd,&argv,p);
    if(st != APR_SUCCESS)
      goto zkproc_create_pipeline_error;
    c->args = apr_array_make(p,3,sizeof(const char*));
    AN(c->args);
    for(; *argv; argv++)
      APR_ARRAY_PUSH(c->args,const char*) = *argv;
    if(c->args->nelts == 0) {
      st = APR_EINVAL;
      goto zkproc_create_pipeline_error;
    }
    c->cmd = APR_ARRAY_IDX(c->args,0,const char*);
  }

  if(!c->args->nelts || APR_ARRAY_IDX(c->args,c->args->nelts-1,const char*) != NULL)
    APR_ARRAY_PUSH(c->args,const char*) = NULL;

  if(IS_SENTINEL(env))
    c->env = apr_table_clone(p,parent->env);
  else {
    AN(env);
    c->env = apr_table_clone(p,env);
  }
  c->user = parent->user;
  c->group = parent->group;
  c->flags &= ~(ZKPROCF_RUNNING|ZKPROCF_DEAD|ZKPROCF_SIGNALED|ZKPROCF_ERROR_OUTPUT);
  if(ZEKE_STATUS_IS_OKAY(st)) {
    apr_int32_t io_in = APR_NO_FILE, io_out = APR_NO_PIPE, io_err = APR_NO_PIPE;
    apr_file_t *pipe = NULL;
    apr_procattr_t *attr;
    st = apr_procattr_create(&attr,p);
    if(st != APR_SUCCESS)
      goto zkproc_create_pipeline_error;
    st = apr_procattr_io_set(attr,io_in,io_out,io_err);
    if (st != APR_SUCCESS)
      goto zkproc_create_pipeline_error;
    if(c->cwd) {
      st = apr_procattr_dir_set(attr,c->cwd);
      if (st != APR_SUCCESS)
        goto zkproc_create_pipeline_error;
    }
    if(c->user) {
      st = apr_procattr_user_set(attr,c->user,NULL);
      if(st != APR_SUCCESS)
        goto zkproc_create_pipeline_error;
    }
    if(c->group) {
      st = apr_procattr_group_set(attr,c->group);
      if(st != APR_SUCCESS)
        goto zkproc_create_pipeline_error;
    }
    st = zkproc_pipeset_get(&pipe,zkproc_pipeset_read,pipeset);
    if(!ZEKE_STATUS_IS_OKAY(st)) {
      zeke_apr_eprintf(st,"cannot get read pipe for %s",c->cmd);
      goto zkproc_create_pipeline_error;
    }
    AN(pipe);
    st = apr_procattr_child_in_set(attr,pipe,NULL);
    /*apr_procattr_child_out_set(attr,devnull(),NULL);
    apr_procattr_child_err_set(attr,devnull(),NULL);
    */
    apr_file_close(pipe);
    if(st != APR_SUCCESS) {
      zeke_apr_eprintf(st,"cannot assign pipe for %s",c->cmd);
      goto zkproc_create_pipeline_error;
    }
    pipe = NULL;
    assert((parent->flags & (ZKPROCF_RUNNING|ZKPROCF_START_ERROR|ZKPROCF_DEAD)) == 0);
    st = zkproc_pipeset_get(&pipe,zkproc_pipeset_write,pipeset);
    apr_procattr_child_in_set(parent->procattr,devnull(),NULL);
    AN(pipe);
    if(ZEKE_STATUS_IS_OKAY(st)) {
      switch(pipeset->index) {
      case ZKPROC_PIPESET_STDOUT:
        st = apr_procattr_child_out_set(parent->procattr,pipe,NULL);
        break;
      case ZKPROC_PIPESET_STDERR:
        st = apr_procattr_child_err_set(parent->procattr,pipe,NULL);
        apr_file_close(pipe);
        break;
      default:
        ERR("unsupported pipeset index %d",(int)pipeset->index);
        goto zkproc_create_pipeline_error;
        break;
      }
      if(st != APR_SUCCESS) {
        zeke_apr_error("apr_procattr",st);
        abort();
        goto zkproc_create_pipeline_error;
      }
    } else if(st != APR_EBUSY) {
      zeke_apr_eprintf(st,"cannot access write pipe for pipe '%s'",parent->cmd);
      goto zkproc_create_pipeline_error;
    }

    apr_procattr_error_check_set(attr,1);
    apr_procattr_child_errfn_set(attr,child_error);
    if(c->env) {
      if(c->flags & ZKPROCF_USE_SHELL)  {
        st = apr_procattr_cmdtype_set(attr,APR_SHELLCMD);
      } else if(apr_table_get(c->env,"PATH") && (c->flags & ZKPROCF_CUSTOM_PATH)) {
        st = apr_procattr_cmdtype_set(attr,APR_PROGRAM_PATH);
      } else {
        st = apr_procattr_cmdtype_set(attr,APR_PROGRAM);
      }
      if(st != APR_SUCCESS)
        goto zkproc_create_pipeline_error;
    } else if (c->flags & ZKPROCF_USE_SHELL)
      st = apr_procattr_cmdtype_set(attr,APR_SHELLCMD_ENV);
    else
      st = apr_procattr_cmdtype_set(attr,APR_PROGRAM_ENV);

    c->procattr = attr;
  }

zkproc_create_pipeline_error:
  if(!ZEKE_STATUS_IS_OKAY(st))
    apr_pool_destroy(p);
  else {
    AN(j);
    apr_pool_pre_cleanup_register(j->pool,zeke_indirect_make(c,c->pool),child_cleanup);
    *cp = c;
  }
  return st;
}

ZEKE_HIDDEN
zk_status_t zkproc_create_ex(zkproc_t **cp, zkjob_t *job,
                             const char *cmd,
                             apr_array_header_t *args,
                             apr_table_t *env,
                             apr_uint32_t flags,
                             apr_pool_t *pool)
{
  apr_pool_t *p;
  const char *logcmd = NULL;
  apr_table_t *etab = NULL;
  zk_status_t st = ZEOK;
  zkproc_t *c;
  int fixups = 0;

  ZK_CHECK_OBJ_NOTNULL(job, ZKJOB_MAGIC);
  if(!pool)
    pool = job->pool;
  if(!cmd) {
    if(job->headers) {
      cmd = apr_table_get(job->headers,"command");
      if(cmd) {
        while(apr_isspace(*cmd)) cmd++;
        if(!*cmd)
          cmd = NULL;
      }
    }
    if(!cmd)
      return APR_EINVAL;
  } else {
    while(apr_isspace(*cmd)) cmd++;
    cmd = apr_pstrdup(pool,cmd);
  }
  AN(cp);

  if(!initialized) zkproc_ring_tryinit();
  AAOK(apr_pool_create(&p,pool));
  AN(p);
  c = apr_pcalloc(p,sizeof(*c));
  c->magic = ZKPROC_MAGIC;
  c->pool = p;
  c->vxid = VXID_Next();

  ZKPROC_RING_ELEM_INIT(c);
  c->ws = zk_workspace_new_ex(p,"zkjobd child process");
  ZK_CHECK_OBJ_NOTNULL(c->ws, ZK_WS_MAGIC);
  c->flags = flags;
  c->job = zkqueue_job_ref_make(job,p);
  c->seq = job->seq;
  if(!args) {
    const char *cp;
    if(flags & ZKPROCF_USE_SHELL) {
      args = apr_array_make(p,1,sizeof(const char*));
      APR_ARRAY_PUSH(args,const char*) = cmd;
      for(cp = cmd; *cp; cp++)
        fixups += (apr_isspace(*cp) ? 1 : 0);
      if(args->nelts == 1 && fixups) {
        for(cp = cmd; apr_isspace(*cp); cp++)
          cmd++;
        for(cp = cmd; !apr_isspace(*cp); cp++)
          ;
        cmd = apr_pstrndup(p,cmd,(cp-cmd));
        fixups = 0;
      }
    } else if(!(flags & ZKPROCF_NO_PROCTITLE)) {
      char **argv = NULL;
      st = apr_tokenize_to_argv(cmd,&argv,p);
      if(st != APR_SUCCESS)
        goto zkproc_create_error;
      args = apr_array_make(p,3,sizeof(const char*));
      AN(args);
      for(; *argv; argv++)
        APR_ARRAY_PUSH(args,const char*) = *argv;
      if(args->nelts == 0) {
        st = APR_EINVAL;
        goto zkproc_create_error;
      }
      cmd = APR_ARRAY_IDX(args,0,const char*);
    } else {
      args = apr_array_make(p,1,sizeof(const char*));
      for(cp = cmd; *cp; cp++)
        fixups += (apr_isspace(*cp) ? 1 : 0);
    }
  } else
    args = apr_array_copy(p,args);

  c->args = args;
  c->cmd = cmd;
  logcmd = cmd;
  if(APR_ARRAY_IDX(args,args->nelts-1,const char*) != NULL)
    APR_ARRAY_PUSH(args,const char*) = NULL;
  AN(default_env);
  etab = apr_table_clone(p,default_env);
  if(env) {
    apr_hash_t *blacklist = env_blacklist(maintenance_pool);
    const char *key;
    char *val;
    apr_hash_index_t *hi;
    apr_hash_t *h = zktool_table_index_get(env);
    AN(h);
    for(hi = apr_hash_first(pool, h); hi; hi = apr_hash_next(hi)) {
      apr_hash_this(hi,(const void**)&key,NULL,(void**)&val);
      if(key && val && !apr_hash_get(blacklist,key,APR_HASH_KEY_STRING))
        apr_table_set(etab,key,val);
    }
  }
  AN(etab);
  if(job->headers) {
    const char *s = NULL;
    char *u = NULL, *g = NULL;
    apr_uid_t uid;
    apr_gid_t gid;
    c->cwd = pstrdup_iif(p,apr_table_get(job->headers,"directory"));
    if(flags & ZKPROCF_ALLOW_PATH_OVERRIDE) {
      const char *path = apr_table_get(job->headers,"path");
      if(path)
        apr_table_set(etab,"PATH",path);
    }
    s = apr_table_get(job->headers,"shell");
    if((flags & ZKPROCF_USE_SHELL) && s) {
      s = apr_hash_get(valid_shells,s,APR_HASH_KEY_STRING);
      if(s)
        c->shell = apr_pstrdup(p,s);
    }
    u = (char*)apr_table_get(job->headers,"user");
    if(u && apr_uid_get(&uid,&gid,u,p) == APR_SUCCESS)
    {
      c->user = apr_pstrdup(p,u);
      if(apr_gid_name_get(&g,gid,p) != APR_SUCCESS)
        g = NULL;

    }
    if(g == NULL) {
      g = (char*)apr_table_get(job->headers,"group");
      if(g && apr_gid_get(&gid,g,p) == APR_SUCCESS)
        c->group = apr_pstrdup(p,g);
    } else {
      char *reqg = (char*)apr_table_get(job->headers,"group");
      if(reqg && apr_gid_get(&gid,reqg,p) == APR_SUCCESS)
        c->group = apr_pstrdup(p,reqg);
    } c->group = apr_pstrdup(p,g);
  }
  if(!c->env && apr_table_elts(etab)->nelts > 0)
    c->env = etab;
  c->flags &= ~(ZKPROCF_RUNNING|ZKPROCF_DEAD|ZKPROCF_SIGNALED|ZKPROCF_ERROR_OUTPUT);
  if(ZEKE_STATUS_IS_OKAY(st)) {
    apr_int32_t io_in = APR_CHILD_BLOCK, io_out = APR_CHILD_BLOCK, io_err = APR_CHILD_BLOCK;
    apr_procattr_t *attr;
    st = apr_procattr_create(&attr,p);
    if(st != APR_SUCCESS)
      goto zkproc_create_error;
    /*if(c->flags & ZKPROCF_ERROR_OUTPUT)
      io_err = APR_NO_PIPE;
    */
    st = apr_procattr_io_set(attr,io_in,io_out,io_err);
    /*
    apr_procattr_child_out_set(attr,devnull(),NULL);
    apr_procattr_child_out_set(attr,devnull(),NULL);
     */
    if (st != APR_SUCCESS)
      goto zkproc_create_error;
    if(c->cwd) {
      st = apr_procattr_dir_set(attr,c->cwd);
      if (st != APR_SUCCESS)
        goto zkproc_create_error;
    }
    if(geteuid() == 0) {
      if(c->user) {
#ifdef ZKJOBD_PROHIBIT_ROOT_JOBS
        if(strcasecmp(c->user,"root") == 0) {
          ERR("attempt to run job '%s' as root user!",logcmd);
          st = APR_EACCES;
          goto zkproc_create_error;
        }
#endif
        if(c->env) {
          apr_table_setn(c->env,"USER",c->user);
          apr_table_setn(c->env,"LOGNAME",c->user);
        }
        st = apr_procattr_user_set(attr,c->user,NULL);

      } else {
        if(c->env) {
          apr_table_setn(c->env,"USER",DEFAULT_USER);
          apr_table_setn(c->env,"LOGNAME",DEFAULT_USER);
        }
        st = apr_procattr_user_set(attr,DEFAULT_USER,NULL);
      }
      if(st == APR_SUCCESS) {
        if(c->group) {
#ifdef ZKJOBD_PROHIBIT_ROOT_JOBS
          if(strcasecmp(c->group,"root") == 0) {
            ERR("attempt to run job '%s' as root group!",logcmd);
            st = APR_EACCES;
            goto zkproc_create_error;
          }
#endif
          st = apr_procattr_group_set(attr,c->group);
        } else
          apr_procattr_group_set(attr,DEFAULT_GROUP);
      }
      if(st != APR_SUCCESS)
        goto zkproc_create_error;
    }
    apr_procattr_error_check_set(attr,1);
    apr_procattr_child_errfn_set(attr,child_error);
    if(fixups && c->env) {
      const char *possible = apr_table_get(c->env,"SHELL");
      if(possible && apr_hash_get(valid_shells,possible,APR_HASH_KEY_STRING)) {
        if(c->args)
          apr_array_clear(c->args);
        else
          c->args = apr_array_make(p,4,sizeof(const char*));
        APR_ARRAY_PUSH(c->args,const char*) = possible;
        APR_ARRAY_PUSH(c->args,const char*) = "-c";
        APR_ARRAY_PUSH(c->args,const char*) = c->cmd;
        APR_ARRAY_PUSH(c->args,const char*) = NULL;
        c->cmd = possible;
        c->flags &= (ZKPROCF_USE_SHELL|ZKPROCF_CUSTOM_PATH);
      }
    }
    if(c->env) {
      if(c->flags & ZKPROCF_USE_SHELL)  {
        st = apr_procattr_cmdtype_set(attr,APR_SHELLCMD);
      } else if(apr_table_get(c->env,"PATH") && (c->flags & ZKPROCF_CUSTOM_PATH)) {
        st = apr_procattr_cmdtype_set(attr,APR_PROGRAM_PATH);
      } else {
        st = apr_procattr_cmdtype_set(attr,APR_PROGRAM);
      }
      if(st != APR_SUCCESS)
        goto zkproc_create_error;
    } else if (c->flags & ZKPROCF_USE_SHELL)
      st = apr_procattr_cmdtype_set(attr,APR_SHELLCMD_ENV);
    else
      st = apr_procattr_cmdtype_set(attr,APR_PROGRAM_ENV);
    c->procattr = attr;
  }

zkproc_create_error:
  if(!ZEKE_STATUS_IS_OKAY(st))
    apr_pool_destroy(p);
  else {
    AAOK(apr_pool_userdata_set(c,ZKPROC_POOL_KEY,NULL,p));
    ZK_CHECK_OBJ_NOTNULL(job->ws, ZK_WS_MAGIC);
    if(logcmd)
      AAOK(apr_pool_userdata_set(logcmd,ZKPROC_POOL_PREFIX "LOG",NULL,p));
    apr_pool_pre_cleanup_register(job->pool,zeke_indirect_make(c,c->pool),child_cleanup);
    *cp = c;
  }
  return st;
}

ZEKE_HIDDEN
zk_status_t zkproc_destroy(zkproc_t *c)
{
  zkproc_assert(c);
  zkjob_ref_t *ref;

  if((c->flags & ZKPROCF_REGISTERED))
    apr_proc_other_child_unregister(c);
  ref = c->job;
  if(ref)
    zkqueue_job_ref_destroy(ref);
  return ZEOK;
}

ZEKE_HIDDEN
zkjob_t *zkproc_job_get(const zkproc_t *c)
{
  zkproc_assert(c);
  zkjob_assert(c->job);

  return zkqueue_job_ref_get(c->job);
}

ZEKE_HIDDEN
apr_int64_t zkproc_seq_get(const zkproc_t *c)
{
  zkjob_t *j;
  zkproc_assert(c);
  zkjob_assert(c->job);
  j = zkqueue_job_ref_get(c->job);
  if(!j || c->seq != j->seq)
    return APR_INT64_C(-1);
  return c->seq;
}

ZEKE_HIDDEN
const apr_proc_t *zkproc_proc_get(const zkproc_t *c)
{
  zkproc_assert(c);
  return c->proc;
}

ZEKE_HIDDEN
const apr_procattr_t *zkproc_procattr_get(const zkproc_t *c)
{
  zkproc_assert(c);
  return c->procattr;
}

ZEKE_HIDDEN
apr_uint32_t zkproc_flags_get(const zkproc_t *c)
{
  zkproc_assert(c);
  return c->flags;
}

ZEKE_HIDDEN
apr_pool_t *zkproc_pool_get(const zkproc_t *c)
{
  zkproc_assert(c);
  return c->pool;
}

ZEKE_HIDDEN
zk_status_t zkproc_user_set(zkproc_t *c, const char *user)
{
  zkproc_assert(c);
  if((c->flags & ZKPROCF_RUNNING) ||
     (c->flags & ZKPROCF_DEAD) || !user || !*user)
    return APR_ENOPROC;
  if(!apr_isalnum(*user))
    return APR_EINVAL;
  if(geteuid() != 0)
    return APR_EACCES;
  c->user = zk_workspace_strdup(c->ws,user);
  return apr_procattr_user_set(c->procattr,c->user,NULL);
}

ZEKE_HIDDEN
zk_status_t zkproc_group_set(zkproc_t *c, const char *group)
{
  zkproc_assert(c);
  if((c->flags & ZKPROCF_RUNNING) ||
     (c->flags & ZKPROCF_DEAD) || !group || !*group)
    return APR_ENOPROC;
  if(!apr_isalnum(*group))
    return APR_EINVAL;
  if(geteuid() != 0)
    return APR_EACCES;
  c->group = zk_workspace_strdup(c->ws,group);
  return apr_procattr_group_set(c->procattr,c->group);
}

ZEKE_HIDDEN
zk_status_t zkproc_env_set(zkproc_t *c, const char *var, const char *val)
{
  zkproc_assert(c);
  if(!var)
    return APR_EINVAL;
  if(!c->env)
    c->env = apr_table_make(c->pool,1);
  if(val)
    apr_table_set(c->env,var,val);
  else
    apr_table_unset(c->env,var);
  return ZEOK;
}

ZEKE_HIDDEN
zk_status_t zkproc_pid_get(zkproc_t *c, apr_os_proc_t *pidp)
{
  if(!pidp)
    return APR_EINVAL;
  zkproc_assert(c);
  if(!c->proc || (c->flags & (ZKPROCF_RUNNING|ZKPROCF_DEAD)) == 0)
    return APR_ENOENT;
  *pidp = c->proc->pid;
  return APR_SUCCESS;
}

static
int zk_env_join(void *wsv, const char *key, const char *val)
{
  struct zkws *ws = (struct zkws*)wsv;
  const char *v;
  AN(key); AN(val);
  while(apr_isspace(*key)) key++;
  while(apr_isspace(*val)) val++;
  if(*key && *val) {
    v = zk_workspace_strcat(ws,key,"=",val,NULL);
    AN(v); AN(ws->data);
    APR_ARRAY_PUSH((apr_array_header_t*)ws->data, const char*) = v;
  }
  return 1;
}

ZEKE_HIDDEN
zk_status_t zkproc_register(zkproc_t *c)
{
  zkproc_assert(c);
  if(c->flags & (ZKPROCF_REGISTERED|ZKPROCF_PIPELINE))
    return APR_EINVAL;
  c->flags |= ZKPROCF_REGISTERED;
  apr_proc_other_child_register(c->proc,zk_proc_maintenance,c,NULL,c->pool);
  INFO("REGISTER: pid=%d vxid=" VXID_FMT, c->proc->pid, VXID_C(c->vxid));
  if((c->flags & ZKPROCF_PIPELINE) == 0) {
    zkproc_t *i;
    ZKPROC_RING_FOREACH(i,&procs) {
      if(i != c && i->vxid == c->vxid &&
         (i->flags & ZKPROCF_RUNNING) && (i->flags & (ZKPROCF_DEAD|ZKPROCF_REGISTERED)) == 0 &&
          i->proc) {
        assert((i->flags & ZKPROCF_PIPELINE) != 0);
        i->flags |= ZKPROCF_REGISTERED;
        INFO("REGISTER/CHILD: pid=%d vxid=" VXID_FMT, i->proc->pid, VXID_C(i->vxid));
        apr_proc_other_child_register(i->proc,zk_proc_maintenance,i,NULL,i->pool);
      }
    }
  }
  return ZEOK;
}

static
void close_pipelines(apr_uint32_t vxid, zkproc_t *c)
{
  zkproc_t *p;

  assert(vxid != 0);
  ZKPROC_RING_FOREACH(p,&procs) {
    if(p->vxid == vxid && (c == NULL || c == p)) {
      unsigned short i;
      apr_file_t *pipe;
      for(i = 0; i < ZKPROC_NUM_PIPESETS; i++)
        if(p->pipesets[i]) {

          ZK_CHECK_OBJ_NOTNULL(p->pipesets[i], ZKPROC_PIPESET_MAGIC);
          pipe = p->pipesets[i]->pipes[zkproc_pipeset_read];
          if(pipe)
            apr_file_close(pipe);
          pipe = p->pipesets[i]->pipes[zkproc_pipeset_write];
          if(pipe)
            apr_file_close(pipe);
          p->pipesets[i]->pipes[zkproc_pipeset_read] = NULL;
          p->pipesets[i]->pipes[zkproc_pipeset_write] = NULL;
        }
    }
  }
}

ZEKE_HIDDEN
unsigned int zkproc_close_pipeline(zkproc_t *c, zkproc_t *parent)
{
  unsigned count = 0;
  zkproc_assert(c);

  close_pipelines(c->vxid, (parent ? c : NULL));
  count++;
  if(parent && !IS_SENTINEL(parent)) {
    close_pipelines(parent->vxid, parent);
    count++;
  }
  return count;
}

ZEKE_HIDDEN
zk_status_t zkproc_start_pipeline(zkproc_t *c, zkproc_t *parent)
{
  zk_status_t st;
  zkproc_t *top;
  const char * const*env = NULL;

  zkproc_assert(c);
  if((c->flags & ZKPROCF_RUNNING) || (c->flags & ZKPROCF_DEAD))
    return APR_ENOPROC;

  if((c->flags & ZKPROCF_PIPELINE) == 0)
    return APR_EINVAL;

  zkproc_assert(parent);
  assert(c->seq == parent->seq);
  for(top = parent; (top->flags & ZKPROCF_PIPELINE) == 0; ) {
    zkproc_t *i;
    ZKPROC_RING_FOREACH(i, &procs) {
      if(i->vxid == parent->vxid && (i->flags & ZKPROCF_PIPELINE) == 0) {
        top = i;
        break;
      }
    }
    assert((top->flags & ZKPROCF_PIPELINE) == 0 && top->seq == c->seq);
    break;
  }
  c->ws->data = NULL;
  if(c->env) {
    c->ws->data = apr_array_make(c->pool,apr_table_elts(c->env)->nelts+1,
                                                    sizeof(const char*));
    apr_table_do(zk_env_join,c->ws,c->env,NULL);
    if(c->ws->data) {
      APR_ARRAY_PUSH((apr_array_header_t*)c->ws->data,const char*) = NULL;
      env = (const char **)((apr_array_header_t*)c->ws->data)->elts;
    }
  }
  c->flags |= ZKPROCF_RUNNING;
  c->start = apr_time_now();
  AZ(c->proc);
  c->proc = apr_pcalloc(c->pool,sizeof(*c->proc));
  AN(c->args);
  AN(c->procattr);
  AN(c->cmd);
#ifdef DEBUGGING
  INFO("pipeline exec: %s",c->cmd);
  do {
    const char * const*argv = (const char**)c->args->elts;
    int i;

    for(i = 0; *argv; argv++, i++)
      INFO("PIPELINE ARG %d: %s",i,*argv);
    for(i = 0, argv=env; argv && *argv; argv++, i++)
      INFO("PIPELINE ENV#%d %s",i,*argv);
  } while(0);
#endif
  st = apr_proc_create(c->proc,c->cmd,(const char **)c->args->elts,env,c->procattr,c->pool);
  if(st == APR_SUCCESS) {
    AN(c->proc);
    ZKPROC_RING_INSERT_TAIL(&procs,c);
    ZKPROC_RING_CHECK_CONSISTENCY(&procs);
    /* Note below is safe because we only destroy this pool as a last resort */
    if(parent)
      apr_pool_note_subprocess(c->pool,c->proc,APR_KILL_ONLY_ONCE);
    if(c->job) {
      zkjob_t *j = zkqueue_job_ref_get(c->job);
      if(j) {
        if(j->vxid)
          assert(j->vxid == c->vxid);
        else
          j->vxid = c->vxid;
      }
    }
  } else {
    zeke_apr_eprintf(st,"Cannot start job #%" APR_UINT64_T_FMT " (pipelined %s)",
                       c->seq,c->cmd);
    c->start = APR_TIME_C(0);
    c->flags &= ~ZKPROCF_RUNNING;
    c->flags |= ZKPROCF_DEAD;
  }

  return st;
}

ZEKE_HIDDEN
zk_status_t zkproc_start(zkproc_t *c, apr_file_t **pipep, apr_interval_time_t timeout)
{
  const char *const *env = NULL;

  zk_status_t st;
  zkproc_assert(c);
  if((c->flags & ZKPROCF_RUNNING) || (c->flags & ZKPROCF_DEAD))
    return APR_ENOPROC;

  if(timeout > APR_TIME_C(0))
    c->timeout = timeout;
  else
    c->timeout = APR_TIME_C(-1);
  c->ws->data = NULL;
  if(c->env) {
    c->ws->data = apr_array_make(c->pool,apr_table_elts(c->env)->nelts+1,
                                                    sizeof(const char*));
    apr_table_do(zk_env_join,c->ws,c->env,NULL);
    if(c->ws->data) {
      APR_ARRAY_PUSH((apr_array_header_t*)c->ws->data,const char*) = NULL;
      env = (const char **)((apr_array_header_t*)c->ws->data)->elts;
    }
  }
  c->flags |= ZKPROCF_RUNNING;
  c->start = apr_time_now();
  AZ(c->proc);
  c->proc = apr_pcalloc(c->pool,sizeof(*c->proc));
  AN(c->proc);
  AN(c->args);
  AN(c->procattr);
  AN(c->cmd);
#ifdef DEBUGGING
#if 0
  INFO("exec: %s",c->cmd);
  do {
    const char * const*argv = (const char**)c->args->elts;
    int i;

    for(i = 0; *argv; argv++, i++)
      INFO("ARG %d: %s",i,*argv);
    for(i = 0, argv=env; argv && *argv; argv++, i++)
      INFO("ENV#%d %s",i,*argv);
  } while(0);
#endif
#endif
  st = apr_proc_create(c->proc,c->cmd,(const char **)c->args->elts,env,c->procattr,c->pool);
  if(st == APR_SUCCESS) {
    char *logcmd = NULL;
    AN(c->proc);
    ZKPROC_RING_INSERT_TAIL(&procs,c);
    ZKPROC_RING_CHECK_CONSISTENCY(&procs);
    /* Note below is safe because we only destroy this pool as a last resort */
    if(c->job) {
      zkjob_t *j = zkqueue_job_ref_get(c->job);
      if(j)
        apr_pool_note_subprocess(j->ws->p,c->proc,APR_KILL_ALWAYS);
    }
    *pipep = c->proc->in;
    if(pipep && *pipep && !(c->flags & ZKPROCF_STDIN_PIPE)) {
      apr_file_close(*pipep);
      *pipep = NULL;
    } else if(c->proc->in)
      apr_file_close(c->proc->in);
    if(!(c->flags & ZKPROCF_PIPELINE) &&
         apr_pool_userdata_get((void**)&logcmd,ZKPROC_POOL_PREFIX "LOG",c->pool) == APR_SUCCESS &&
                                                               logcmd != NULL) {
      zeke_log_pid((c->user ? c->user : "-"),(int)c->proc->pid,"JOB: %s",logcmd);
    }
    if((c->flags & ZKPROCF_PIPELINE) == 0) {
      zkjob_t *j;
      zkjob_assert(c->job);
      j = zkqueue_job_ref_get(c->job);
      AN(j);
      j->vxid = c->vxid;
    }
  } else {
    if(c->flags & ZKPROCF_PIPELINE)
      zeke_apr_eprintf(st,"Cannot start pipeline process '%s'",c->cmd);
    else
      zeke_apr_eprintf(st,"Cannot start job #%" APR_UINT64_T_FMT " (%s)",
                       c->seq,c->cmd);
    c->start = APR_TIME_C(0);
    c->flags &= ~ZKPROCF_RUNNING;
    c->flags |= ZKPROCF_DEAD;
    if(c->job) {
      zkqueue_job_ref_destroy(c->job);
      c->job = NULL;
    }
  }
  return st;
}

static
int zkproc_interrupt_handler(zeke_connection_t *conn)
{
  static apr_pool_t *p = NULL;
  apr_status_t st;
  apr_exit_why_e why;
  apr_proc_t proc;
  int code;

  if(!initialized) zkproc_ring_tryinit();
  AN(maintenance_pool);
  if(!p)
    apr_pool_create(&p,maintenance_pool);

  INFO("zkproc_interrupt_handler");
  for(st = APR_CHILD_DONE; st == APR_CHILD_DONE; ) {
    if((st = apr_proc_wait_all_procs(&proc,&code,&why,APR_NOWAIT,p)) == APR_CHILD_DONE) {
      LOG("apr_proc_wait_all_procs: %d %d",proc.pid,(int)why);
      apr_proc_other_child_alert(&proc, APR_OC_REASON_DEATH, ZKTOOL_MKSTATUS_OC(code,why));
    }
  }
  return 0;
}

ZEKE_HIDDEN
int zkproc_get_total_job_count(void)
{
  int count = 0;
  zkproc_t *c;
  if(!initialized) zkproc_ring_tryinit();

  ZKPROC_RING_FOREACH(c,&procs) {
    if((c->flags & ZKPROCF_RUNNING) &&
       (c->flags & ZKPROCF_PIPELINE) == 0 && c->proc)
      count++;
  }

  return count;
}

ZEKE_HIDDEN
zk_status_t zkproc_signal(zkproc_t *c, int sig)
{
  zkproc_assert(c);
  if(c->proc)
    return apr_proc_kill(c->proc,sig);
  return APR_EINVAL;
}

ZEKE_HIDDEN
zk_status_t zkproc_signal_all(int sig)
{
  apr_status_t st = APR_SUCCESS;
  zkproc_t *c;
  int killed = 0;
  if(!initialized) zkproc_ring_tryinit();

  ZKPROC_RING_FOREACH(c,&procs) {
    if(c->proc && (c->flags & ZKPROCF_PIPELINE) == 0) {
      apr_status_t t = apr_proc_kill(c->proc,sig);
      if(st == APR_SUCCESS) {
        killed++;
        st = t;
      }
    }
  }

  if(killed == 0 && st == APR_SUCCESS) {
    ZKPROC_RING_FOREACH(c,&procs) {
      if(c->proc && (c->flags & ZKPROCF_PIPELINE) != 0) {
        apr_status_t t = apr_proc_kill(c->proc,sig);
        if(st == APR_SUCCESS)
          st = t;
      }
    }
  }
  return st;
}

ZEKE_HIDDEN
unsigned int zkproc_get_references(apr_uint32_t vxid, int alive)
{
  unsigned int refs = 0;
  zkproc_t *c;
  ZKPROC_RING_FOREACH(c,&procs) {
    if(c->vxid == vxid) {
      if(alive > 0 && ((c->flags & ZKPROCF_DEAD) == 0 ||
                   (c->flags & (ZKPROCF_RUNNING|ZKPROCF_REGISTERED))))
        refs++;
      else if(alive == 0 && (c->flags|ZKPROCF_DEAD|ZKPROCF_START_ERROR) == 0)
        refs++;
      else if(alive < 0)
        refs++;
    }
  }

  return refs;
}

ZEKE_HIDDEN
void zkproc_maintenance(apr_pool_t *p, int *njobs, int *nalive,
                                       int *nkilled, int *nreaped)
{
  int alive = 0;
  int killed = 0;
  int reaped = 0;
  int total = 0;
  int valid = 0;
  apr_time_t now;
  zkproc_t *c,*state;
  AN(p);

  if(!initialized) zkproc_ring_tryinit();

  now = apr_time_now();

  ZKPROC_RING_FOREACH_SAFE(c,state,&procs) {
    total++;
    if((c->flags & ZKPROCF_RUNNING) && c->proc) {
      alive++;
      if((c->flags & ZKPROCF_PIPELINE) == 0)
        valid++;
      if(c->timeout > APR_TIME_C(-1)) {
        apr_interval_time_t elapsed = now - c->start;
        if(c->termcnt < 3 && elapsed >= c->timeout + (apr_time_from_sec(c->termcnt))) {
#ifdef DEBUGGING
          ERR("KILL elapsed=%lld timeout=%lld termcnt=%d",
              (long long)elapsed,(long long)c->timeout,(int)c->termcnt);
#endif
          apr_proc_kill(c->proc,SIGTERM);
          c->termcnt++;
          killed++;
        } else if(c->termcnt >= 3) {
          zkproc_t *i,*s2;
          ZKPROC_RING_FOREACH_SAFE(i,s2,&procs)
            if(i->vxid == c->vxid && i != c) {
              i->flags &= ~ZKPROCF_RUNNING;
              i->flags |= ZKPROCF_DEAD;
              if(i->flags & ZKPROCF_REGISTERED) {
                apr_proc_other_child_unregister(i);
                killed++;
              }
              ZKPROC_RING_REMOVE(i);
            }
          killed++;
          zkproc_destroy(c);
        }
      }
    }
    if(c->flags & ZKPROCF_DEAD && zkproc_get_references(c->vxid,1) == 0) {
      ZKPROC_RING_REMOVE(c);
      zkproc_destroy(c);
      reaped++;
    }
  }
#ifdef DEBUGGING
  LOG("maint, total=%d, njobs=%u, nkilled=%u, nreaped=%u", total,
       (unsigned)alive,(unsigned)killed,(unsigned)reaped);
#endif
  if(njobs) *njobs = valid;
  if(nalive) *nalive = alive;
  if(nkilled) *nkilled = killed;
  if(nreaped) *nreaped = reaped;
}
