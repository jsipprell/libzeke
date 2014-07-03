#ifndef _ZEKE_ZKJOBS_H
#define _ZEKE_ZKJOBS_H

#include "libzeke.h"
#include "libzeke_event.h"
#include "libzeke_event_loop.h"
#include "libzeke_nodedir.h"
#include "libzeke_errno.h"
#include "libzeke_regex.h"
#include "libzeke_trans.h"
#include "libzeke_util.h"

#include "tools.h"

#ifndef ZKALIGN
#define ZKALIGN(p) APR_ALIGN_DEFAULT((p))
#endif /* ZKALIGN */

#define RETURN do { zeke_callback_status_set(cbd,ZEOK); \
                 return ZEKE_CALLBACK_IGNORE; } while(0)
#define DESTROY do { zeke_callback_status_set(cbd,ZEOK); \
                 return ZEKE_CALLBACK_DESTROY; } while(0)
#define DECLARE_CONTEXT(type) \
  static inline type *cast_context_to_##type ( zeke_context_t *ctx ) { \
                return ( type * )ctx; \
  } \
  static inline type *cbd_to_##type ( const zeke_callback_data_t *cbd ) { \
                return ( type * )zeke_session_context(cbd->connection, \
                                                      (char*)cbd->ctx); \
  } \
  static inline type *cbwd_to_##type ( const zeke_watcher_cb_data_t *cbwd ) { \
                return ( type * )zeke_session_context(cbwd->connection, \
                                                      (char*)cbwd->ctx); \
  }

#define CONTEXT_ARG zeke_context_t *_ctx
#define CONTEXT_VAR(type,name) type name = (type)((zeke_context_t*)_ctx)

#ifndef AZOK
#define AZOK(st) do { \
  assert( ZEKE_STATUS_IS_OKAY( (st) )); \
} while(0)
#endif
#ifndef AN
#define AN(cond) do { \
  assert( (void*)(cond) != (void*)0 ); \
} while(0)
#endif
#ifndef AZ
#define AZ(cond) do { \
  assert( (void*)(cond) == (void*)0 ); \
} while(0)
#endif
#ifndef AAOK
#define AAOK(st) do { \
  assert( (st) == APR_SUCCESS ); \
} while(0)
#endif

#define ZK_CHECK_OBJ_NOTNULL(o,m) do { \
  assert((void*)(o) != (void*)0); \
  assert(!(m) || ((o)->magic == (m))); \
} while(0)

#ifndef ZKE_TO_ERROR
#define ZKE_TO_ERROR(s) ((s) & 0xff ? (s) & 0xff : ((s) >> 8) | 1)
#endif

typedef struct zkws {
  apr_uint32_t magic;
#define ZK_WS_MAGIC 0x098feb0a
  char *f,*r,*b,*e;
  apr_pool_t *p;
  void *data;
} zkworkspace_t;

/* workspaces */
ZKTOOL_DECL(struct zkws*) zk_workspace_new_ex(apr_pool_t *parent, const char *name);
ZKTOOL_DECL(struct zkws*) zk_workspace_new(apr_pool_t *parent);
ZKTOOL_DECL_VA(struct zkws*) zk_subworkspace_new(struct zkws*,...);
#define ZK_SUBWORKSPACE(ws) zk_subworkspace_new((ws),NULL)
ZKTOOL_DECL(void) zk_workspace_free(struct zkws**);
ZKTOOL_DECL(void) zk_workspace_clear(struct zkws**);
ZKTOOL_DECL(apr_off_t) zk_workspace_reserve(struct zkws*, apr_size_t);
ZKTOOL_DECL(void*) zk_workspace_reservep(struct zkws*, apr_size_t);
ZKTOOL_DECL(char*) zk_workspace_release(struct zkws*, apr_size_t);
ZKTOOL_DECL(char*) zk_workspace_releasep(struct zkws*, void*);
ZKTOOL_DECL(apr_off_t) zk_workspace_snapshot(struct zkws*);
ZKTOOL_DECL(void) zk_workspace_restore(struct zkws*,apr_off_t);
ZKTOOL_DECL(void) zk_workspace_reset(struct zkws*);
ZKTOOL_DECL(void*) zk_workspace_alloc(struct zkws*,apr_size_t);
ZKTOOL_DECL(char*) zk_workspace_strdup(struct zkws*,const char*);
ZKTOOL_DECL(char*) zk_workspace_strndup(struct zkws*,const char*,apr_size_t);
ZKTOOL_DECL(char*) zk_workspace_strcatv(struct zkws*,const char*,va_list);
ZKTOOL_DECL_VA(char*) zk_workspace_strcat(struct zkws*,const char*,...);
ZKTOOL_DECL(char*) zk_workspace_nodecatv(struct zkws*,va_list);
ZKTOOL_DECL_VA(char*) zk_workspace_nodecat(struct zkws*,...);
ZKTOOL_DECL(char*) zk_workspace_vprintf(struct zkws*, const char *fmt, va_list ap);
ZKTOOL_DECL_PFMT(char*,2,3) zk_workspace_printf(struct zkws*, const char *fmt, ...);

/* child procs */
enum zkprocflags {
  zkprocf_running = (1 << 0),
#define ZKPROCF_RUNNING zkprocf_running
  zkprocf_dead = (1 << 1),
#define ZKPROCF_DEAD zkprocf_dead
  zkprocf_signaled = (1 << 2),
#define ZKPROCF_SIGNALED zkprocf_signaled
  zkprocf_registered = (1 << 3),
#define ZKPROCF_REGISTERED zkprocf_registered
  zkprocf_start_error = (1 << 4),
#define ZKPROCF_START_ERROR zkprocf_start_error
  zkprocf_error_output = (1 << 5),
#define ZKPROCF_ERROR_OUTPUT zkprocf_error_output
  zkprocf_use_shell = (1 << 6),
#define ZKPROCF_USE_SHELL zkprocf_use_shell
  zkprocf_custom_path = (1 << 7),
#define ZKPROCF_CUSTOM_PATH zkprocf_custom_path
  zkprocf_stdin_pipe = (1 << 8),
#define ZKPROCF_STDIN_PIPE zkprocf_stdin_pipe
  zkprocf_no_proctitle = (1 << 9),
#define ZKPROCF_NO_PROCTITLE zkprocf_no_proctitle
  zkprocf_allow_path_override = (1 << 10),
#define ZKPROCF_ALLOW_PATH_OVERRIDE zkprocf_allow_path_override
  zkprocf_temp_file = (1 << 11),
#define ZKPROCF_TEMP_FILE zkprocf_temp_file
  zkprocf_custom_env = (1 << 12),
#define ZKPROCF_CUSTOM_ENV zkprocf_custom_env
  zkprocf_pipeline = (1 << 13),
#define ZKPROCF_PIPELINE zkprocf_pipeline
};


typedef enum {
  zkproc_pipeset_read = 0,
  zkproc_pipeset_write = 1,
} zkproc_pipeset_type;

typedef struct zkproc zkproc_t;
typedef struct zkproc_pipeset zkproc_pipeset_t;

#define ZKPROC_PIPESET_STDIN 0
#define ZKPROC_PIPESET_STDOUT 1
#define ZKPROC_PIPESET_STDERR 2

#define ZKPROC_PIPESET_OPT_MAPPING          0x000f
#define ZKPROC_PIPESET_OPT_STDOUT_TO_STDERR 0x0002
#define ZKPROC_PIPESET_OPT_STDERR_TO_STDOUT 0x0006
#define ZKPROC_PIPESET_OPT_NO_STDOUT 0x0010
#define ZKPROC_PIPESET_OPT_NO_STDERR 0x0020

/* elections */
typedef struct zkelection zkelection_t;
enum zkelec {
  zkelec_win = 1,
#define ZKELEC_WIN zkelec_win
  zkelec_change,
#define ZKELEC_CHANGE zkelec_change
  zkelec_inval
#define ZKELEC_INVAL zkelec_inval
};

enum zkelecstate {
  zkelec_state_init = 0,
#define ZKELEC_STATE_INIT zkelec_state_init
  zkelec_state_starting = (1 << 0),
#define ZKELEC_STATE_STARTING zkelec_state_starting
  zkelec_state_have_nodedir = (1 << 1),
#define ZKELEC_STATE_HAVE_NODEDIR zkelec_state_have_nodedir
  zkelec_state_registered = (1 << 2),
#define ZKELEC_STATE_REGISTERED zkelec_state_registered
  zkelec_state_winner = (1 << 3),
#define ZKELEC_STATE_WINNER zkelec_state_winner
  zkelec_state_dirty = (1 << 4),
#define ZKELEC_STATE_DIRTY zkelec_state_dirty
  zkelec_state_wait_nodedir = (1 << 5),
#define ZKELEC_STATE_WAIT_NODEDIR zkelec_state_wait_nodedir
  zkelec_state_waiting = (1 << 6),
#define ZKELEC_STATE_WAITING zkelec_state_waiting
  zkelec_state_refresh_in_progress = (1 << 7),
#define ZKELEC_STATE_REFRESH_IN_PROGRESS zkelec_state_refresh_in_progress
  zkelec_state_relinquish = (1 << 8),
#define ZKELEC_STATE_RELINQUISH zkelec_state_relinquish
  zkelec_state_restart = (1 << 9),
#define ZKELEC_STATE_RESTART zkelec_state_restart
};

typedef zeke_cb_t (*zkelection_callback_fn)(zkelection_t*, enum zkelec what, void*);

/* queues */
typedef struct zkqueue zkqueue_t;
typedef struct zkjob_ref zkjob_ref_t;
typedef struct zkjob_priv zkjob_priv_t;

typedef struct {
  apr_uint32_t magic;
#define ZKJOB_MAGIC 0x4be0455b
  apr_pool_t *pool;
  apr_uint32_t vxid;
  struct zkws *ws;
  apr_uint64_t seq;
  apr_table_t *headers;
  const unsigned char *content;
  apr_size_t len;
  zkjob_priv_t *priv;
} zkjob_t;

enum zkq_event {
  zkq_ev_indexed,
  zkq_ev_job_new,
  zkq_ev_job_loaded,
  zkq_ev_job_error,
};

#define VXID_FMT "0x%08" APR_UINT64_T_HEX_FMT
#define VXID_C(v) ((apr_uint64_t)v & APR_UINT64_C(0xffffffff))
#define VXID_Next() zktool_vxid_get(vxid_pool)

typedef zeke_cb_t (*zkqueue_callback_fn)(zkqueue_t*, enum zkq_event,
                                          zkjob_t*, zeke_context_t*);

ZKTOOL_DECL(void *) zkproc_sentinel;
#define SENTINEL ((void*)zkproc_sentinel)
#define IS_SENTINEL(p) (SENTINEL == ((const void*)(p)))

/* flag helper module prototypes */
ZKTOOL_DECL(void) zkjobq_flags_print(int use_stderr);
ZKTOOL_DECL(zk_status_t) zkjobq_flags_parse(const char *flagstr, apr_uint32_t *flags);
ZKTOOL_DECL(const char*) zkjobq_flags_unparse(apr_uint32_t flags, struct zkws*);

/* election prototypes */
ZKTOOL_DECL(zk_status_t) zkelection_create(zkelection_t**,
                                           const char *root,
                                           const char *ident,
                                           zkelection_callback_fn update_fn,
                                           const zeke_connection_t*,
                                           apr_pool_t *pool);
ZKTOOL_DECL(apr_pool_t*) zkelection_pool_get(const zkelection_t*);
ZKTOOL_DECL(zk_status_t) zkelection_cancel(zkelection_t*);
ZKTOOL_DECL(zk_status_t) zkelection_relinquish_leader(zkelection_t*);
ZKTOOL_DECL(void) zkelection_context_set(zkelection_t*,void*);
ZKTOOL_DECL(void*) zkelection_context_get(zkelection_t*);
ZKTOOL_DECL(zk_status_t) zkelection_start(zkelection_t*,
                                          apr_interval_time_t *timeout);
ZKTOOL_DECL(zk_status_t) zkelection_restart(zkelection_t*);
ZKTOOL_DECL(struct zkws*) zkelection_workspace(const zkelection_t*);
ZKTOOL_DECL(const apr_array_header_t*) zkelection_members(const zkelection_t*);
ZKTOOL_DECL(apr_uint32_t) zkelection_state(const zkelection_t*);
ZKTOOL_DECL(const char*) zkelection_path(const zkelection_t*);
ZKTOOL_DECL(const char*) zkelection_winner(const zkelection_t*);
ZKTOOL_DECL(zk_status_t) zkelection_name_get(const zkelection_t*,
                                             const char **name,
                                             apr_size_t *namelen);
ZKTOOL_DECL(int) zkelection_member_count(const zkelection_t*);
ZKTOOL_DECL(zk_status_t) zkelection_refresh(zkelection_t*);

/* queue prototypes */
ZKTOOL_DECL(apr_pool_t*) zkqueue_pool_get(const zkqueue_t*);
ZKTOOL_DECL(zk_status_t) zkqueue_create(zkqueue_t**,const char *path,
                                        zkqueue_callback_fn default_cb,
                                        const zeke_connection_t*,
                                        apr_pool_t*);
ZKTOOL_DECL(void) zkqueue_context_set(zkqueue_t*,zeke_context_t*);
ZKTOOL_DECL(zeke_context_t*) zkqueue_context_get(zkqueue_t*);
ZKTOOL_DECL(zk_status_t) zkqueue_start(zkqueue_t*,zkqueue_callback_fn update_fn,
                                       apr_interval_time_t *timeout);
ZKTOOL_DECL(zk_status_t) zkqueue_clear(zkqueue_t*);
#define zkqueue_stop zkqueue_clear
ZKTOOL_DECL(zk_status_t) zkqueue_job_delete(zkjob_t*);
ZKTOOL_DECL(void) zkqueue_job_release(zkjob_t**);
ZKTOOL_DECL(zk_status_t) zkqueue_job_lock(zkjob_t*);
ZKTOOL_DECL(zk_status_t) zkqueue_job_unlock(zkjob_t*);
/* notes: zkqueue_is_job_locked() return vals: 1 = locked, 0 = unlocked, -1 = error */
ZKTOOL_DECL(int) zkqueue_is_job_locked(zkjob_t*);
/* zkqueue_job_load loads a job asyncronously and call the queue update callback with
 * an zkq_ev_job_loaded event once loaded. If this callback returns ZEKE_CALLBACK_DESTROY
 * the job will be scheduled for deletion immediately.
 *
 * If an error occurs while loading the update callback is called with an
 * zkq_ev_job_error event and zkqueue_job_status_get() can retrieve the error.
 */
ZKTOOL_DECL(zk_status_t) zkqueue_job_load(zkjob_t*);
ZKTOOL_DECL(int) zkqueue_job_is_loaded(zkjob_t*);
/* return the first pending job or APR_EINCOMPLETE if there are non pending.
   The job will no longer be pending and locked, belonging to the caller. */
ZKTOOL_DECL(zk_status_t) zkqueue_job_pending_pop(zkqueue_t*,zkjob_t**);
ZKTOOL_DECL(zk_status_t) zkqueue_job_status_get(zkjob_t*,zk_status_t *out_status);
ZKTOOL_DECL(zk_status_t) zkqueue_job_status_clear(zkjob_t*);
ZKTOOL_DECL(const char*) zkqueue_job_node_get(const zkjob_t*);

/* Push a job on the pending queue. The job's reference count will be
 * incremented and the pending queue is considered to "own" it until
 * zkqueue_job_pending_pop() is called.
 */
ZKTOOL_DECL(zk_status_t) zkqueue_job_pending_push(zkqueue_t*,zkjob_t*);
ZKTOOL_DECL(int) zkqueue_job_is_pending(zkjob_t*);

/* job indirect refereces (ref counted to vanish when refs drops to 0 */
ZKTOOL_DECL(zkjob_ref_t*) zkqueue_job_ref_make(zkjob_t*,apr_pool_t*);
ZKTOOL_DECL(zkjob_t*) zkqueue_job_ref_get(const zkjob_ref_t*);
ZKTOOL_DECL(void) zkqueue_job_ref_destroy(zkjob_ref_t*);
ZKTOOL_DECL(int) zkqueue_job_ref_eq(const zkjob_ref_t *a, const zkjob_ref_t *b);

/* child process prototypes */
/* ensure proc.c is initialized, safe to call many times */
#define ZKPROC_COPY_PARENT_ENV SENTINEL

ZKTOOL_DECL(void) zkproc_initialize(void);
ZKTOOL_DECL(void) zkproc_init_default_environment(const char *const*);
ZKTOOL_DECL(zk_status_t) zkproc_destroy(zkproc_t*);
ZKTOOL_DECL(apr_pool_t*) zkproc_pool_get(const zkproc_t*);
ZKTOOL_DECL(zk_status_t) zkproc_create_ex(zkproc_t**,zkjob_t *job,
                                           const char *cmd,
                                           apr_array_header_t *args,
                                           apr_table_t *env,
                                           apr_uint32_t flags,
                                           apr_pool_t*);
#define zkproc_create(cp,j,cmd,args) zkproc_create_ex((cp),(j),(cmd),(args),NULL,\
                                      ZKPROCF_ALLOW_PATH_OVERRIDE,NULL)
#define zkproc_command_create(cp,j) zkproc_create_ex((cp),(j),NULL,NULL,NULL,\
                                      ZKPROCF_USE_SHELL|ZKPROCF_ALLOW_PATH_OVERRIDE,NULL);
ZKTOOL_DECL(zk_status_t) zkproc_create_pipeline(zkproc_t**,zkproc_t *parent,
                                                const char *cmd,
                                                apr_table_t *env,
                                                apr_uint32_t flags);
ZKTOOL_DECL(zk_status_t) zkproc_register(zkproc_t*);
ZKTOOL_DECL(zk_status_t) zkproc_pid_get(zkproc_t*, apr_os_proc_t *pid);
ZKTOOL_DECL(zk_status_t) zkproc_user_set(zkproc_t*,const char *user);
ZKTOOL_DECL(zk_status_t) zkproc_group_set(zkproc_t*,const char *group);
ZKTOOL_DECL(zk_status_t) zkproc_env_set(zkproc_t*,const char *var, const char *val);
ZKTOOL_DECL(zkjob_t*) zkproc_job_get(const zkproc_t*);
ZKTOOL_DECL(apr_int64_t) zkproc_seq_get(const zkproc_t*);
ZKTOOL_DECL(const apr_proc_t*) zkproc_proc_get(const zkproc_t*);
ZKTOOL_DECL(const apr_procattr_t*) zkproc_procattr_get(const zkproc_t*);
ZKTOOL_DECL(apr_uint32_t) zkproc_flags_get(const zkproc_t*);
ZKTOOL_DECL(int) zkproc_is_alive(const zkproc_t*);
ZKTOOL_DECL(zk_status_t) zkproc_status_get(zkproc_t*, int *code, apr_exit_why_e *why);
ZKTOOL_DECL(zk_status_t) zkproc_start(zkproc_t*, apr_file_t **input, apr_interval_time_t max_run_time);
ZKTOOL_DECL(zk_status_t) zkproc_start_pipeline(zkproc_t*, zkproc_t *parent);
ZKTOOL_DECL(unsigned int) zkproc_close_pipeline(zkproc_t*, zkproc_t *parent);
ZKTOOL_DECL(void) zkproc_maintenance(apr_pool_t *p, int *njobs, int *nalive,
                                                    int *nkilled, int *nreaped);
ZKTOOL_DECL(int) zkproc_get_total_job_count(void);
ZKTOOL_DECL(zk_status_t) zkproc_signal(zkproc_t*, int sig);
ZKTOOL_DECL(zk_status_t) zkproc_signal_all(int sig);
ZKTOOL_DECL(int) zkproc_is_valid_shell(const char *shell);

ZKTOOL_DECL(zk_status_t) zkproc_pipeset_create(zkproc_pipeset_t**, unsigned short index, 
                                               apr_pool_t *pool);
ZKTOOL_DECL(zk_status_t) zkproc_pipeset_get(apr_file_t **, zkproc_pipeset_type type,
                                            zkproc_pipeset_t *pipeset);
ZKTOOL_DECL(int) zkproc_pipeset_inuse(const zkproc_pipeset_t *, zkproc_pipeset_type type);
ZKTOOL_DECL(apr_pool_t*) zkproc_pipeset_pool_get(const zkproc_pipeset_t*);
ZKTOOL_DECL(unsigned short) zkproc_pipeset_index_get(const zkproc_pipeset_t*);
ZKTOOL_DECL(zk_status_t) zkproc_pipeset_assign(zkproc_pipeset_t*, zkproc_pipeset_type,
                                               apr_file_t**, apr_uint32_t options,
                                               zkproc_t*);
ZKTOOL_DECL(unsigned int) zkproc_get_references(apr_uint32_t vxid, int alive);
#define ZKPROC_JOB_REFCOUNT(j) ((j)->vxid ? zkproc_get_references((j)->vxid,-1) : 0)
#define ZKPROC_JOB_REFS_ALIVE(j) ((j)->vxid ? zkproc_get_references((j)->vxid, 1) : 0)
#define ZKPROC_JOB_REFS_DEAD(j) ((j)->vxid ? zkproc_get_references((j)->vxid, 0) : 0)

#endif /* _ZEKE_ZKJOBS_H */
