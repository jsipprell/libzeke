#include "libzeke.h"
#include "libzeke_event.h"
#include "libzeke_event_loop.h"
#include "libzeke_tree.h"
#include "libzeke_errno.h"
#include "libzeke_util.h"
#include "libzeke_connection_hooks.h"

#include "tools.h"
#include "io.h"
#include "connection.h"
#include "event_private.h"

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

#define LINKCH_NO_JOIN '|'
#define LINKCH_JOIN_BOTTOM '+'
#define LINKCH_JOIN_ALL '+'
#define LINKCH_JOIN_RIGHT '`'
#define LINKCH_JOIN_NONE '-'
#define LINKCH_JOIN_EMPTY ' '

#ifndef ZKE_TO_ERROR
#define ZKE_TO_ERROR(s) ((s) & 0xff ? (s) & 0xff : ((s) >> 8) | 1)
#endif
enum {
  OPT_DEBUG = 1,
  OPT_NS = 2,
  OPT_MAX_DEPTH = 3,
  OPT_HELP = 'h',
  OPT_LOGFILE = 'L',
  OPT_ZKHOSTS = 'C',
  OPT_RM = 'r',
  OPT_V = 'v',
  OPT_Q = 'q',
  OPT_S = 's',
  OPT_VERBOSE = 0x0100,
  OPT_QUIET = 0x0200,
  OPT_SHORT = 0x0400,
};

#define TIMEOUT_AFTER_MSEC 5000

typedef struct zktree {
  apr_uint32_t magic;
#define ZKTREE_MAGIC 0x94bb1c47
  apr_pool_t *pool;
  const char *node,*path;
  zeke_connection_t *conn;
  zeke_tree_t *tree;
  apr_uint32_t flags;
  apr_array_header_t *fmt;
  apr_time_t start_time;
  apr_interval_time_t timeout_after;
  apr_uint32_t max_depth;
  int mode;
  struct event *timer_ev;
  zeke_tree_iter_t *state;
} zktree_t;

typedef struct {
  apr_uint32_t magic;
#define ZKTREE_NS_MAGIC 0xbeff04a1
  char *name;
  apr_size_t len;
} zktree_ns_t;

static apr_pool_t *zktree_pool = NULL;
static apr_getopt_option_t options[] = {
 { "connect",       OPT_ZKHOSTS,    1,
  "override zookeeper connection string (default is from env $ZOOKEEPER_ENSEMBLE)" },
 { "logfile",       OPT_LOGFILE,    1,
  "set the general logging file (default is stderr)" },
 { NULL,            OPT_RM,         0,
  "recursively delete an entire tree or subtree of zookeeper nodes"},
 { "max-depth",     OPT_MAX_DEPTH,  1,
   "Limit recursion to maximum depth (ignore nodes deeper than indicated)"},
 { "verbose",       OPT_V,          0,
   "Extra verbose output"},
 { "short",         OPT_S,          0,
   "Display short node names instead of full paths"},
 { "quiet",         OPT_Q,          0,
   "Minimize/quiet output"},
 { "debug",         OPT_DEBUG,      0,
  "enable debug mode" },
 { "help",          'h',            0,  "display this help" },
 { NULL,0,0,NULL }
};

static void usage(int exitcode)
{
  if(!exitcode || exitcode >= 100) {
    zeke_printf("zktree [options] %s\n","<zookeeper-node-path>");
    zeke_printf("%s\n","  Display a graph of a zookeeper hierarchy or sub-hierarchy");
    zeke_printf("%s\n","options:");
  }

  zktool_display_options(options,(!exitcode || exitcode >= 100 ? zstdout : zstderr),NULL);

  exit(exitcode);
}

#if 0
static
zktree_ns_t *build_namespaces(const char *path, apr_array_header_t *tmpl,
                              apr_array_header_t **arrp,
                              zktree_t *zk,
                              apr_pool_t *p)
{
  zktree_ns_t *ent;
  apr_size_t i = 0;
  char *state, *tok;
  apr_array_header_t *ns;
  char buf[1024];

  if(!p)
    p = zk->pool;

  if(arrp && *arrp) {
    ns = *arrp;
    apr_array_clear(ns);
  } else {
    ns = apr_array_make(p,10,sizeof(zktree_ns_t));
    if(arrp)
      *arrp = ns;
  }
  if(*path == '/') path++;
  apr_cpystrn(buf,path,sizeof(buf));
  for(tok = apr_strtok(buf,"/",&state); tok != NULL; tok = apr_strtok(NULL,"/",&state), i++) {
    apr_size_t l = strlen(tok);
    if (l > 15) l = 15;
    ent = &APR_ARRAY_PUSH(ns,zktree_ns_t);
    memset(ent,0,sizeof(*ent));
    ent->magic = ZKTREE_NS_MAGIC;
    if(tmpl) {
      zktree_ns_t *t;
      assert(i < tmpl->nelts);
      t = &APR_ARRAY_IDX(tmpl,i,zktree_ns_t);
      ZEKE_MAGIC_CHECK(t,ZKTREE_NS_MAGIC);
      printf("name=%s len=%d\n",t->name,(int)t->len);
      printf("     %s/%d\n",tok,(int)l);
      ent->name = apr_pcalloc(p,(l > t->len ? l+100 : t->len+100));
      apr_snprintf(ent->name,t->len,t->name,tok);
      ent->len = strlen(ent->name);
    } else {
      ent->name = apr_psprintf(p,"%%%us",(unsigned int)l);
      ent->len = l;
      printf("ADD FORMAT NS: %s len=%d\n",ent->name,(int)l);
    }
  }

  if(arrp)
    *arrp = ns;
  return (zktree_ns_t*)ns->elts;
}

static
apr_array_header_t *array_clone(apr_pool_t *p, apr_array_header_t *src)
{
  int i;
  apr_array_header_t *dst = apr_array_make(p,src->nalloc,src->elt_size);

  assert(src->elt_size == sizeof(zktree_ns_t));
  AN(dst);
  for(i = 0; i < src->nelts; i++) {
    zktree_ns_t *se,*de;
    de = &APR_ARRAY_PUSH(dst,zktree_ns_t);
    se = &APR_ARRAY_IDX(src,i,zktree_ns_t);
    memcpy(de,se,src->elt_size);
    de->name = apr_pstrdup(p,se->name);
  }

  return dst;
}

static
const char *format_entry(const char *name,
                         char *buf, apr_size_t buflen,
                         zktree_t *zk)
{
  int i;
  char *cp;
  static apr_array_header_t *ear = NULL;
  static apr_pool_t *p = NULL;
  static apr_array_header_t *prev = NULL;

  if(p == NULL) {
    apr_pool_create(&p,zk->pool);
    zeke_pool_cleanup_indirect(p,p);
    zeke_pool_cleanup_indirect(p,ear);
    zeke_pool_cleanup_indirect(p,prev);
  }
  AN(zk->fmt);
  assert(zk->fmt->elt_size ==sizeof(zktree_ns_t));
  if(ear == NULL) {
    ear = apr_array_make(p,zk->fmt->nelts,zk->fmt->elt_size);
    prev = NULL;
  } else {
    apr_array_header_t *tmp_arr = prev;
    prev = ear;
    if(tmp_arr) {
      apr_array_clear(tmp_arr);
      ear = tmp_arr;
    } else
      ear = apr_array_make(p,zk->fmt->nelts,zk->fmt->elt_size);
  }

  build_namespaces(name,zk->fmt,&ear,zk,p);
  cp = buf;
  *cp++ = LINKCH_NO_JOIN;
  buflen--;
  for(i = 0; i < ear->nelts; i++) {
    char *next = cp;
    apr_size_t fmtlen;
    const char *fmt;
    zktree_ns_t *fmt_ent = NULL;
    zktree_ns_t *ent = &APR_ARRAY_IDX(ear, i, zktree_ns_t);
    zktree_ns_t *pe = NULL;

    if(i < zk->fmt->nelts) {
      fmt_ent = &APR_ARRAY_IDX(zk->fmt,i,zktree_ns_t);
      fmt = fmt_ent->name;
      fmtlen = fmt_ent->len;
    } else {
      fmt = "%s";
      fmtlen = buflen;
    }
    if(prev && i < prev->nelts)
      pe = &APR_ARRAY_IDX(prev, i, zktree_ns_t);
    if(prev && i <= prev->nelts && pe && strcmp(pe->name,ent->name) == 0) {
      char spcfmt[16];
      apr_snprintf(spcfmt,sizeof(spcfmt),"%%%lusx",(unsigned long)ent->len);
      next = cp + apr_snprintf(cp, buflen, spcfmt, "");
    } else {
      next = cp + apr_snprintf(cp, buflen, fmt, ent->name);
    }
    buflen -= strlen(cp);
    cp = next;
  }
  *cp = '\0';
  return buf;
}

static
void preformat(const char *name, zktree_t *zk)
{
  apr_pool_t *tmp;
  apr_array_header_t *ns = NULL;

  apr_pool_create(&tmp,zk->pool);
  build_namespaces(name,NULL,&ns,zk,tmp);
  if(!zk->fmt)
    zk->fmt = apr_array_make(zk->pool, 10, sizeof(zktree_ns_t));
  else if(ns->nelts > 0) {
    int i;
    zktree_ns_t *new,*old;
    for(i = 0; i < ns->nelts; i++) {
      new = old = NULL;
      if(i >= zk->fmt->nelts) {
        old = &APR_ARRAY_PUSH(zk->fmt,zktree_ns_t);
        memset(old,0,sizeof(*new));
      } else old = &APR_ARRAY_IDX(zk->fmt,i,zktree_ns_t);
      new = &APR_ARRAY_IDX(ns,i,zktree_ns_t);
      if(!old->magic) {
        old->magic = ZKTREE_NS_MAGIC;
        old->name = apr_pstrdup(zk->pool,new->name);
        old->len = new->len;
        printf("ADD fmt %s/%d\n",old->name,(int)old->len);
      } else if(new->len > old->len) {
        old->name = apr_pstrdup(zk->pool,new->name);
        old->len = new->len;
        printf("UPDATE fmt %s/%d\n",old->name,(int)old->len);
      }
    }
  }
  apr_pool_destroy(tmp);
}
#endif

static
int get_depth(const zeke_tree_node_t *n)
{
  int depth = 0;
  for(; n != NULL; n = zeke_tree_node_parent_get(n))
    depth++;

  return depth;
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

static void interval_event(int ignore, short what, void *data)
{
  zktree_t *zk = (zktree_t*)data;

  if(zk->conn) {
    zk->timeout_after = APR_TIME_C(0);
  } else if(zk->timeout_after > APR_TIME_C(0)) {
    if(apr_time_now() - zk->start_time > zk->timeout_after) {
      if(!(zk->mode & OPT_QUIET))
        zeke_eprintf("Zookeeper connection timeout, aborting.\n");
      zeke_safe_shutdown_conn(zk->conn, 1);
      return;
    }
  }
}

static
void zkt_dump(const zeke_tree_node_t *node, int depth, char *buf,
              apr_size_t blen, zktree_t *zk)
{
  const char *name, *absname = "me";
  const char *pname;
  const zeke_tree_head_t *head;
  apr_interval_time_t ts;
  char *fmt = "%s\n";

  ZEKE_MAGIC_CHECK(zk, ZKTREE_MAGIC);
#if 0
  char fmt[256] = "%s\n";
  apr_snprintf(fmt,sizeof(fmt),"%%%ds%%s [%%" APR_UINT64_T_FMT "]\n",depth);
#endif
  zeke_tree_node_info_get(node,&name,&absname,&ts);
#if 0
  format_entry(absname,buf,blen,zk);
#endif
  if(zk->mode & OPT_SHORT)
    pname = name;
  else
    pname = absname;
  if(zk->mode & OPT_VERBOSE)
    zeke_printf("[%" APR_UINT64_T_HEX_FMT "] %s\n",ts,pname);
  else
    zeke_printf(fmt,pname);
  head = zeke_tree_node_children_get(node);
  if(head && !ZKTREE_EMPTY(head))
    ZKTREE_FOREACH(node,head)
      zkt_dump(node,depth+2,buf,blen,zk);
}

struct rm_arg {
  apr_pool_t *p;
  zktree_t *zk;
  const char *node;
};

static struct rm_arg
*make_rm_arg(zktree_t *zk, const char *node)
{
  apr_pool_t *p;
  struct rm_arg *arg;

  ZEKE_MAGIC_CHECK(zk, ZKTREE_MAGIC);
  AN(node);
  AAOK(apr_pool_create(&p,zk->pool));
  arg = apr_palloc(p,sizeof(*arg));
  arg->p = p;
  arg->zk = zk;
  arg->node = apr_pstrdup(p,node);
  return arg;
}

static
zeke_cb_t rm_root(const zeke_callback_data_t *cbd)
{
  zk_status_t st;
  zeke_tree_t *t;
  zktree_t *zk = (zktree_t*)cbd->ctx;

  ZEKE_MAGIC_CHECK(zk, ZKTREE_MAGIC);
  t = zk->tree;
  AN(t);

  st = zeke_callback_status_get(cbd);

  if(!ZEKE_STATUS_IS_OKAY(st)) {
    zeke_apr_error(zk->path,st);
    zeke_callback_status_set(cbd,ZEOK);
  }
  zeke_tree_hold_set(t,0);
  zeke_tree_destroy(t);

  zeke_safe_shutdown_conn(zk->conn, ZKE_TO_ERROR(st));
  return ZEKE_CALLBACK_DESTROY;
}
static
zeke_cb_t rm_complete(const zeke_callback_data_t *cbd)
{
  zk_status_t st;
  zeke_tree_t *t;
  zeke_tree_iter_t *iter;
  struct rm_arg *arg = (struct rm_arg*)cbd->ctx;
  zktree_t *zk = arg->zk;
  int deleted_nodes = 0;
  const char *pname, *name, *absname = "me";
  apr_time_t when = APR_TIME_C(1);
  zeke_tree_node_t *node;

  ZEKE_MAGIC_CHECK(zk, ZKTREE_MAGIC);

  t = zk->tree;
  iter = zk->state;
  AN(t);
  AN(iter);
  st = zeke_callback_status_get(cbd);


  iter = zk->state;
  zk->state = NULL;
  AN(iter);
  node = zeke_tree_iter_this(iter,NULL);
  AN(node);
  zeke_tree_node_info_get(node,&name,&absname,&when);

  if(zk->mode & OPT_SHORT)
    pname = name;
  else
    pname = absname;

  if(!ZEKE_STATUS_IS_OKAY(st)) {
    zeke_apr_eprintf(st,"rm %s",arg->node);
    zeke_safe_shutdown_conn(zk->conn, ZKE_TO_ERROR(st));
    return ZEKE_CALLBACK_DESTROY;
  } else if (zk->mode & OPT_VERBOSE) {
    zeke_printf("removed %s [%" APR_UINT64_T_HEX_FMT "]\n", pname,
                when);
  } else if(!(zk->mode & OPT_QUIET)) {
    zeke_printf("removed %s\n",name);
  }
  apr_pool_destroy(arg->p);

  ZKTREE_REMOVE(node);
  for(iter = zeke_tree_iter_next(iter,NULL); iter != NULL;
      iter = zeke_tree_iter_next(iter,NULL)) {
    struct rm_arg *arg;
    zk_status_t st;
    node = zeke_tree_iter_this(iter,NULL);
    AN(node);
    zeke_tree_node_info_get(node,&name,&absname,&when);

    if(zk->mode & OPT_SHORT)
      pname = name;
    else
      pname = absname;

    if(zk->mode & OPT_VERBOSE)
      INFO("sending adelete: %s [%" APR_UINT64_T_HEX_FMT "]",pname,when);

    arg = make_rm_arg(zk,absname);
    st = zeke_adelete(NULL,zk->conn,arg->node,-1,rm_complete,arg);
    if(!ZEKE_STATUS_IS_OKAY(st)) {
      zeke_apr_eprintf(st,"removing node '%s'",arg->node);
      zeke_safe_shutdown_conn(zk->conn, ZKE_TO_ERROR(st));
      return ZEKE_CALLBACK_DESTROY;
    }
    deleted_nodes++;
    break;
  }

  if(deleted_nodes)
    zk->state = iter;
  else {
    zeke_tree_iter_destroy(iter);
    /* finally remove the root if there is one */
    if (strcmp(zk->path,"/") != 0) {
      st = zeke_adelete(NULL,zk->conn,zk->path,-1,rm_root,zk);
      if(!ZEKE_STATUS_IS_OKAY(st)) {
        zeke_apr_eprintf(st,"removing node '%s'",zk->path);
        zeke_safe_shutdown_conn(zk->conn, ZKE_TO_ERROR(st));
        return ZEKE_CALLBACK_DESTROY;
      }
    }
  }
  return ZEKE_CALLBACK_DESTROY;
}

static
int zkt_complete(zeke_tree_t *t, zeke_context_t *ctx)
{
  zk_status_t st = ZEOK;
  zktree_t *zk = (zktree_t*)ctx;
  const zeke_tree_head_t *head;
  const zeke_tree_node_t *node;
  const char *pname,*root;
  apr_pool_t *p;
  char buf[1024];

  ZEKE_MAGIC_CHECK(zk, ZKTREE_MAGIC);

  if(zeke_tree_is_error(zk->tree,&st)) {
    zeke_apr_eprintf(st,"%s [zookeeper error]",zk->path);
    zeke_safe_shutdown_conn(zk->conn, ZKE_TO_ERROR(st));
    return 0;
  }

  st = zeke_tree_head_get(zk->tree,&root,&head);
  if(!ZEKE_STATUS_IS_OKAY(st))
    zeke_fatal(zk->path,st);
  if(zk->mode & OPT_VERBOSE)
    INFO("root: %s (%d nodes)\n",root,zeke_tree_node_count(zk->tree));

  p = zeke_tree_pool_get(t);
  if ((zk->mode & 0x00ff) == OPT_RM) {
    int deleted_nodes = 0;
    zeke_tree_iter_t *iter = zeke_tree_iter_first(zk->tree, zeke_tree_iter_bottom,
                                                  NULL,p);
    zk->state = NULL;
    for(; iter != NULL; iter = zeke_tree_iter_next(iter,NULL)) {
      struct rm_arg *arg;
      const char *name, *absname = "me";
      apr_time_t when = APR_TIME_C(1);
      zeke_tree_node_t *node = zeke_tree_iter_this(iter,NULL);
      AN(node);
      zeke_tree_node_info_get(node,&name,&absname,&when);
      if(zk->mode & OPT_SHORT)
        pname = name;
      else
        pname = absname;
      if(zk->mode & OPT_VERBOSE)
        INFO("RM: %s [%" APR_UINT64_T_HEX_FMT "]",pname,when);
      arg = make_rm_arg(zk,absname);
      st = zeke_adelete(NULL,zk->conn,arg->node,-1,rm_complete,arg);
      if(!ZEKE_STATUS_IS_OKAY(st)) {
        zeke_apr_eprintf(st,"removing node '%s'",arg->node);
        zeke_safe_shutdown_conn(zk->conn, ZKE_TO_ERROR(st));
        return 0;
      }
      deleted_nodes++;
      break;
    }
    if(deleted_nodes)
      zk->state = iter;

    if(!zk->state) {
      zeke_tree_iter_destroy(iter);
      zeke_safe_shutdown_conn(zk->conn, 0);
      return 0;
    }
    zeke_tree_hold_set(t,1);
    return 1;
  }
  ZKTREE_FOREACH(node,head)
    zkt_dump(node,get_depth(node),buf,sizeof(buf),zk);
  zeke_tree_hold_set(t,0);
  LOG("SHUTDOWN");
  zeke_safe_shutdown_conn(zk->conn,0);
  return 0;
}

static
apr_uint32_t limit_depth(const zeke_tree_node_t *p,
                         const char **name,
                         const char **absname,
                         zeke_context_t *ctx)
{
  zktree_t *zk = (zktree_t*)ctx;
  const char *parent_name = NULL;
  int depth = get_depth(p);

  ZEKE_MAGIC_CHECK(zk, ZKTREE_MAGIC);
  if(p) {
    ZEKE_MAGIC_CHECK(p, ZEKE_TREE_NODE_MAGIC);
    zeke_tree_node_info_get(p,NULL,&parent_name,NULL);
  }
  if(!parent_name) {
#if 0
    preformat(*name,zk);
    INFO("[%d nodes] build: %s",zeke_tree_node_count(zk->tree),*name);
#endif
    return ZKTFILTER_KEEP;
  } else depth++;

  if(zk->mode & OPT_VERBOSE) {
    INFO("[%d nodes/depth %d] build: %s --> %s",zeke_tree_node_count(zk->tree),
          depth, parent_name,*name);
  } else if(!(zk->mode & OPT_QUIET)) {
    LOG("[%d nodes/depth %d] build: %s --> %s",zeke_tree_node_count(zk->tree),
          depth, parent_name,*name);
  }

  if(zk->max_depth && depth > zk->max_depth)
    return ZKTFILTER_DISCARD;
  return ZKTFILTER_KEEP;
}

static void zkt_closed(const zeke_connection_t *c)
{
  apr_socket_t *s = c->sock;
  LOG("ZKTREE: CONNECTION CLOSED");
  if(zeke_socket_event_get(s)) {
    LOG("ZKTREE: Connection has an associated event");
  } else {
    LOG("ZKTREE: Connection has NO associated event");
  }
}

static void zkt_new_connection(zeke_connection_t *c)
{
  LOG("ZKTREE: NEW CONNECTION: %pp",c);
}

static void zkt_event_loop_begin(struct event_base *base, apr_pool_t *p)
{
  LOG("ZKTREE: EVENT LOOP +++ (base=%pp, pool=%pp)",base,p);
}

static void zkt_event_loop_end(struct event_base *base, apr_pool_t *p)
{
  LOG("ZKTREE: EVENT LOOP --- (base=%pp, pool=%pp)",base,p);
}

static int zkt_shutdown(int *code)
{
  if(!code || *code == 0)
    return ZEKE_HOOK_DECLINE;
  WARN("ZKTREE: ERROR DETECTED -- CODE %d",*code);
  return ZEKE_HOOK_OKAY;
}

static void zkt_setup(zeke_connection_t *c, void *d)
{
  zk_status_t st;
  zktree_t *zk = (zktree_t*)d;
  zeke_tree_t *t;

  ZEKE_MAGIC_CHECK(zk, ZKTREE_MAGIC);
  zeke_hook_connection_closed(c,zkt_closed, "zktree", NULL, NULL, ZEKE_HOOK_DEFAULT_ORDER);
  zeke_hook_sort_all(zeke_connection_pool_get(c),NULL);
  zk->conn = c;

  if(zeke_global_hooks_are_modifiable())
    zeke_hook_shutdown(zkt_shutdown,NULL,NULL,ZEKE_HOOK_DEFAULT_ORDER);

  AZ(zk->tree);
  if(!zk->path) {
    if(*zk->node != '/')
      zk->path = apr_psprintf(zk->pool,"/%s",zk->node);
    else
      zk->path = apr_pstrdup(zk->pool,zk->node);
  }
  LOG("ROOT=%s",zk->path);

  st = zeke_tree_create(&zk->tree,zk->path,zkt_complete,NULL,zk,c);
  if(!ZEKE_STATUS_IS_OKAY(st))
    zeke_fatal(zk->path,st);
  t = zk->tree;
  AZOK(zeke_tree_build_filter_set(t,limit_depth));
  AZOK(zeke_tree_start(t));
}

static int start_zk(const char *zkhosts, zktree_t *zk, const char *ns)
{
  int i,rc = 0;
  apr_pool_t *p = zk->pool;
  zk_status_t st = ZEOK;
  zeke_runloop_callback_fn setup_func = zkt_setup;
  zeke_io_event_t ev_loop = NULL;
  struct event_base *base;

  zk->node = zeke_nodepath_join(p,ns,zk->node,NULL);
  AN(zk->node);
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
  do {
    /* make sure we have an event base at the root */
    (void)zeke_event_base_get_ex(zk->pool,1);

    ev_loop = zeke_io_event_loop_create(zk->pool,NULL);
    assert(zeke_io_event_loop_base_get(ev_loop) == zeke_event_base_get(zk->pool));

    AZOK(zeke_event_create(&zk->timer_ev,zk->pool));
    zeke_pool_pre_cleanup_indirect(zk->pool,zk->timer_ev);
    AN(zk->timer_ev);
    AZOK(zeke_generic_event_type_set(zk->timer_ev,EV_TIMEOUT|EV_PERSIST));
    AZOK(zeke_generic_event_callback_set(zk->timer_ev,interval_event,zk));
    AZOK(zeke_generic_event_add(zk->timer_ev, APR_TIME_C(500) * APR_TIME_C(1000)));
    AZOK(zeke_io_event_loop_connect(ev_loop,zkhosts,setup_func,0,zk));
  } while(0);

  for(base = zeke_event_base_get(zk->pool), i = -1;
      base != NULL && !zeke_io_event_loop_gotexit(ev_loop,&rc);)
  {
    if(i != -1)
      assert(zeke_io_event_loop_restart(ev_loop) == APR_SUCCESS);
    i = event_base_loop(base,0);
    switch(i) {
    case -1:
      st = APR_FROM_OS_ERROR(errno);
      if(st != APR_SUCCESS)
        zeke_fatal("event_base_loop",st);
      break;
    case 1:
    case 0:
      if(zeke_io_event_loop_gotbreak(ev_loop)) {
        AZOK(zeke_io_event_loop_status_get(ev_loop,&st));
        zeke_apr_eprintf(st,"io_event_loop status for %pp",ev_loop);
        if(!ZEKE_STATUS_IS_OKAY(st)) {
          rc = ZKE_TO_ERROR(st);
          zeke_safe_shutdown(rc);
          goto start_zk_exit;
        }
      }
      break;
    default:
      zeke_eprintf("GOT %d\n",i);
      break;
    }
  }

start_zk_exit:
  if(zktree_pool)
    apr_pool_destroy(zktree_pool);
  exit(rc);
}

ZEKE_HIDDEN
int main(int argc, const char * const *argv,
                   const char * const *env)
{
  zk_status_t st = zeke_init_cli(&argc,&argv,&env);
  apr_pool_t *p = NULL;
  apr_pool_t *getopt_pool = NULL;
  apr_getopt_t *getopt = NULL;
  int i,opt;
  const char *opt_arg;
  zktree_t zk;
  const char *zkhosts = NULL;

  memset(&zk,0,sizeof(zk));
  zk.magic = ZKTREE_MAGIC;
  zk.start_time = apr_time_now();
  zk.timeout_after = APR_TIME_C(TIMEOUT_AFTER_MSEC) * APR_TIME_C(1000);

  AZOK(st);
  AN((zktree_pool = zeke_root_subpool_create()));
  zeke_pool_pre_cleanup_indirect(zktree_pool,zktree_pool);
  AAOK(apr_pool_create(&p,zktree_pool));

#ifdef DEBUGGING
  zoo_set_debug_level(ZOO_LOG_LEVEL_INFO);
#elif defined(NDEBUG)
  zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);
#else
  zoo_set_debug_level(ZOO_LOG_LEVEL_WARN);
#endif
  zeke_pool_tag(p, "zktree app pool");

  AAOK(apr_pool_create(&zk.pool,zktree_pool));
  AAOK(apr_pool_create(&getopt_pool,zktree_pool));
  AAOK(apr_getopt_init(&getopt,getopt_pool,argc,argv));

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
      case OPT_DEBUG:
        zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
        break;
      case OPT_S:
        zk.mode |= OPT_SHORT;
        break;
      case OPT_Q:
        zk.mode &= ~OPT_VERBOSE;
        zk.mode |= OPT_QUIET;
        break;
      case OPT_V:
        zk.mode &= ~OPT_QUIET;
        opt = OPT_VERBOSE;
        /* fall-thru */
      case OPT_RM:
        zk.mode |= opt;
        break;
      case OPT_MAX_DEPTH:
          for(;apr_isspace(*opt_arg); opt_arg++)
            ;
          do {
            char *end;
            apr_int64_t val;

            if(strcasecmp(opt_arg,"inf") == 0) {
              zk.max_depth = APR_UINT32_MAX;
              break;
            }
            val = apr_strtoi64(opt_arg,&end,0);
            if((!end || *end) || val < APR_INT64_C(1)) {
              zeke_eprintf("'%s' is not a valid depth, must be integer greater than 0.\n",
                           opt_arg);
              exit(13);
            }
            zk.max_depth = (apr_uint32_t)val;
          } while(0);

        break;
      case OPT_ZKHOSTS:
        for(;apr_isspace(*opt_arg); opt_arg++)
          ;
        zkhosts = apr_pstrdup(p,opt_arg);
        if(!*zkhosts) {
          zeke_eprintf("Invalid connection string: %s\n",opt_arg);
          exit(10);
        }
        break;
      default:
        zeke_fatal("unsupported option", APR_BADARG);
        break;
      }
      break;
    default:
      zeke_fatal("zktree",st);
      break;
    }
  }

  for(i = getopt->argc-1; i >= getopt->ind && zk.node == NULL; i--) {
    if(getopt->argv[i] != NULL) {
      zk.node = validate_nodep(zk.pool,getopt->argv[i],"%s is not a valid node");
    }
  }

  if(zk.node == NULL) {
    zeke_eprintf("%s [%s] (argc=%d,ind=%d)\n","no zookeeper node path specified",
                  getopt->argv[getopt->ind],getopt->argc,getopt->ind);
    usage(5);
  }

  for(; apr_isspace(*zk.node) || *zk.node == '/'; zk.node++)
    ;
  if(strcmp(zk.node,"") == 0 && !zk.max_depth) {
    if(!(zk.mode & OPT_QUIET)) {
      zeke_eprintf("%s\n",
           "(Recursive operations on the root of a zookeeper hierarchy can be intensive, applying\n"
           "automatic --max-depth=2. Use --max-depth=inf to override this in the future.)");
    }
    zk.max_depth = 2;
  } else if(zk.max_depth == APR_UINT32_MAX)
    zk.max_depth = 0;
  apr_pool_destroy(getopt_pool);

  zeke_hook_new_connection(zkt_new_connection,NULL,NULL,ZEKE_HOOK_DEFAULT_ORDER);
  zeke_hook_event_loop_begin(zkt_event_loop_begin,NULL,NULL,ZEKE_HOOK_DEFAULT_ORDER);
  zeke_hook_event_loop_end(zkt_event_loop_end,NULL,NULL,ZEKE_HOOK_DEFAULT_ORDER);
  zeke_global_hooks_modified();

  i = start_zk(zkhosts,&zk,"/");
  apr_pool_destroy(zktree_pool);
  return i;
}
