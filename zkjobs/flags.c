/* zkjobd/zkjobc flag parsing and unparsing */
#include "zkjobs.h"

static struct zkproc_flag {
  char c;
  enum zkprocflags v;
  int has_arg;
  const char *desc;
} pflags[] = {
  { 's',  ZKPROCF_USE_SHELL,  0,   "Always execute the job using /bin/sh -c"},
  { 'p',  ZKPROCF_CUSTOM_PATH, 0,  "Always use a custom PATH."},
  { 'e',  ZKPROCF_STDIN_PIPE, 0,   "Always use a custom environment"},
  { 'E',    ZKPROCF_ERROR_OUTPUT, 0, "Capture job stderr output (not impl)"},
  { 'o',  ZKPROCF_ALLOW_PATH_OVERRIDE, 0, "Allow PATH in environment to override default PATH"},
  { 'f',  ZKPROCF_TEMP_FILE, 0,
      "Write job to temporary executable file and delete it after completion (stdin unused)"},
  { 0, 0, 0, 0}
};

ZEKE_HIDDEN
void zkjobq_flags_print(int err)
{
  struct zkproc_flag *z;

  for(z = &pflags[0]; z->c; z++) {
    if(z->desc) {
      if(err)
        zeke_eprintf("  %c: %s\n",z->c,z->desc);
      else
        zeke_printf("  %c: %s\n", z->c,z->desc);
    }
  }
}

ZEKE_HIDDEN
zk_status_t zkjobq_flags_parse(const char *flags, apr_uint32_t *f)
{
  struct zkproc_flag *z;
  if(!f || !flags)
    return APR_EINVAL;

  for(; apr_isspace(*flags); flags++) ;
  *f = 0;
  for(; *flags && !apr_isspace(*flags); flags++) {
    for(z = &pflags[0]; z->c; z++)
      if(z->c == *flags) {
        *f |= z->v;
        break;
      }
  }
  return APR_SUCCESS;
}

static
apr_size_t add_flag(char c, struct zkws *ws, apr_size_t *sz, apr_size_t r)
{
  assert(c > 0);
  if(r-1 < 1) {
    apr_size_t nsz = (*sz) * 2;
    r = ws->r - ws->b;
    zk_workspace_release(ws,0);
    *sz = zk_workspace_reserve(ws,nsz);
    assert(*sz >= nsz);
    ws->r = ws->b + r;
    r = *sz;
  }
  assert(ws->r < ws->e-1);
  *ws->r++ = c;
  return r-1;
}

ZEKE_HIDDEN
const char *zkjobq_flags_unparse(apr_uint32_t f, struct zkws *ws)
{
  struct zkproc_flag *z;
  apr_size_t u,remain;
  unsigned short cnt = 0;
  ZK_CHECK_OBJ_NOTNULL(ws, ZK_WS_MAGIC);

  u = remain = zk_workspace_reserve(ws,0);
  assert(u >= 1);
  for(z = &pflags[0]; z->c; z++)
    if((f & z->v) == z->v) {
      remain = add_flag(z->c,ws,&u,remain);
      cnt++;
    }

  if(cnt == 0) {
    zk_workspace_release(ws,0);
    return NULL;
  }
  *ws->r++ = '\0'; assert(ws->r < ws->e);
  return zk_workspace_releasep(ws,ws->r);
}
