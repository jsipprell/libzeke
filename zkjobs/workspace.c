/* temporary workspace allocation/handling */
#include "zkjobs.h"

#include <apr_atomic.h>

#ifndef ZKWS_BSIZE
#define ZKWS_BSIZE 2048
#endif

static apr_pool_t *get_pool(apr_pool_t *parent)
{
  apr_pool_t *newp;
  static apr_pool_t *p = NULL;
  static volatile apr_uint32_t spinlock = 0;

  if(!p && !parent) {
    while(apr_atomic_cas32(&spinlock,1,0) != 0)
      ;
    if(!p) {
      AAOK(apr_pool_create_unmanaged(&p));
      AN(p);
    }
    assert(apr_atomic_dec32(&spinlock) == 0);
    parent = p;
  }
  AAOK(apr_pool_create(&newp,(parent ? parent : p)));
  return newp;
}

ZKTOOL_PRIVATE(struct zkws*)
zk_workspace_new_ex(apr_pool_t *parent, const char *tag)
{
  struct zkws *ws;
  apr_size_t sz = ZKALIGN(sizeof(*ws)+ZKWS_BSIZE);
  apr_pool_t *p  = get_pool(parent);
  AN(p);
  if(tag)
    zeke_pool_tag(p,tag);

  ws = apr_palloc(p,sz);
  AN(ws);
  ws->magic = ZK_WS_MAGIC;
  ws->p = p;
  ws->r = NULL;
  ws->data = NULL;
  sz -= ZKALIGN(sizeof *ws);
  ws->f = ws->b = ((char*)ws) + ZKALIGN(sizeof *ws);
  ws->e = ws->b + (sz - ZKALIGN(32)); /* safety */
  return ws;
}

ZKTOOL_PRIVATE(struct zkws*)
zk_workspace_new(apr_pool_t *parent)
{
  return zk_workspace_new_ex(parent,NULL);
}

ZKTOOL_PRIVATE(struct zkws*)
zk_subworkspace_new(struct zkws *parent, ...)
{
  va_list ap;
  ZK_CHECK_OBJ_NOTNULL(parent,ZK_WS_MAGIC);
  va_start(ap,parent);
  return zk_workspace_new_ex(parent->p,va_arg(ap,const char*));
  va_end(ap);
}

ZKTOOL_PRIVATE(void)
zk_workspace_free(struct zkws **ws)
{
  if(ws) {
    if(*ws) {
      apr_pool_t *p;
      ZK_CHECK_OBJ_NOTNULL(*ws,ZK_WS_MAGIC);
      p = (*ws)->p;
      if(p) {
        (*ws)->p = NULL;
        apr_pool_destroy(p);
      }
      *ws = NULL;
    }
  }
}

ZKTOOL_PRIVATE(apr_off_t)
zk_workspace_reserve(struct zkws *ws, apr_size_t bytes)
{
  apr_off_t u;
  apr_size_t sz;
  ZK_CHECK_OBJ_NOTNULL(ws,ZK_WS_MAGIC);
  AZ(ws->r); AN(ws->e); AN(ws->b);
  assert(ws->e > ws->b);
  assert(ws->f >= ws->b && ws->f < ws->e);
  sz = ws->e - ws->f;
  if(bytes > 0) {
    if (ZKALIGN(bytes) >= sz) {
      char *n;
      apr_size_t nsz;
      for(nsz = ((ws->e - ws->b)+ZKALIGN(32))*2; bytes >= nsz; nsz *= 2)
        ;
      n = apr_palloc(ws->p,ZKALIGN(nsz));
      AN(n);
      nsz -= ZKALIGN(32);
      memmove(n,ws->b,(ws->e - ws->b));
      ws->e = n + nsz;
      ws->f = n + (ws->f - ws->b);
      ws->b = n;
    }
    if(bytes >= (ws->e - ws->f))
      bytes = (ws->e - ws->f) - 1;
    ws->r = ws->f + bytes;
    assert(ws->r < ws->e);
    u = bytes;
  } else {
    u = sz;
    ws->r = ws->f;
  }
  return u;
}

ZKTOOL_PRIVATE(void*)
zk_workspace_reservep(struct zkws *ws, apr_size_t bytes)
{
  apr_off_t u = zk_workspace_reserve(ws,bytes);
  if(u >= bytes)
    return ws->f;
  if(ws->r)
    zk_workspace_release(ws,0);
  return NULL;
}

ZKTOOL_PRIVATE(char*)
zk_workspace_release(struct zkws *ws, apr_size_t bytes)
{
  char *f;
  ZK_CHECK_OBJ_NOTNULL(ws,ZK_WS_MAGIC);
  AN(ws->r); AN(ws->e); AN(ws->b);
  assert(ws->e > ws->b);
  assert((ws->f+bytes) >= ws->b && (ws->f+bytes) < ws->e);
  assert(ws->r < ws->e);
  assert(ws->r >= ws->f);
  f = ws->f;
  if(ws->f + ZKALIGN(bytes) < ws->e)
    bytes = ZKALIGN(bytes);
  ws->f += bytes;
  ws->r = NULL;
  return f;
}

ZKTOOL_PRIVATE(void)
zk_workspace_clear(struct zkws **wsp)
{
  struct zkws *ws;
  apr_size_t sz;
  apr_pool_t *p;
  AN(wsp);
  ws = *wsp;
  ZK_CHECK_OBJ_NOTNULL(ws,ZK_WS_MAGIC);
  AN(ws->e); AN(ws->b);
  assert(ws->e > ws->b);
  if(ws->r)
    zk_workspace_release(ws,0);
  p = ws->p;
  apr_pool_clear(p);
  sz = ZKALIGN(sizeof(*ws)+ZKWS_BSIZE);
  ws = apr_palloc(p,sz);
  AN(ws);
  ws->magic = ZK_WS_MAGIC;
  ws->p = p;
  ws->r = NULL;
  ws->data = NULL;
  sz -= ZKALIGN(sizeof *ws);
  ws->f = ws->b = ((char*)ws) + ZKALIGN(sizeof *ws);
  ws->e = ws->b + (sz - ZKALIGN(32)); /* safety */
  ws->f = ws->b;
  *wsp = ws;
}

ZKTOOL_PRIVATE(char*)
zk_workspace_releasep(struct zkws *ws, void *p)
{
  char *f;
  ZK_CHECK_OBJ_NOTNULL(ws,ZK_WS_MAGIC);
  AN(ws->r); AN(ws->e); AN(ws->b);
  assert(ws->e > ws->b);
  assert(ws->r < ws->e);
  assert(ws->r >= ws->f);

  assert((char*)p >= ws->f);
  assert((char*)p < ws->e);
  f = ws->f;
  if (ws->b + ZKALIGN((char*)p - ws->b) >= ws->e)
    ws->f = p;
  else
    ws->f = ws->b + ZKALIGN((char*)p - ws->b);
  ws->r = NULL;
  return f;
}

ZKTOOL_PRIVATE(apr_off_t)
zk_workspace_snapshot(struct zkws *ws)
{
  ZK_CHECK_OBJ_NOTNULL(ws,ZK_WS_MAGIC);
  AZ(ws->r); AN(ws->e); AN(ws->b);
  assert(ws->e > ws->b);
  assert(ws->f >= ws->b && ws->f < ws->e);
  return ws->f - ws->b;
}

ZKTOOL_PRIVATE(void)
zk_workspace_restore(struct zkws *ws, apr_off_t o)
{
  ZK_CHECK_OBJ_NOTNULL(ws,ZK_WS_MAGIC);
  AZ(ws->r); AN(ws->e); AN(ws->b);
  assert(ws->e > ws->b);
  assert(ws->f >= ws->b && ws->f < ws->e);

  assert((ws->b + o) >= ws->b && (ws->b + o) < ws->e);
  ws->f = ws->b + o;
}

ZKTOOL_PRIVATE(void)
zk_workspace_reset(struct zkws *ws)
{
  ZK_CHECK_OBJ_NOTNULL(ws,ZK_WS_MAGIC);
  AZ(ws->r); AN(ws->e); AN(ws->b);
  assert(ws->e > ws->b);
  assert(ws->f >= ws->b && ws->f < ws->e);

  ws->f = ws->b;
}

ZKTOOL_PRIVATE(void*)
zk_workspace_alloc(struct zkws *ws, apr_size_t bytes)
{
  char *m;
  apr_size_t sz;
  ZK_CHECK_OBJ_NOTNULL(ws,ZK_WS_MAGIC);
  AZ(ws->r); AN(ws->e); AN(ws->b);
  assert(ws->e > ws->b);
  assert(ws->f >= ws->b && ws->f < ws->e);

  sz = ws->e - ws->f;
  if(ZKALIGN(bytes) >= sz)
    m = apr_palloc(ws->p,ZKALIGN(bytes));
  else {
    assert(ZKALIGN(ws->f - ws->b) >= (ws->f - ws->b));
    m = ws->b + ZKALIGN(ws->f - ws->b);
    assert(m >= ws->f);
    ws->f = m + ZKALIGN(bytes);
    assert(ws->f < ws->e);
  }
  return m;
}

ZKTOOL_PRIVATE(char*)
zk_workspace_nodecatv(struct zkws *ws, va_list ap)
{
  va_list ap2;
  const char *arg = NULL, *root = NULL;
  char *node = NULL;
  apr_size_t sz = 0;
  apr_size_t n = 0;
  AZ(ws->r); AN(ws->e); AN(ws->b);
  root = zeke_nodepath_chroot_get();
  va_copy(ap2,ap);

  if(!root || !*root) {
    arg = va_arg(ap2, const char *);
    sz = 1;
  } else
    arg = root;
  for(; arg != NULL; sz++) {
    if(!*arg || *arg != '/')
      n++;
    n += strlen(arg);
    arg = va_arg(ap2,const char *);
  }
  va_end(ap2);
  if(sz > 1) {
    char *f;
    const char *s;
    char last = '\0';
    apr_size_t u;
    if(!root || !*root)
      arg = va_arg(ap, const char *);
    else
      arg = root;
    u = zk_workspace_reserve(ws,n+1);
    assert(u >= n+1);
    for(f = ws->f; arg != NULL; arg = va_arg(ap, const char*)) {
      while(*arg == '/') arg++;
      if(last != '/')
        *f++ = '/';
      for(s = arg; *s; s++) {
        if(*s != '/' || *(s+1))
          *f++ = *s;
      }
      last = *(f-1);
    }
    *f++ = '\0';
    assert(f - ws->f <= u);
    node = zk_workspace_releasep(ws,f);
  } else {
    node = zk_workspace_strdup(ws,(root ? root : "/"));
  }
  return node;
}

ZKTOOL_PRIVATE(char*)
zk_workspace_nodecat(struct zkws *ws, ...)
{
  char *b;
  va_list ap;

  va_start(ap,ws);
  b = zk_workspace_nodecatv(ws,ap);
  va_end(ap);
  return b;
}

ZKTOOL_PRIVATE(char*)
zk_workspace_strcatv(struct zkws *ws, const char *s, va_list ap)
{
  char *b;
  const char *cp;
  ZK_CHECK_OBJ_NOTNULL(ws,ZK_WS_MAGIC);
  AZ(ws->r); AN(ws->e); AN(ws->b);
  assert(ws->e > ws->b);
  assert(ws->f >= ws->b && ws->f < ws->e);

  ws->r = ws->f;
  for(; s != NULL; s = va_arg(ap,const char *)) {
    for(cp = s; *cp; cp++) {
      while(ws->f >= (ws->e-1)) {
        apr_off_t o = ws->r - ws->b;
        apr_size_t sz = (ws->e - ws->b) + ZKALIGN(32);
        char *n = apr_palloc(ws->p,ZKALIGN(sz*2));
        sz = ZKALIGN((ws->e - ws->b) * 2);
        AN(n);
        memmove(n,ws->b,(ws->e - ws->b));
        ws->e = n + sz;
        ws->f = n + (ws->f - ws->b);
        ws->b = n;
        ws->r = ws->b + o;
      }
      *ws->f++ = *cp;
    }
    *ws->f = '\0';
  }
  ws->f++;
  b = ws->r;
  ws->r = NULL;
  return b;
}

ZKTOOL_PRIVATE(char*)
zk_workspace_strcat(struct zkws *ws, const char *s, ...)
{
  char *b;
  va_list ap;
  va_start(ap,s);
  b = zk_workspace_strcatv(ws,s,ap);
  va_end(ap);
  return b;
}

ZKTOOL_PRIVATE(char*)
zk_workspace_strdup(struct zkws *ws, const char *s)
{
  return zk_workspace_strcat(ws,s,NULL);
}

ZKTOOL_PRIVATE(char*)
zk_workspace_strndup(struct zkws *ws, const char *s, apr_size_t n)
{
  char *b;
  apr_size_t u;
  const char *cp;
  ZK_CHECK_OBJ_NOTNULL(ws,ZK_WS_MAGIC);
  AZ(ws->r); AN(ws->e); AN(ws->b);
  assert(ws->e > ws->b);
  assert(ws->f >= ws->b && ws->f < ws->e);

  u = zk_workspace_reserve(ws,n+1);
  assert(u > n);
  for(cp = s, b = ws->f; *cp && n > 0; n--)
    *b++ = *cp++;
  *b++ = '\0';
  return zk_workspace_releasep(ws,b);
}

static inline void zk_vprintf_set(struct zkws *ws)
{
  /* cheat by taking advantage of the 32 byte safety zone */
  *((struct zkws**)ws->e) = ws;
}

static inline struct zkws *zk_vprintf_get(char *e)
{
  struct zkws *ws = (struct zkws*)e;
  ZK_CHECK_OBJ_NOTNULL(ws, ZK_WS_MAGIC);
  return ws;
}

static int zk_vprintf_flush(apr_vformatter_buff_t *vb)
{
  struct zkws *ws;

  AN(vb);
  ws = zk_vprintf_get(vb->endpos+1);
  assert(vb->curpos >= ws->f);
  assert(vb->endpos < ws->e);
  if(vb->curpos >= vb->endpos) {
    apr_size_t sz = vb->endpos - ws->f;
    apr_off_t o = vb->curpos - ws->f;
    zk_workspace_release(ws,0);
    assert(zk_workspace_reserve(ws,sz*2) > sz);
    vb->curpos = ws->f + o;
    vb->endpos = ws->e - 1;
    assert(vb->endpos > vb->curpos);
    zk_vprintf_set(ws);
  }
  return 0;
}

ZKTOOL_PRIVATE(char*)
zk_workspace_vprintf(struct zkws *ws, const char *fmt, va_list ap)
{
  apr_vformatter_buff_t vfmt;
  apr_size_t u;
  ZK_CHECK_OBJ_NOTNULL(ws, ZK_WS_MAGIC);
  AZ(ws->r); AN(ws->e); AN(ws->b);
  assert(ws->e > ws->b);
  assert(ws->f >= ws->b && ws->f < ws->e);

  u = zk_workspace_reserve(ws,0);
  if(u < 10) {
    zk_workspace_release(ws,0);
    u = zk_workspace_reserve(ws,1000);
  }
  assert(u > 10);
  zk_vprintf_set(ws);
  vfmt.curpos = ws->f;
  vfmt.endpos = ws->e-1;
  if(apr_vformatter(zk_vprintf_flush,&vfmt,fmt,ap) > 0) {
    if(vfmt.curpos >= vfmt.endpos-1)
      vfmt.curpos--;
    *(vfmt.curpos) = '\0';
    return zk_workspace_releasep(ws,vfmt.curpos+1);
  }
  if(ws->r)
    zk_workspace_release(ws,0);
  return NULL;
}

ZKTOOL_PRIVATE(char*)
zk_workspace_printf(struct zkws *ws, const char *fmt, ...)
{
  char *s;
  va_list ap;

  va_start(ap,fmt);
  s = zk_workspace_vprintf(ws,fmt,ap);
  va_end(ap);
  return s;
}
