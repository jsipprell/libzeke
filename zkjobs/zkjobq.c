#include "zkjobs.h"
#include "libzeke_log.h"

#define ZKJOBQ_SESSION_KEY "zkjobq_session"

static apr_pool_t *zkjob_pool = NULL;
ZEKE_HIDDEN
int main(int argc, const char* const *argv, const char* const *env)
{
  zeke_shmlog_header_t *slog;
  zk_status_t st = zeke_init_cli(&argc, &argv, &env);
  int rc = 0;
  apr_pool_t *pool;

  AZOK(st);
  AN(zkjob_pool = zeke_root_subpool_create());
  AAOK(apr_pool_create(&pool,zkjob_pool));
  zeke_shmlog_ident_set("zkjobd");
  slog = zeke_shmlog_open(pool,-1);
  if(slog) {
    unsigned i;
    apr_uint32_t nrecs,first,next;
    const zeke_log_entry_t *le;

    st = zeke_shmlog_size_get(slog,&nrecs,&first,&next);
    if(st != APR_SUCCESS)
      zeke_fatal("shmlog",st);
    zeke_printf("SHM: max=%u, first=%u, next=%u\n",(unsigned)nrecs,(unsigned)first,
                                                   (unsigned)next);
    for(i = first; i != next; ) {
      le = zeke_shmlog_entry_get(slog,i);
      if(le == NULL) {
        zeke_eprintf("uhoh, out of range at %u\n",i);
        break;
      } else
        zeke_printf("[%s] %s",le->source,le->info);
      if (++i >= nrecs)
        i = 0;
    }
  }
  apr_pool_destroy(zkjob_pool);
  return rc;
}
