#ifndef _LIBZEKE_COMPAT_H
#define _LIBZEKE_COMPAT_H

/* Compatibility decls */
#ifndef HAVE_APR_TABLE_CLONE
ZEKE_EXPORT apr_table_t *apr_table_clone(apr_pool_t*,const apr_table_t*);
#endif

#ifndef HAVE_APR_ARRAY_CLEAR
ZEKE_EXPORT void apr_array_clear(apr_array_header_t*);
#endif

#ifndef apr_pool_create_unmanaged
#define apr_pool_create_unmanaged(p) apr_pool_create( (p), NULL )
#endif

#ifndef HAVE_QSORT_R
ZEKE_EXPORT void qsort_r(void *base, size_t nmemb, size_t size, void *thunk,
                       int (*compar)(const void*, const void* ,void*));
#endif

#endif /* _LIBZEKE_COMPAT_H */
