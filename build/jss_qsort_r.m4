dnl
dnl jss_qsort_r.m4 : test for qsort_r presence and argument order difference
dnl between BSD and GNU.
dnl
dnl JSS_CHECK_QSORT_R(if-bsd, if-gnuc, if-not-found)
dnl
dnl All arguments are optional. HAVE_SORT_R will always be defined if ANY
dnl qsort_r funcution is present.
dnl
dnl If the <if-bsd> argument is missing and bsd qsort_r is detected then
dnl the symbol QSORT_R_BSD will be defined.
dnl If the <if-gnuc> argument ismissing and gnu qsort_r is detected then
dnl the symbol QSORT_R_GNUC will be defined.
dnl
dnl Neither the first two args are evaluated NOR either of the two symbols
dnl is no qsort_r function is detected.
AC_DEFUN([JSS_CHECK_QSORT_R],[
  AC_CHECK_FUNC([qsort_r],[
    AC_DEFINE([HAVE_QSORT_R],[1],[whether or not qsort_r is available])
    js_have_qsort_r=yes],[js_have_qsort_r=no])
  AS_VAR_IF([js_have_qsort_r],[no],[m4_ifval([$3],[$3],[:])],[
  AC_MSG_CHECKING([that qsort_r is the BSD version])
    AC_LANG_PUSH([C])
    AC_RUN_IFELSE([
      AC_LANG_PROGRAM([[
#include <stdio.h>
#include <stdlib.h>

int unsorted[16]={1,3,5,7,9,11,13,15,2,4,6,8,10,12,14,16};
int bsd_sort_compare(void *a, const void *b, const void *c)
{
  const int *p1, *p2;
  if(a != (void *)unsorted) exit(2);
  p1 = (const int *)b;
  p2 = (const int *)c;
  if(*p1 > *p2) return 1;
  else if(*p2 > *p1) return -1;
  return 0;
}
]],[[
  int i1;
  qsort_r(unsorted, 16, sizeof(unsorted[0]), (void *)unsorted,
          bsd_sort_compare);
  for(i1; i1 < 16; i1++)
    if(unsorted[i1 - 1] + 1 != unsorted[i1]) exit(3);
  return 0;
]])],[AC_MSG_RESULT([yes])
      m4_ifvaln([$1],[$1],[AC_DEFINE([QSORT_R_BSD],[1],[Define to 1 if your platform users the BSD style of qsort_r()])])dnl
     ],
     [AC_MSG_RESULT([no])
      m4_ifvaln([$2],[$2],[AC_DEFINE([QSORT_R_GNUC],[1],[Define to 1 if your platform uses the GNUC style of qsort_r()])])dnl
     ])
AC_LANG_POP([C])])
])dnl
