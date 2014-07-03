builtin(include, build/m4/compat.m4)dnl
AC_PREREQ([2.63])
AC_INIT(zeke,1.1.0)
AC_CONFIG_AUX_DIR(build)
AC_CONFIG_MACRO_DIR(build)
AC_CONFIG_SRCDIR([src])
AC_CANONICAL_TARGET
AM_INIT_AUTOMAKE([subdir-objects])
AM_MAINTAINER_MODE

AC_PROG_CC
LT_INIT
AC_PROG_INSTALL
AC_LANG_C
AM_PROG_CC_C_O
PKG_PROG_PKG_CONFIG([0.23])

AC_CONFIG_LIBOBJ_DIR([lib])

AC_ARG_VAR(ZK_INCDIR,[libzookeeper include directory])
AC_ARG_VAR(ZK_LIBDIR,[libzookeeper lib directory])
AC_ARG_VAR(APR_LIBS,[Apache Portable Runtime libraries])
AC_ARG_VAR(APR_UTIL_LIBS,[Apache Portable Runtime Utility libaries])
AC_ARG_VAR(APR_CFLAGS,[Apache Portable Runtime CFLAGS])
AC_ARG_VAR(APR_CPPFLAGS,[Apache Portable Runtime CPPFLAGS])
AC_ARG_VAR(DEBUG_CFLAGS,[additional debugging CFLAGS])

# libtool "age"
LIBZEKE_ABI_COMPAT=0
# libtool "current"
LIBZEKE_ABI_MAJOR=3
# libtool "rev"
LIBZEKE_REVISION=0

LIBZEKE_API_VERSION=3

dnl PLATFORM_CFLAGS="-fno-strict-aliasing"
ZEKE_EXTRA_CPPFLAGS="-DHAVE_LIBZEKE_CONFIG_H"
ZK_TOOL_LIBS=
ZK_LINK=
ZK_INCLUDE=
ZK_REQS=
ZK_CPPFLAGS=

LIBZEKE_VERSION="\"${LIBZEKE_API_VERSION}.${LIBZEKE_REVISION}\""
ZEKE_VERSION="AC_PACKAGE_VERSION"

AS_IF([test x"$ZK_INCDIR" != x""],
  [zeke_try_CPPFLAGS="-I$ZK_INCDIR"],
  [zeke_try_CPPFLAGS=])
AS_IF([test x"$ZK_LIBDIR" != x""],
  [zeke_try_LDFLAGS="-L$ZK_LIBDIR"],
  [zeke_try_LDFLAGS=])

zeke_zookeeper_include=
zeke_zookeeper_prefix_include="yes"
zeke_cf_debug=unset
zeke_cf_extra_debug=
zeke_cf_debugging_macro=no

AC_ARG_ENABLE(zkjobd-security,
  AC_HELP_STRING([--disable-zkjobd-security],
    [Disable restriction that prohibits zkjobd from executing jobs with the 'root' user or group assigned]),
    [zkjobd_cf_prohibit_root_jobs="${enableval:-yes}"],
    [zkjobd_cf_prohibit_root_jobs=yes])

AC_ARG_ENABLE(debug,
  AC_HELP_STRING([--enable-debug],
    [Enable runtime logging of debug messages]),
    [zeke_cf_debug="${enableval:-yes}"],[])
AC_ARG_WITH(zookeeper,
  AC_HELP_STRING([--with-zookeeper@<:@=PATH@:>@],
    [Specify location of libzookeeper installation]),
    [],
    [with_zookeeper=unset])dnl

AC_ARG_WITH(tmpdir,
  AC_HELP_STRING([--with-tmpdir@<:@=PATH@:>@],
    [Specify temporary directory to be used for script execution, default is $localstatedir/tmp]),
    [zkjobq_exec_tmpdir="$with_tmpdir"],
    [zkjobq_exec_tmpdir=no])dnl
AS_CASE($zkjobq_exec_tmpdir,
  [no],[ZKJOBQ_EXEC_TMPDIR='${localstatedir}/tmp'],
  [yes],[ZKJOBQ_EXEC_TMPDIR='${localstatedir}/tmp'],
  [ZKJOBQ_EXEC_TMPDIR="$zkjobq_exec_tmpdir"])dnl

AC_ARG_WITH(logdir,
  AC_HELP_STRING([--with-logdir@<:@=PATH@:>@],
    [Specify directory to be used for standard and/or shared memory logging, default is $localstatedir/log]),
    [zeke_cf_logdir="${with_logdir:-yes}"],
    [zeke_cf_logdir=no])dnl
AS_CASE($zeke_cf_logdir,
  [no],[ZEKE_LOGDIR='${localstatedir}/log'],
  [yes],[ZEKE_LOGDIR='${localstatedir}/log'],
  [ZEKE_LOGDIR="$zeke_cf_logdir"])dnl

AC_ARG_WITH(libevent,
  AC_HELP_STRING([--with-libevent],
    [Link against libevent to support additional libevent based network I/O API functionality]),
    [zeke_cf_libevent="${enableval:-yes}"],
    [zeke_cf_libevent=no])

AC_ARG_ENABLE(debug-gdb,
  AC_HELP_STRING([--enable-debug-gdb],
    [Disable all optimization and output full debugging symbols]),
    [AS_VAR_IF([zeke_cf_debug],[yes],[zeke_cf_extra_debug=yes])
     zeke_cf_debug="gdb"],[])dnl
AC_ARG_ENABLE(install-pkgconfig,
  AC_HELP_STRING([--disable-install-pkgconfig],
    [Don't install libzeke.pc file for use with pkg-config]))dnl
AC_ARG_ENABLE(threading,
  AC_HELP_STRING([--enable-threading],
    [Enable thread support (requires threading in libapr as well)]))

AS_IF([test x"$with_zookeeper" = x"unset" && test x"$ZK_INCDIR" = x"" && test x"$ac_default_prefix" != x"$prefix" && test x"$prefix" != x"NONE"],
  [with_zookeeper="${prefix:-unset}"
   zeke_zookeeper_prefix_include="${prefix:+yes}"])

AS_CASE($enable_threading,
  [yes],
    [zookeeper_library_name="zookeeper_mt"
     zeke_cf_use_threads=yes],
  [single],
    [zookeeper_library_name="zookeeper_st"
     ZEKE_EXTRA_CPPFLAGS="$ZEKE_EXTRA_CPPFLAGS${ZEKE_EXTRA_CPPFLAGS:+ }-DZEKE_SINGLE_THREADED"
     zeke_cf_use_threads=yes],
  [zookeeper_library_name="zookeeper_st"
   zeke_cf_use_threads=no])

AS_VAR_IF([zeke_cf_use_threads],[yes],
  [zeke_cf_libevent_name=libevent_pthreads
  AC_DEFINE([ZEKE_USE_THREADS],[1],[Define to enable special multi-threading support])],
  [zeke_cf_libevent_name=libevent])

AM_CONDITIONAL([INSTALL_PKGCONFIG],[test x"$enable_install_pkgconfig" != x"no"])

AS_IF([test x"$zeke_cf_debug" = x"unset"],
  [AM_SILENT_RULES([yes])],
  [enable_shared=no
   AM_SILENT_RULES([no])])

AS_CASE($with_zookeeper,
  [no],[AC_MSG_ERROR([but .. but .. but .. I NEED zookeeper! NEED IT DAMN YOU!])],
  [unset],,
  [AS_IF([test -d "$with_zookeeper/include/zookeeper"],
         [zeke_try_CPPFLAGS="-I$with_zookeeper/include -I$with_zookeeper/include/zookeeper"
         AS_IF([test x"$zeke_zookeeper_prefix_include" = x"yes"],
               [zeke_zookeeper_include="$with_zookeeper/include/zookeeper"])],
         [test -d "$with_zookeeper/zookeeper"],
         [zeke_try_CPPFLAGS="-I$with_zookeeper/zookeeper"
         AS_IF([test x"$zeke_zookeeper_prefix_include" = x"yes"],
               [zeke_zookeeper_include="$with_zookeeper/zookeeper"])],
         [test -d "$with_zookeeper/include"],
         [zeke_try_CPPFLAGS="-I$with_zookeeper/include"
         AS_IF([test x"$zeke_zookeeper_prefix_include" = x"yes"],
               [zeke_zookeeper_include="$with_zookeeper/include"])],
         [zeke_try_CPPFLAGS="-I$with_zookeeper"])
   AS_IF([test -d "$with_ookeeper/lib64"],
         [zeke_try_LDFLAGS="-L$with_zookeeper/lib64"],
         [test -d "$with_zookeeper/lib"],
         [zeke_try_LDFLAGS="-L$with_zookeeper/lib"],
         [zeke_try_LDFLAGS="-L$with_zookeeper"])
  ])

AS_CASE($zeke_cf_debug,
  [gdb],[CFLAGS="-ggdb3 -Wall"],
  [yes],[CFLAGS="$CFLAGS${CFLAGS:+ }-ggdb"
         ZEKE_EXTRA_CPPFLAGS="$ZEKE_EXTRA_CPPFLAGS${ZEKE_EXTRA_CPPFLAGS+ }-DDEFAULT_UUID_NON_EPHEMERAL"
         zeke_cf_debugging_macro=yes],
  [unset],,
  [
   CFLAGS="$CFLAGS${CFLAGS:+ } -g$zeke_cf_debug"
   zeke_cf_debugging_macro=yes
  ])

AS_VAR_IF([zeke_cf_extra_debug],[yes],
 [zeke_cf_debugging_macro=yes
  AS_VAR_IF([APR_CFLAGS],[],[APR_CFLAGS=-Wno-unused-function])
  ZEKE_EXTRA_CPPFLAGS="$ZEKE_EXTRA_CPPFLAGS${ZEKE_EXTRA_CPPFLAGS+ }-Wno-unused-function"
])

AS_IF([test x"$zeke_cf_debugging_macro" = x"yes"],
   [AC_DEFINE([DEBUGGING],[1],[Define to enable emitting debug messages])])

ZEKE_CHECK_GCC_VISIBILITY
ZEKE_CHECK_LIBC_PROGNAME
ZEKE_CHECK_ALIAS_ATTRIBUTE

PKG_CHECK_MODULES([PCRE],[libpcre >= 6.5])

AS_VAR_IF([zeke_cf_libevent],[no],[],[
  PKG_CHECK_MODULES([LIBEVENT],[$zeke_cf_libevent_name >= 2.0.0],
    [AC_DEFINE([LIBZEKE_USE_LIBEVENT],[1],
               [Define to provide libevent-based network I/O API])
    zeke_cf_libevent=yes],
    [AC_MSG_ERROR([cannot find libevent])])
])dnl

dnl we don't use pkg-config for this because it's difficult to precisely control
dnl linkage of additional libs beyond apr/apr-util (ldap, etc) with pkg-config.
APR_FIND_APR([],[],[1],[1])
AS_IF([test x"$apr_found" != x"yes"],
  [AC_MSG_ERROR([cannot find libapr])],
  [[zeke_apr_version="`$apr_config --version`"
   zeke_apr_major="`$apr_config --version | sed 's/^\\([0-9]\\{1,\}\\)\\..*\$/\\1/'`"]])

APR_FIND_APU([],[],[1],[1])
AS_IF([test x"$apu_found" != x"yes"],
  [AC_MSG_ERROR([cannot find libapr-util])],
  [[zeke_apu_version="`$apu_config --version`"
   zeke_apu_major="`$apu_config --version | sed 's/^\\([0-9]\\{1,\\}\\)\\..*\$/\\1/'`"]]
  [AS_VAR_IF([zeke_apu_major],[$zeke_apr_major],[],
    [AC_MSG_ERROR([Version mismatch between libapr-$zeke_apr_major and libapr-util-$zeke_apu_major])])
  ])dnl

ZK_LINK="`$apr_config --ldflags --link-ld --libs` `$apu_config --ldflags --link-ld`"
ZK_INCLUDE="-I`$apr_config --includedir`"
ZK_CPPFLAGS="$ZEKE_EXTRA_CPPFLAGS `$apr_config --cppflags`"

AS_IF([test x"$APR_CPPFLAGS" = x""],
  [APR_CPPFLAGS="`$apr_config --cppflags` $ZK_INCLUDE"])
AS_IF([test x"$APR_CFLAGS" = x""],
  [APR_CFLAGS="`$apr_config --cflags`"])
AS_IF([test x"$APR_LIBS" = x""],
  [APR_LIBS="`$apr_config --ldflags --link-ld --libs`"])
AS_IF([test x"$APR_UTIL_LIBS" = x""],
  [APR_UTIL_LIBS="`$apu_config --ldflags --link-ld`"])

dnl libapr and libaprutil
AS_IF([test x"$APR_CPPFLAGS" != x""],
  [CPPFLAGS="$CPPFLAGS${CPPFLAGS+ }$APR_CPPFLAGS"],[])
AS_IF([test x"$APR_CFLAGS" != x""],
  [CFLAGS="$CFLAGS${CFLAGS+ }$APR_CFLAGS"],[])
AS_IF([test x"$APR_LIBS" != x""],
  [ZK_TOOL_LIBS="$ZK_TOOL_LIBS${ZK_TOOL_LIBS+ }$APR_LIBS"],[])
AS_IF([test x"$APR_UTIL_LIBS" != x""],
  [ZK_TOOL_LIBS="$ZK_TOOL_LIBS${ZK_TOOL_LIBS+ }$APR_UTIL_LIBS"],[])

dnl libpcre
AS_IF([test x"$PCRE_CFLAGS" != x""],
  [CFLAGS="$CFLAGS${CFLAGS+ }$PCRE_CFLAGS"],[])
AS_IF([test x"$PCRE_LIBS" != x""],
  [ZK_TOOL_LIBS="$ZK_TOOL_LIBS${ZK_TOOL_LIBS+ }$PCRE_LIBS"],[])

dnl libevent
AS_VAR_IF([zeke_cf_libevent],[yes],[
  AS_VAR_IF([LIBEVENT_CFLAGS],[],[],
    [CFLAGS="$CFLAGS${CFLAGS+ }$LIBEVENT_CFLAGS"])
  AS_VAR_IF([LIBEVENT_LIBS],[],[],
    [ZK_TOOL_LIBS="$ZK_TOOL_LIBS${ZK_TOOL_LIBS+ }$LIBEVENT_LIBS"])
  JSS_PKG_MOD_VERSION([LIBEVENT_VERSION],[$zeke_cf_libevent_name >= 2.0.0],
    [ZK_REQS="$ZK_REQS${ZK_REQS+ }$zeke_cf_libevent_name >= $LIBEVENT_VERSION"],
    [AC_MSG_ERROR([libevent version is ][["$LIBEVENT_VERSION"]])])
])dnl

AC_STDC_HEADERS
AC_HEADER_ASSERT
AC_CHECK_HEADERS([signal.h assert.h math.h])
AS_VAR_IF([zeke_cf_libevent],[yes],
  [
   AS_VAR_IF([LIBEVENT_CFLAGS],[],[],
    [zeke_libevent_save_CPPFLAGS="$CPPFLAGS"
     CPPFLAGS="$CPPFLAGS${CPPFLAGS+ }$LIBEVENT_CFLAGS"])
   AC_CHECK_HEADERS([event2/thread.h])
   AS_VAR_IF([LIBEVENT_CFLAGS],[],[],
    [CPPFLAGS="$zeke_libevent_save_CPPFLAGS"])
  ])
AC_FUNC_ALLOCA
AC_FUNC_STRTOLD
AC_CHECK_FUNCS_ONCE([strtod funopen fopencookie])

AS_IF([test x"$ac_cv_func_alloca_works" != x"yes"],
  [AC_MSG_ERROR([this system does not support alloca(), yet a compatible implementation is not included with this distribution])])

AS_IF([test x"$zeke_try_CPPFLAGS" != x""],
  [zeke_save_CPPFLAGS="$CPPFLAGS"
  CPPFLAGS="$CPPFLAGS $zeke_try_CPPFLAGS"
  AC_CHECK_HEADER(zookeeper.h,
    [zeke_CPPFLAGS="$zeke_CPPFLAGS${zeke_CPPFLAGS:+ }$zeke_try_CPPFLAGS"],
    [AC_MSG_FAILURE([no zookeeper bad])])
  CPPFLAGS="$zeke_save_CPPFLAGS"
  ],
  [AC_CHECK_HEADER(zookeeper.h,,
      [AC_MSG_FAILURE([unable to find zookeeper header files, perhaps you should try ZK_INCDIR=...?])])
  ])

AS_IF([test x"$zeke_try_LDFLAGS" != x""],
  [zeke_save_LDFLAGS="$LDFLAGS"
  LDFLAGS="$LDFLAGS $zeke_try_LDFLAGS"
  AC_CHECK_LIB($zookeeper_library_name,zookeeper_init,
    [zeke_LDFLAGS="$zeke_LDFLAGS${zeke_LDFLAGS:+ }$zeke_try_LDFLAGS"
     ZK_TOOL_LIBS="${ZK_TOOL_LIBS}${ZK_TOOL_LIBS:+ }-l$zookeeper_library_name"],
    [AC_MSG_FAILURE([cannot link with lib$zookeeper_library_name library])])
  LDFLAGS="$zeke_save_LDFLAGS"
  ],
  [AC_CHECK_LIB($zookeeper_library_name,zookeeper_init,
    [ZK_TOOL_LIBS="${ZK_TOOL_LIBS}${ZK_TOOL_LIBS:+ }-l$zookeeper_library_name"],
    [AC_MSG_FAILURE([cannot find $zookeeper_library_name library, perhaps you should try ZK_LIBDIR=...?])])
  ])

AS_IF([test x"$zeke_CPPFLAGS" != x""],
  [CPPFLAGS="$CPPFLAGS${CPPFLAGS:+ }$zeke_CPPFLAGS"
   CFLAGS="$CFLAGS${CFLAGS:+ }$zeke_CPPFLAGS"])

AS_IF([test x"$zeke_LDFLAGS" != x""],
  [LDFLAGS="${LDFLAGS}${LDFLAGS:+ }$zeke_LDFLAGS"])

zeke_save_LIBS="$LIBS"
LIBS="$LIBS${LIBS:+ }$ZK_TOOL_LIBS"
AC_REPLACE_FUNCS([apr_table_clone apr_array_clear])
JSS_CHECK_QSORT_R([],[],[AC_LIBOBJ([qsort_r])])
LIBS="$zeke_save_LIBS"

AS_IF([test x"$zeke_zookeeper_include" != x""],
  [ZK_INCLUDE="$ZK_INCLUDE${ZK_INCLUDE:+ }-I$zeke_zookeeper_include"])

AC_RUN_IFELSE(
  [AC_LANG_PROGRAM([[#include <apr_version.h>
                     #include <stdio.h>]],
                   [[FILE *fp = fopen("conftest.out","w");
                     fprintf(fp,"%u\n%u\n%u",APR_MAJOR_VERSION,
                                         APR_MINOR_VERSION,
                                         APR_PATCH_VERSION);]])],
  [eval `
    cat conftest.out | {
    if read x; then
      echo APR_MAJOR_VERSION=\$x
      if read x; then
        echo APR_MINOR_VERSION=\$x
        if read x; then
          echo APR_PATCH_VERSION=\$x
        else
          false
        fi
      else
        false
      fi
    else
      false
    fi }
  `],[AC_MSG_FAILURE([Cannot determine Apache Portable Runtime version.])])

AS_IF([test 0"$APR_MAJOR_VERSION" -eq 1],
  [AS_IF([test 0"$APR_MINOR_VERSION" -gt 2],
    [AC_DEFINE([USING_APR_13],[1],[Define if you are building against the Apache Portable Runtime Version 1.3 or higher.])dnl
    ],[test 0"$APR_MINOR_VERSION" -eq 2],
    [AC_DEFINE([USING_APR_12],[1],[Define if you are building against the Apache Portable Runtime Version 1.2.])],
    [AC_MSG_ERROR([Unsupported minor version of the Apache Portable Runtime, require version 1.2.0 or higher.])dnl
  ])],
  [AC_MSG_ERROR([Unsupported major version of the Apache Portable Runtime, require version 1.2/1.3/1.x.])dnl
])

AS_VAR_IF([zkjobd_cf_prohibit_root_jobs],[yes],
  [AC_DEFINE([ZKJOBD_PROHIBIT_ROOT_JOBS], [1], [Define if you wish to disallow executing any job as the root user or group])])dnl

AC_SUBST([ZEKE_LOGDIR])
AC_SUBST([ZKJOBQ_EXEC_TMPDIR])
AC_SUBST([ZEKE_EXTRA_CPPFLAGS])
AC_SUBST([PLATFORM_CFLAGS])

AC_SUBST([LIBEVENT_VERSION])
AC_SUBST([LIBZEKE_ABI_MAJOR])
AC_SUBST([LIBZEKE_ABI_COMPAT])
AC_SUBST([LIBZEKE_API_VERSION])
AC_SUBST([LIBZEKE_REVISION])
AC_SUBST([LIBZEKE_VERSION])
AC_SUBST([ZEKE_VERSION])
AC_SUBST([ZK_TOOL_LIBS])
AC_SUBST([ZK_LINK])
AC_SUBST([ZK_INCLUDE])
AC_SUBST([ZK_CPPFLAGS])
AC_SUBST([ZK_REQS])

AM_CONFIG_HEADER([include/zeke/libzeke_config.h])

AC_OUTPUT([Makefile
          tools/Makefile
          zkjobs/Makefile
          include/zeke/libzeke_version.h 
          include/internal_config.h
          src/libzeke.pc])
