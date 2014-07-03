AC_DEFUN([ZEKE_CHECK_LIBC_PROGNAME],
  [AC_CACHE_CHECK([whether libc has the __progname and __progname_full symbols],
    [ac_cv_libc__progname],
    [AC_LINK_IFELSE([
      AC_LANG_PROGRAM([
        extern const char *__progname;
        extern const char *__progname_full;
      ],[
        __progname = strdup("test");
        __progname_full = strdup(__progname);
        return 0;
      ])],
    [ac_cv_libc__progname=yes],[ac_cv_libc__progname=no])])dnl
    
  if test x"$ac_cv_libc__progname" = x"yes"; then
    AC_DEFINE([HAVE_LIBC_PROGNAME],[1],
      [Define to 1 if your libc uses the __progname and __progname_full symbols.])
  fi
  ])dnl
    
