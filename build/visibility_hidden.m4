AC_DEFUN([_ZEKE_CHECK_GCC_VISIBILITY],
  [if test x"$_zeke_vis_checked" = x""; then
    _zeke_vis_checked=1
    cat >conftest.c <<EOF
      int foo __attribute__ ((visibility ("hidden"))) = 1;
      int bar __attribute__ ((visibility ("default"))) = 1;
EOF
    _z_vis=no
    _z_vis_hidden=
    _z_vis_export=
    if ${CC-cc} -Werror -S conftest.c -o conftest.s >/dev/null 2>&1; then
      if grep '\.hidden.*foo' conftest.s >/dev/null; then
        _z_vis_hidden="__attribute__ ((visibility (\"hidden\")))"
      fi
      if grep '\.globl.*bar' conftest.s >/dev/null; then
        _z_vis_export="__attribute__ ((visibility (\"default\")))"
      fi
      _z_vis=yes
    fi

    ac_cv_have_visibility_attribute="${ac_cv_have_visibility_attribute:-${_z_vis}}"
    ac_cv_gcc_visibility_hidden="${ac_cv_gcc_visibility_hidden:-${_z_vis_hidden}}"
    ac_cv_gcc_visibility_export="${ac_cv_gcc_visibility_export:-${_z_vis_export}}"
  fi
  ])dnl

AC_DEFUN([_ZEKE_CHECK_DEFAULT_VISIBILITY],
  [AS_VAR_IF([_zeke_vis_default_checked],[],[
    _zeke_vis_default_checked=1
    cat >conftest.c <<EOF
      int foo = 1;
EOF
    _z_vis=no
    if ${CC-cc} -fvisibility=hidden -Werror -S conftest.c -o conftest.s >/dev/null 2>&1; then
      if grep '\.hidden.*foo' conftest.s >/dev/null; then
        _z_vis=yes
      fi
    fi

    ac_cv_cc_f_visibility="${ac_cv_cc_f_visibility:-${_z_vis}}"])])dnl

AC_DEFUN([ZEKE_CHECK_GCC_VISIBILITY],
  [AC_CACHE_CHECK([whether your C compiler supports -fvisibility],
      [ac_cv_cc_f_visibility], _ZEKE_CHECK_DEFAULT_VISIBILITY)
   AS_VAR_IF([ac_cv_cc_f_visibility],[yes],
     [m4_ifval([$1],[$1],[CFLAGS="$CFLAGS${CFLAGS+ }-fvisibility=hidden"])],
     [ac_cv_cc_f_visibility=no])
  AC_CACHE_CHECK([whether __attribute__((visibility())) is supported],
      [ac_cv_have_visibility_attribute], _ZEKE_CHECK_GCC_VISIBILITY)
    if test x"$ac_cv_have_visibility_attribute" = x"yes"; then
      AC_CACHE_CHECK([whether symbol hidding attribute works],
                    [ac_cv_gcc_visibility_hidden], _ZEKE_CHECK_GCC_VISIBILITY)
      AC_CACHE_CHECK([whether symbol exporting attribute works],
                    [ac_cv_gcc_visibility_export], _ZEKE_CHECK_GCC_VISIBILITY)
    else
      ac_cv_gcc_visibility_hidden=
      ac_cv_gcc_visibility_export=
    fi

    if test x"$ac_cv_have_visibility_attribute" = x"yes"; then
      AC_DEFINE_UNQUOTED([HAVE_VISIBILITY_ATTRIBUTE],[1],
        [Define to 1 if your C compilter supports the visibility __attribute__.])
      AC_DEFINE_UNQUOTED([VISIBILITY_HIDDEN],[$ac_cv_gcc_visibility_hidden],
        [Define to your C compiler's attribute for preventing an exported symbol being dynamically linkable.])
      AC_DEFINE_UNQUOTED([VISIBILITY_EXPORT],[$ac_cv_gcc_visibility_export],
        [Define to your C compiler's attribute for exporting symbols for dynamic linking.])
    fi
  ])dnl

