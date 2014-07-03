dnl JSS_PKG_MOD_VERSION(VARIABLE, MODULE, [optional code])
dnl ----------------------------------------------------
dnl Stores the module version int the specified variable.
AC_DEFUN([JSS_PKG_MOD_VERSION],
[AC_REQUIRE([PKG_PROG_PKG_CONFIG])dnl

_PKG_CONFIG([$1], [modversion],[$2])
AS_VAR_COPY([$1], [pkg_cv_][$1])
AS_VAR_IF([$1], [""], [$4], [$3])
]) # JSS_PKG_MOD_VERSION
dnl
dnl JSS_PKG_REQUIRES(VARIABLE, MODULE,
dnl [code-if-found-and-not-empty],
dnl [code-if-not-found-or-empty], private/optional keyword)
dnl --------------------------------------------------
AC_DEFUN([JSS_PKG_REQUIRES],
[AC_REQUIRE([PKG_PROG_PKG_CONFIG])dnl

m4_pushdef([_jss_pkg_command],[print-requires][m4_ifval([$5],[-][$5],[])])
_PKG_CONFIG([$1], _jss_pkg_command,[$2])
AS_VAR_COPY([$1], ["][pkg_cv_][$1]["])
AS_VAR_IF([$1], [""], [$4], [$3])
m4_popdef([_jss_pkg_command])
]) # JSS_PKG_REQUIRES
dnl
dnl JSS_PKG_PROVIDES(VARIABLE, MODULE,
dnl [code-if-found-and-not-empty],
dnl [code-if-not-found-or-empty])
dnl --------------------------------------------------
AC_DEFUN([JSS_PKG_PROVIDES],
[AC_REQUIRE([PKG_PROG_PKG_CONFIG])dnl

_PKG_CONFIG([$1], [print-provides],[$2])
AS_VAR_COPY([$1], ["][pkg_cv_][$1]["])
AS_VAR_IF([$1], [""], [$4], [$3])
]) # JSS_PKG_REQUIRES
