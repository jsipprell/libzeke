ACLOCAL_AMFLAGS = -I $(top_srcdir)/build
AM_CPPFLAGS = @ZEKE_EXTRA_CPPFLAGS@ -I$(top_srcdir)/include
AM_CFLAGS = $(DEBUG_CFLAGS) @PLATFORM_CFLAGS@ -Wall -Wno-unused-function

noinst_HEADERS = tools.h

COMMON_SRCS = cli.c timeout.c index.c vxid.c

noinst_LTLIBRARIES = libzktool.la

libzktool_la_SOURCES = $(COMMON_SRCS) $(noinst_HEADERS)
libzktool_la_LIBADD = $(top_srcdir)/libzeke.la

bin_PROGRAMS = zklock zkelection zktree

zklock_SOURCES = zklock.c $(noinst_HEADERS)
zklock_LDADD = $(top_srcdir)/libzeke.la libzktool.la @ZK_TOOL_LIBS@

zkelection_SOURCES = zkelection.c $(noinst_HEADERS)
zkelection_LDADD = $(top_srcdir)/libzeke.la libzktool.la @ZK_TOOL_LIBS@

zktree_SOURCES = zktree.c $(noinst_HEADERS)
zktree_LDADD = $(top_srcdir)/libzeke.la libzktool.la @ZK_TOOL_LIBS@

