ACLOCAL_AMFLAGS = -I build
AM_CPPFLAGS = @ZEKE_EXTRA_CPPFLAGS@ -I$(top_srcdir)/include -DZEKE_LOGDIR="\"@ZEKE_LOGDIR@\""
AM_CFLAGS = $(DEBUG_CFLAGS) @PLATFORM_CFLAGS@ -Wall
LIBZEKE_ABI_MAJOR = @LIBZEKE_ABI_MAJOR@
LIBZEKE_ABI_COMPAT = @LIBZEKE_ABI_COMPAT@
LIBZEKE_API_VERSION = @LIBZEKE_API_VERSION@
LIBZEKE_REVISION = @LIBZEKE_REVISION@

ZEKEINC = $(top_srcdir)/include/zeke
EXTRA_DIST = LICENSE contrib/* build/m4/*.m4 include/internal_config.h.in

SUBDIRS = . tools zkjobs

if INSTALL_PKGCONFIG
pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = src/libzeke.pc
endif

lib_LTLIBRARIES = libzeke.la
noinst_LTLIBRARIES = libzeke-static.la libcompat.la

noinst_PROGRAMS = zeked

libzeke_la_includedir = $(includedir)/zeke
libzeke_la_include_HEADERS = $(ZEKEINC)/libzeke.h $(ZEKEINC)/libzeke_version.h \
							 $(ZEKEINC)/libzeke_config.h \
							 $(ZEKEINC)/libzeke_errno.h \
							 $(ZEKEINC)/libzeke_event.h \
							 $(ZEKEINC)/libzeke_event_loop.h \
							 $(ZEKEINC)/libzeke_session.h \
							 $(ZEKEINC)/libzeke_indirect.h \
							 $(ZEKEINC)/libzeke_regex.h \
							 $(ZEKEINC)/libzeke_trans.h \
							 $(ZEKEINC)/libzeke_util.h \
							 $(ZEKEINC)/libzeke_nodedir.h \
							 $(ZEKEINC)/libzeke_uuid.h \
							 $(ZEKEINC)/libzeke_hooks.h \
							 $(ZEKEINC)/libzeke_connection_hooks.h \
							 $(ZEKEINC)/libzeke_log.h \
							 $(ZEKEINC)/libzeke_tree.h

libzeke_la_LDFLAGS = -version-info $(LIBZEKE_ABI_MAJOR):$(LIBZEKE_REVISION):$(LIBZEKE_ABI_COMPAT)

libcompat_la_SOURCES = include/compat.h lib/stub.c
libcompat_la_LIBADD = @LTLIBOBJS@

libzeke_la_SOURCES = src/io.c src/dispatch.c \
					 src/event.c include/event_private.h \
					 src/util.c include/util.h \
					 src/errors.c src/init.c src/re.c src/log.c \
					 include/dispatch.h include/internal.h include/internal_config.h \
					 include/io.h include/compat.h \
					 src/indirect.c \
					 src/connection.c include/connection.h \
					 src/session.c \
					 src/hooks.c include/hooks.h \
					 src/transaction.c include/transaction.h \
					 src/nodedir.c src/uuid.c src/tree.c

libzeke_la_LIBADD = libcompat.la

libzeke_static_la_SOURCES = $(libzeke_la_SOURCES)
libzeke_static_la_LIBADD = libcompat.la

zeked_SOURCES = src/zeked.c
zeked_CFLAGS = -Wno-unused-function
zeked_LDADD = @ZK_TOOL_LIBS@ libzeke-static.la

