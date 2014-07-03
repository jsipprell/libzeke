ACLOCAL_AMFLAGS = -I $(top_srcdir)/build
AM_CPPFLAGS = @ZEKE_EXTRA_CPPFLAGS@ -I$(top_srcdir)/tools -I$(top_srcdir)/include -DEXEC_TMPDIR="\"@ZKJOBQ_EXEC_TMPDIR@\""
AM_CFLAGS = $(DEBUG_CFLAGS) @PLATFORM_CFLAGS@ -Wall -Wno-unused-function
COMMON_SOURCES = workspace.c flags.c

EXTRA_DIST = README.zkjobs
TOOLS_DIR = $(top_srcdir)/tools

noinst_HEADERS = zkjobs.h
sbin_PROGRAMS = zkjobd
bin_PROGRAMS = zkjobc zkjobq

zkjobd_SOURCES = zkjobd.c proc.c election.c queue.c $(COMMON_SOURCES)
zkjobd_LDADD = $(top_srcdir)/libzeke.la $(TOOLS_DIR)/libzktool.la @ZK_TOOL_LIBS@

zkjobc_SOURCES = zkjobc.c $(COMMON_SOURCES)
zkjobc_LDADD = $(top_srcdir)/libzeke.la $(TOOLS_DIR)/libzktool.la @ZK_TOOL_LIBS@

zkjobq_SOURCES = zkjobq.c $(COMMON_SOURCES)
zkjobq_CFLAGS = -DZEKE_LOGDIR="\"@ZEKE_LOGDIR@\""
zkjobq_LDADD = $(top_srcdir)/libzeke.la $(TOOLS_DIR)/libzktool.la @ZK_TOOL_LIBS@
