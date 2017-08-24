# Local defs
db_MEMGEN:=$(SRCHOME)/db/mem_uncategorized.h
db_SOURCES:=db/comdb2.c db/endian.c db/handle_buf.c db/sltdbt.c		\
    db/toblock.c db/eventlog.c db/toblob.c db/reqdebug.c		\
    db/rmtpolicy.c db/process_message.c db/appsock_handler.c		\
    db/glue.c db/tag.c db/record.c db/sigutil.c db/debug.c		\
    db/history.c db/trans.c db/db_util.c db/resource.c db/dbqueue.c	\
    db/sqlglue.c db/sqlsupport.c db/sqlinterfaces.c db/sqlexplain.c	\
    db/verify.c db/constraints.c db/plink_timestamp.o db/prefault.c	\
    db/prefault_toblock.c db/prefault_net.c db/prefault_readahead.c	\
    db/prefault_helper.c db/thrman.c db/pushlogs.c db/errstat.c		\
    db/socket_interfaces.c db/reqlog.c db/twophase.c db/sqloffload.c	\
    db/mtrap.c db/toproxy.c db/repl_wait.c db/utf8.c db/osqlsession.c	\
    db/osqlrepository.c db/osqlblockproc.c db/osqlcomm.c		\
    db/osqlcheckboard.c db/osqlshadtbl.c db/osqlsqlthr.c		\
    db/block_internal.c db/request_stats.c db/quantize.c		\
    db/memdebug.c db/dbglog_support.c db/osql_srs.c db/localrep.c	\
    db/sqlstat1.c db/sqlanalyze.c db/watchdog.c db/translistener.c	\
    db/osqlblkseq.c db/lrucache.c db/testcompr.c db/fdb_util.c		\
    db/fdb_fend.c db/fdb_boots.c db/fdb_bend.c db/fdb_bend_sql.c	\
    db/fdb_comm.c db/fdb_fend_cache.c db/fdb_access.c db/envstubs.c	\
    db/comdb2uuid.c db/printlog.c db/autoanalyze.c db/marshal.c		\
    db/sqllog.c db/llops.c db/rowlocks_bench.c db/plugin.c db/views.c	\
    db/views_cron.c db/views_persist.c db/trigger.c db/bpfunc.c		\
    db/ssl_bend.c db/db_tunables.c db/config.c db/sequences.c
db_OBJS:=$(db_SOURCES:.c=.o)

# Defined in the top level makefile
OBJS+=$(db_OBJS)
GENH+=$(db_MEMGEN)
TASKS+=$(SRCHOME)/comdb2

COMDB2_VERSION=-DCOMDB2_VERSION=\"2\"
db_CPPFLAGS:=-I$(SRCHOME)/db -DCOMDB2_DB_COMPILE -DMSPACES		\
-DSQLITE_BUILDING_FOR_COMDB2 $(COMDB2_VERSION) -I$(SRCHOME)/sqlite	\
-I$(SRCHOME)/sqlite/inline -I$(SRCHOME)/cdb2api $(CPPFLAGS)		\
-I$(SRCHOME)/bdb -I$(SRCHOME)/net -I$(SRCHOME)/csc2			\
-I$(SRCHOME)/datetime -I$(SRCHOME)/dlmalloc				\
-I$(SRCHOME)/dfp/decNumber -I$(SRCHOME)/dfp/dfpal -I$(SRCHOME)/lua	\
-I$(SRCHOME)/protobuf -I$(SRCHOME)/comdb2rle -I$(SRCHOME)/crc32c	\
$(SQLITE_FLAGS) -I$(SRCHOME)/berkdb/build -I$(SRCHOME)/berkdb/dbinc	\
-I$(SRCHOME)/berkdb/dbinc_auto -I$(SRCHOME)/schemachange		\
-I$(SRCHOME)/berkdb -I$(SRCHOME)/bbinc -I$(SRCHOME)/cson		\
-I$(SRCHOME)/bb -I$(SRCHOME)/sockpool -I.

VERSION?=$(shell dpkg-parsechangelog | grep Version | cut -d' ' -f2 | sed 's/-.*//')

SYSLIBS=$(BBSTATIC) -lssl -lcrypto -lz -llz4 -luuid -lprotobuf-c \
   $(BBDYN) -lpthread -lrt -lm -ldl

# Custom defines
$(SRCHOME)/comdb2: CPPFLAGS=$(db_CPPFLAGS)

$(SRCHOME)/comdb2: $(LIBS_BIN) $(db_OBJS) $(tools_LIBS)
	$(CC) -o $(SRCHOME)/comdb2 $(LCLFLAGS) $(LDFLAGS) $(db_OBJS) $(LCLLIBS) $(SYSLIBS) $(ARCHLIBS) $(tools_LIBS)

$(db_OBJS): $(db_MEMGEN)

# This directory depends on parse.h from sqlite
$(db_OBJS): sqlite/parse.h

# Needed since vebecompare.c is straight included
db/sqlglue.c: sqlite/inline/vdbecompare.c

# Plink - an object file that has an array of information such that we
# can find it in the resulting binary. Information is version, git
# subject, among other things.
db/plink_timestamp.c: db/build_constants
	db/build_constants > db/plink_timestamp.c

GENC+=db/plink_timestamp.c
