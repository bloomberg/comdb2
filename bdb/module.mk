# Local defs
bdb_MEMGEN=bdb/mem_bdb.h
bdb_SOURCES:=bdb/callback.c bdb/rep.c bdb/bdb.c				\
    bdb/bdb_schemachange.c bdb/tranread.c bdb/threads.c bdb/count.c	\
    bdb/summarize.c bdb/del.c bdb/llog_auto.c bdb/cursor_rowlocks.c	\
    bdb/bdb_access.c bdb/bdb_verify.c bdb/upd.c bdb/genid.c		\
    bdb/fstdump.c bdb/rowlocks.c bdb/bdb_osqlbkfill.c bdb/compact.c	\
    bdb/compress.c bdb/cursor_ll.c bdb/rowlocks_util.c bdb/odh.c	\
    bdb/util.c bdb/temphash.c bdb/bdb_thd_io.c bdb/error.c		\
    bdb/serializable.c bdb/ll.c bdb/bdb_osqltrn.c	\
    bdb/temptable.c bdb/add.c bdb/tran.c bdb/os_namemangle.c		\
    bdb/lite.c bdb/bdb_net.c bdb/locks.c bdb/bdb_osqllog.c bdb/file.c	\
    bdb/llmeta.c bdb/queue.c bdb/custom_recover.c bdb/info.c		\
    bdb/bdb_osqlcur.c bdb/cursor.c bdb/fetch.c bdb/read.c bdb/phys.c	\
    bdb/bdblock.c bdb/attr.c bdb/locktest.c bdb/berktest.c		\
    bdb/bdb_llops.c bdb/bdb_blkseq.c bdb/queuedb.c
bdb_GENSOURCES:=bdb/llog_auto.c
bdb_GENOBJS:=$(bdb_GENSOURCES:.c=.o)
bdb_OBJS:=$(bdb_SOURCES:.c=.o) $(bdb_GENOBJS)

# Defined in the top level makefile
ARS+=bdb/libbdblib.a
OBJS+=$(bdb_OBJS)
GENC+=$(bdb_GENSOURCES)
GENH+=$(bdb_MEMGEN)
GENH+=$(bdb_GENSOURCES:.c=.h) $(bdb_GENSOURCES:auto.c=int.h)	\
$(bdb_GENSOURCES:auto.c=extern.h)
GENMISC+=bdb/template bdb/extern.list bdb/int.list

bdb_CPPFLAGS+=-DBERKDB_4_2 -I$(SRCHOME)/bdb -I$(SRCHOME)/crc32c	\
-I$(SRCHOME)/berkdb -I$(SRCHOME)/berkdb/build			\
-I$(SRCHOME)/berkdb/dbinc -I$(SRCHOME)/berkdb/dbinc_auto	\
-I$(SRCHOME)/db -I$(SRCHOME)/net -I$(SRCHOME)/dlmalloc		\
-I$(SRCHOME)/comdb2rle -I$(SRCHOME)/protobuf -I$(SRCHOME)/cson	\
-I$(SRCHOME)/bb -I$(SRCHOME) $(OPTBBINCLUDE)

# Custom defines
bdb/libbdblib.a: CPPFLAGS+=$(bdb_CPPFLAGS)

bdb/libbdblib.a: $(bdb_OBJS)
	$(AR) $(ARFLAGS) $@ $^

# This directory depends on generated h from berkdb and protobuf
$(bdb_OBJS): $(berkdb_MEMGEN) $(BERKDB_EXT_H) protobuf/sqlresponse.pb-c.c 

$(bdb_OBJS): bdb/llog_auto.h bdb/llog_auto.c
$(bdb_OBJS): $(bdb_MEMGEN)

bdb/llog_auto.h: bdb/llog_auto.c

bdb/llog_auto.c: bdb/llog.src
	bdb/build_recfiles.sh $(<F) bdb berkdb
