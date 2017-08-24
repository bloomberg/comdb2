# Local defs
SQLITE_MEMGEN:=sqlite/mem_sqlite.h
SQLITE_OBJS:=sqlite/analyze.o sqlite/attach.o sqlite/auth.o		\
	 sqlite/bitvec.o sqlite/btmutex.o sqlite/build.o		\
	 sqlite/callback.o sqlite/complete.o sqlite/ctime.o		\
	 sqlite/decimal.o sqlite/dbstat.o sqlite/delete.o		\
	 sqlite/dttz.o sqlite/treeview.o sqlite/expr.o sqlite/fault.o	\
	 sqlite/func.o sqlite/global.o sqlite/hash.o sqlite/insert.o	\
	 sqlite/legacy.o sqlite/loadext.o sqlite/main.o			\
	 sqlite/malloc.o sqlite/mem1.o sqlite/memjournal.o		\
	 sqlite/mutex.o sqlite/mutex_noop.o sqlite/mutex_unix.o		\
	 sqlite/os.o sqlite/os_unix.o sqlite/pragma.o			\
	 sqlite/prepare.o sqlite/printf.o sqlite/random.o		\
	 sqlite/resolve.o sqlite/rowset.o sqlite/select.o		\
	 sqlite/sqlite_tunables.o sqlite/status.o sqlite/table.o	\
	 sqlite/tokenize.o sqlite/trigger.o sqlite/update.o		\
	 sqlite/utf.o sqlite/util.o sqlite/vdbe.o sqlite/vdbeapi.o	\
	 sqlite/vdbeaux.o sqlite/vdbeblob.o sqlite/vdbemem.o		\
	 sqlite/vdbesort.o sqlite/vdbetrace.o sqlite/vtab.o		\
	 sqlite/walker.o sqlite/where.o sqlite/wherecode.o		\
	 sqlite/whereexpr.o sqlite/comdb2build.o sqlite/comdb2lua.o	\
	 sqlite/comdb2vdbe.o sqlite/md5.o

SQLITE_OBJS_GEN:=sqlite/parse.o sqlite/opcodes.o sqlite/inline/serialget.o sqlite/inline/memcompare.o sqlite/inline/vdbecompare.o

SQLITE_OBJS_EXT:=                   \
sqlite/ext/comdb2/tables.o          \
sqlite/ext/comdb2/columns.o         \
sqlite/ext/comdb2/keys.o            \
sqlite/ext/comdb2/keycomponents.o   \
sqlite/ext/comdb2/constraints.o     \
sqlite/ext/comdb2/tablesizes.o      \
sqlite/ext/comdb2/procedures.o      \
sqlite/ext/comdb2/users.o           \
sqlite/ext/comdb2/tablepermissions.o\
sqlite/ext/comdb2/triggers.o        \
sqlite/ext/comdb2/keywords.o        \
sqlite/ext/comdb2/limits.o          \
sqlite/ext/comdb2/tunables.o        \
sqlite/ext/comdb2/threadpools.o     \
sqlite/ext/comdb2/sequences.o        \
sqlite/ext/misc/completion.o        \
sqlite/ext/misc/series.o            \
sqlite/ext/misc/json1.o

sqlite/md5.o: CFLAGS+=-O2

SQLITE_GENC:=sqlite/parse.c sqlite/opcodes.c sqlite/inline/serialget.c	\
sqlite/inline/memcompare.c sqlite/inline/vdbecompare.c
SQLITE_GENH:=sqlite/parse.h sqlite/opcodes.h sqlite/keywordhash.h

sqlite_CPPFLAGS:=$(SQLITE_FLAGS)
sqlite_CPPFLAGS+=-Isqlite -Isqlite/inline -Isqlite/ext/comdb2	\
-Isqlite/ext/misc -Idatetime -Idfp/decNumber -Idfp/dfpal -Icsc2   	\
-Icdb2api -Ischemachange -Ibdb -Inet -Iprotobuf -Idlmalloc -Ilua -Idb	\
$(OPTBBINCLUDE)

SQLITE_TOOLS=sqlite/mkkeywordhash sqlite/lemon

# Defined in the top level makefile
ARS+=sqlite/libsqlite.a
OBJS+=$(SQLITE_OBJS) $(SQLITE_OBJS_GEN) $(SQLITE_OBJS_EXT) 
GENC+=$(SQLITE_GENC)
GENH+=$(SQLITE_MEMGEN) $(SQLITE_GENH)
GENMISC+=sqlite/parse.h.temp sqlite/parse.out
TASKS+=$(SQLITE_TOOLS)

# Custom defines
sqlite/libsqlite.a: CPPFLAGS+=$(sqlite_CPPFLAGS)
$(SQLITE_TOOLS): CPPFLAGS+=$(sqlite_CPPFLAGS)

sqlite/libsqlite.a: $(SQLITE_OBJS) $(SQLITE_OBJS_GEN) $(SQLITE_OBJS_EXT)
	$(AR) $(ARFLAGS) $@ $^

$(SQLITE_OBJS_GEN): $(SQLITE_MEMGEN) $(SQLITE_GENC) $(SQLITE_GENH)
$(SQLITE_OBJS_EXT): $(SQLITE_MEMGEN) $(SQLITE_GENC) $(SQLITE_GENH)
$(SQLITE_OBJS): $(SQLITE_MEMGEN) $(SQLITE_GENC) $(SQLITE_GENH)

sqlite/keywordhash.h: sqlite/mkkeywordhash
	$< > $@

sqlite/parse.c: sqlite/lemon sqlite/parse.y sqlite/lempar.c
	rm -rf sqlite/inline/; mkdir sqlite/inline
	sqlite/lemon $(SQLITE_FLAGS) sqlite/parse.y
	mv sqlite/parse.h sqlite/parse.h.temp
	tclsh sqlite/addopcodes.tcl sqlite/parse.h.temp > sqlite/parse.h
	cat sqlite/parse.h sqlite/vdbe.c | tclsh sqlite/mkopcodeh.tcl > sqlite/opcodes.h
	sort -n -b -k 3 sqlite/opcodes.h | tclsh sqlite/mkopcodec.tcl sqlite/opcodes.h > sqlite/opcodes.c

# This file depends on db/ which depends on protobuf
sqlite/parse.c: protobuf/sqlquery.pb-c.c protobuf/bpfunc.pb-c.c db/mem_uncategorized.h

sqlite/opcodes.h: sqlite/parse.c
sqlite/opcodes.c: sqlite/parse.c
sqlite/parse.h: sqlite/parse.c

sqlite/inline/memcompare.c:  sqlite/parse.c sqlite/vdbeaux.c sqlite/inline/serialget.c
	echo '#include "serialget.c"' > sqlite/inline/memcompare.c
	sed -n "/START_INLINE_MEMCOMPARE/,/END_INLINE_MEMCOMPARE/p" sqlite/vdbeaux.c >> sqlite/inline/memcompare.c

sqlite/inline/serialget.c: sqlite/parse.c sqlite/vdbeaux.c
	sed -n "/START_INLINE_SERIALGET/,/END_INLINE_SERIALGET/p" sqlite/vdbeaux.c > sqlite/inline/serialget.c

sqlite/inline/vdbecompare.c: sqlite/parse.c sqlite/inline/memcompare.c
	echo '/* This file, vdbecompare.c, is automatically generated from vdbeaux.c. See libsqlite.mk. */' > sqlite/inline/vdbecompare.c
	echo '#include "memcompare.c"' >> sqlite/inline/vdbecompare.c
	sed -n "/START_INLINE_VDBECOMPARE/,/END_INLINE_VDBECOMPARE/p" sqlite/vdbeaux.c >> sqlite/inline/vdbecompare.c




