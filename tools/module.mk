# Local defs
tools_TASKS:=cdb2sql comdb2sc cdb2sockpool comdb2ar pmux cdb2_dump	\
cdb2_stat cdb2_verify cdb2_printlog
lcl_TASKS:=$(foreach task,$(tools_TASKS),tools/$(task)/$(task))

$(tools_TASKS): $(lcl_TASKS) 
	@rm -f $@
	cp tools/$(@F)/$(@F) $@

tools_INCLUDE:=-I$(SRCHOME)/crc32c -I$(SRCHOME)/bbinc			\
-I$(SRCHOME)/cdb2api -I$(SRCHOME)/berkdb -I$(SRCHOME)/berkdb/build	\
-I$(SRCHOME)/dlmalloc -I$(SRCHOME)/sockpool $(OPTBBINCLUDE)

tools_SYSLIBS=$(BBSTATIC) -lprotobuf-c -lssl -lcrypto -llz4 $(BBDYN)	\
-lpthread -lrt -lm -lz $(ARCHLIBS)

tools_CPPFLAGS:=$(tools_INCLUDE) $(CPPFLAGS)
tools_LDFLAGS:=$(LDFLAGS) $(LCLFLAGS)
tools_LDLIBS:=$(LDLIBS) -ldl $(LCLLIBS) $(tools_SYSLIBS)

# Base rules for all objects
tools/%.o: tools/%.c tools/%.d $(LIBS_BIN)
	$(CC) $(DEPFLAGS) -g $(tools_CPPFLAGS) $(CFLAGS) -c -o $@ $<
	$(POSTCOMPILE)

tools/%.o: tools/%.cpp tools/%.d $(LIBS_BIN)
	$(CXX11) $(DEPFLAGS_CXX11) -g $(tools_CPPFLAGS) $(CXX11FLAGS) -c -o $@ $<
	$(POSTCOMPILE)

# Cdb2sql and Comdb2sc - These only have .c file each, though they do
# depend on some of the auto-generated .h from other moodules
# Cdb2sql and comdb2sc
tools/%: tools/%.o
	$(CC) $(tools_LDFLAGS) $^ $(tools_LDLIBS) -o $@

# Cdb2sockpool - Use base rules, multiple object files
cdb2sockpool_SOURCES:=utils.c settings.c cdb2sockpool.c
cdb2sockpool_OBJS:=$(patsubst %.c,tools/cdb2sockpool/%.o,$(cdb2sockpool_SOURCES))
cdb2sockpool_LDLIBS=-lbb -lsockpool

tools/cdb2sockpool/cdb2sockpool: $(cdb2sockpool_OBJS)

# Comdb2ar - Use base rules
comdb2ar_SOURCES:=appsock.cpp comdb2ar.cpp db_wrap.cpp		\
		   deserialise.cpp error.cpp fdostream.cpp	\
		   file_info.cpp logholder.cpp lrlerror.cpp	\
		   repopnewlrl.cpp riia.cpp serialise.cpp	\
		   serialiseerror.cpp tar_header.cpp util.cpp	\
		   chksum.cpp

comdb2ar_OBJS:=$(patsubst %.cpp,tools/comdb2ar/%.o,		\
	$(filter %.cpp,$(comdb2ar_SOURCES)))			\
	$(patsubst %.c,tools/comdb2ar/%.o,			\
	$(filter %.c,$(comdb2ar_SOURCES)))

# Must omit schemachange to avoid conflicts with berkdb_dum
comdb2ar_LDLIBS+= $(BBSTATIC) $(BBLIB) $(DLMALLOC)		\
		  $(BBDYN) -lpthread -lm -lssl -lcrypto -ldl -lrt -lz $(ARCHLIBS)

tools/comdb2ar/comdb2ar: $(comdb2ar_OBJS)
	$(CXX11) $(tools_LDFLAGS) $^ $(comdb2ar_LDLIBS) -o $@

# Files that include db.h require COMDB2AR to be defined
db_wrap_FLAGS=$(CFLAGS_ARCHFLAGS) -DCOMDB2AR -I$(SRCHOME)/mem	\
-I$(SRCHOME)/crc32c

tools/comdb2ar/db_wrap.o: tools_CPPFLAGS+=$(db_wrap_FLAGS)
tools/comdb2ar/serialise.o: tools_CPPFLAGS+=$(db_wrap_FLAGS)

cdb2sql_LDLIBS=$(tools_LDLIBS) $(LIBREADLINE)
cdb2sql_OBJS:=tools/cdb2sql/cdb2sql.o
tools/cdb2sql/cdb2sql: $(cdb2sql_OBJS)
	$(CC) $(tools_LDFLAGS) $^ $(cdb2sql_LDLIBS) -o $@

# Pmux - Use flag for C++11 standard
pmux_LDFLAGS=$(CXX11LDFLAGS) -L$(SRCHOME)/cdb2api	\
-L$(SRCHOME)/protobuf -L$(SRCHOME)/bb $(OPTBBRPATH)
pmux_LDLIBS=$(CDB2API_BIN) -lbb -lcdb2protobuf $(BBLDPREFIX)$(BBSTATIC)	\
-lsqlite3 -lprotobuf-c -L$(SRCHOME)/dlmalloc -ldlmalloc $(BBLDPREFIX)$(BBDYN) -lpthread -ldl -lssl -lcrypto
ifeq ($(arch),Linux)
    pmux_LDLIBS+=-lrt
else
ifeq ($(arch),SunOS)
    pmux_LDLIBS+=-lsocket -lnsl -lresolv
endif
endif

pmux_SOURCES:=tools/pmux/pmux.cpp
pmux_OBJS:=$(patsubst %.cpp,%.o,$(pmux_SOURCES))

tools/pmux/pmux: $(pmux_OBJS)
	$(CXX11) $(pmux_LDFLAGS) $< $(pmux_LDLIBS) -o $@

$(pmux_OBJS): %.o: %.cpp $(LIBS_BIN)
	$(CXX11) $(CPPFLAGS) $(tools_CPPFLAGS) $(CXX11FLAGS) -c $< -o $@


# Cdb2_dump et al. - Needs more dependencies for the cdb2_ tools
# Cdb2_dump and others. Omit cdb2_printlog for now because it needs
# multiple $OBJS
cdb2_TASKS:=$(filter tools/cdb2_%,$(filter-out tools/cdb2_printlog%,$(lcl_TASKS)))
cdb2_OBJS:=$(patsubst %,%.o,$(cdb2_TASKS))

# Dependencies for the cdb2_ tools
cdb2_CPPFLAGS:=-I$(SRCHOME)/bdb -I$(SRCHOME)/net -I$(SRCHOME)/crc32c

$(cdb2_OBJS): tools_CPPFLAGS+=$(cdb2_CPPFLAGS)


# Since there are circular dependencies -ldb and -lschemachange must
# go first.
$(cdb2_TASKS): %: %.o
	$(CC) $(tools_LDFLAGS) $< $(BERKDB) $(SCHEMA) $(tools_LDLIBS) -o $@

# Cdb2_printlog
cdb2_printlog_SOURCES:=comdb2_dbprintlog.c cdb2_printlog.c
cdb2_printlog_OBJS:=$(patsubst %.c,tools/cdb2_printlog/%.o,$(cdb2_printlog_SOURCES))

$(cdb2_printlog_OBJS): tools_CPPFLAGS+=$(cdb2_CPPFLAGS)

# Since there are circular dependencies -ldb and -lschemachange must
# go first.
tools/cdb2_printlog/cdb2_printlog: $(cdb2_printlog_OBJS)
	$(CC) $(tools_LDFLAGS) $(cdb2_printlog_OBJS) $(BERKDB) $(SCHEMA) $(tools_LDLIBS) -o $@

# Defined in the top level makefile
TASKS+=$(lcl_TASKS) $(tools_TASKS)
OBJS+=tools/cdb2sql/cdb2sql.o tools/comdb2sc/comdb2sc.o		\
$(cdb2sockpool_OBJS) $(comdb2ar_OBJS) $(pmux_OBJS) $(cdb2_OBJS)	\
$(cdb2_printlog_OBJS)

# Build tools by default
all: $(tools_TASKS)
