# Local defs

tools_LIBS:=libcdb2util.a

tools_INCLUDE:=-I$(SRCHOME)/crc32c -I$(SRCHOME)/bbinc			\
-I$(SRCHOME)/cdb2api -I$(SRCHOME)/berkdb -I$(SRCHOME)/berkdb/build	\
-I$(SRCHOME)/dlmalloc -I$(SRCHOME)/sockpool -I$(SRCHOME)/cson		\
$(OPTBBINCLUDE)

tools_SYSLIBS=$(BBSTATIC) -lprotobuf-c -lssl -lcrypto -llz4 $(BBDYN)	\
-lpthread -lrt -lm -lz $(ARCHLIBS)

tools_CPPFLAGS:=$(tools_INCLUDE) $(CPPFLAGS)
tools_LDFLAGS:=$(LDFLAGS) $(LCLFLAGS)
tools_LDLIBS:=$(LDLIBS) -ldl $(LCLLIBS) $(tools_SYSLIBS)

# Base rules for all objects
tools/%.o: tools/%.c tools/%.d $(LIBS_BIN)
	$(CC) $(DEPFLAGS) $(tools_CPPFLAGS) $(CFLAGS) -c -o $@ $<
	$(POSTCOMPILE)

tools/%.o: tools/%.cpp tools/%.d $(LIBS_BIN)
	$(CXX11) $(DEPFLAGS_CXX11) $(tools_CPPFLAGS) $(CXX11FLAGS) -c -o $@ $<
	$(POSTCOMPILE)

# Cdb2sql - This only has .c file , though it does
# depend on some of the auto-generated .h from other modules
cdb2sql_OBJS:=tools/cdb2sql/cdb2sql.o
cdb2sql: tools_LDLIBS+=$(LIBREADLINE)
cdb2sql: $(cdb2sql_OBJS)
	$(CC) $(tools_LDFLAGS) $^ $(tools_LDLIBS) -o $@

cdb2replay_OBJS:=tools/cdb2_sqlreplay/cdb2_sqlreplay.o
cdb2replay_CFLAGS=-Icson
cdb2replay_LDLIBS=-Lcson -lcson $(CDB2API) $(PROTOBUF) \
                  -lprotobuf-c -lssl -lcrypto -lz -lpthread

$(cdb2replay_OBJS): %.o: %.cpp $(LIBS_BIN)
	$(CXX11) $(CPPFLAGS) $(tools_CPPFLAGS) $(cdb2replay_CFLAGS) $(CXX11FLAGS) -c $< -o $@


cdb2_sqlreplay: $(cdb2replay_OBJS)
	$(CXX11) $(tools_CPPFLAGS) $(LDFLAGS) $< $(cdb2replay_LDLIBS) -o $@

# Cdb2sockpool - Use base rules, multiple object files
cdb2sockpool_SOURCES:=utils.c settings.c cdb2sockpool.c
cdb2sockpool_OBJS:=$(patsubst %.c,tools/cdb2sockpool/%.o,$(cdb2sockpool_SOURCES))
cdb2sockpool_LDLIBS=$(tools_LDLIBS) -lbb -lsockpool
cdb2sockpool: $(cdb2sockpool_OBJS)
	$(CC) $(tools_LDFLAGS) $^ $(cdb2sockpool_LDLIBS) -o $@

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
comdb2ar_LDLIBS+= $(BBSTATIC) $(BBLIB) $(CRC32C) -ldlmalloc $(DLMALLOC)		\
		  $(BBDYN) -lpthread -lm -lssl -lcrypto -ldl -lrt -lz $(ARCHLIBS)

comdb2ar: $(comdb2ar_OBJS)
	$(CXX11) $(tools_LDFLAGS) $^ $(comdb2ar_LDLIBS) -o $@


# Files that include db.h require COMDB2AR to be defined
db_wrap_FLAGS=$(CFLAGS_ARCHFLAGS) -DCOMDB2AR -I$(SRCHOME)/mem	\
-I$(SRCHOME)/crc32c

tools/comdb2ar/db_wrap.o: tools_CPPFLAGS+=$(db_wrap_FLAGS)
tools/comdb2ar/serialise.o: tools_CPPFLAGS+=$(db_wrap_FLAGS)

# Pmux - Use flag for C++11 standard
pmux_LDFLAGS=$(CXX11LDFLAGS) -L$(SRCHOME)/cdb2api       \
-L$(SRCHOME)/protobuf -L$(SRCHOME)/bb $(OPTBBRPATH)
pmux_LDLIBS=$(CDB2API) $(PROTOBUF) -lbb $(BBLDPREFIX)$(BBSTATIC) \
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

pmux: $(pmux_OBJS)
	$(CXX11) $(pmux_LDFLAGS) $< $(pmux_LDLIBS) -o $@

$(pmux_OBJS): %.o: %.cpp $(LIBS_BIN)
	$(CXX11) $(CPPFLAGS) $(tools_CPPFLAGS) $(CXX11FLAGS) -c $< -o $@

# Cdb2_dump et al. - Needs more dependencies for the cdb2_ tools
# Cdb2_dump and others. Omit cdb2_printlog for now because it needs
# multiple $OBJS
cdb2_SRC:=cdb2_dump/cdb2_dump.c cdb2_stat/cdb2_stat.c cdb2_verify/cdb2_verify.c cdb2_printlog/comdb2_dbprintlog.c cdb2_printlog/cdb2_printlog.c
BERKOBJS=berkdb/common/util_sig.o berkdb/common/util_cache.o
cdb2_OBJS:=$(patsubst %.c,tools/%.o,$(cdb2_SRC)) $(BERKOBJS)

# Dependencies for the cdb2_ tools
cdb2_CPPFLAGS:=-I$(SRCHOME)/bdb -I$(SRCHOME)/net -I$(SRCHOME)/crc32c

$(cdb2_OBJS): tools_CPPFLAGS+=$(cdb2_CPPFLAGS)

libcdb2util.a: $(cdb2_OBJS)
	$(AR) $(ARFLAGS) $@ $^

# Cdb2_printlog
cdb2_printlog_SOURCES:=comdb2_dbprintlog.c cdb2_printlog.c
cdb2_printlog_OBJS:=$(patsubst %.c,tools/cdb2_printlog/%.o,$(cdb2_printlog_SOURCES))

$(cdb2_printlog_OBJS): tools_CPPFLAGS+=$(cdb2_CPPFLAGS)

# Defined in the top level makefile
TASKS+=$(lcl_TASKS) $(tools_LIBS)
tools_TASKS:=pmux cdb2sql comdb2ar cdb2sockpool cdb2_sqlreplay
# Defined in the top level makefile
TASKS+=$(tools_TASKS) $(tools_LIBS)

OBJS+=$(comdb2ar_OBJS) $(cdb2sockpool_OBJS) $(pmux_OBJS) $(cdb2sql_OBJS) $(cdb2_OBJS) $(BERKOBJS) $(cdb2replay_OBJS)

# Build tools by default
all: $(tools_LIBS) $(tools_TASKS)
