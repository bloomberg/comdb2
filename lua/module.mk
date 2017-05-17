lua_MEMGEN:=lua/mem_lua.h
lua_SOURCES:=lua/lapi.o lua/lcode.o lua/ldebug.o lua/ldo.o		\
   lua/ldump.o lua/lfunc.o lua/lgc.o lua/llex.o lua/lmem.o		\
   lua/lobject.o lua/lopcodes.o lua/lparser.o lua/lstate.o		\
   lua/lstring.o lua/ltable.o lua/ltm.o lua/lundump.o lua/lvm.o		\
   lua/lzio.o lua/lauxlib.o lua/lbaselib.o lua/ldblib.o lua/liolib.o	\
   lua/lmathlib.o lua/loslib.o lua/ltablib.o lua/lstrlib.o		\
   lua/loadlib.o lua/linit.o lua/ltypes.o lua/luautil.o lua/sp.o	\
   lua/syssp.o
lua_OBJS:=$(lua_SOURCES:.c=.o)

# Defined in the top level makefile
ARS+=lua/liblua.a
OBJS+=$(lua_OBJS)
GENH+=$(lua_MEMGEN)

lua_CPPFLAGS:=-I$(SRCHOME)/cdb2api -I$(SRCHOME)/datetime		\
-I$(SRCHOME)/dfp/dfpal -I$(SRCHOME)/dfp/decNumber -I$(SRCHOME)/sqlite	\
-I$(SRCHOME)/csc2 -I$(SRCHOME)/db -I$(SRCHOME)/lua -I$(SRCHOME)/bdb	\
-I$(SRCHOME)/net -I$(SRCHOME)/protobuf $(OPTBBINCLUDE)			\
-I$(SRCHOME)/dlmalloc -I$(SRCHOME)/cson -DLUA_USE_POSIX			\
-DLUA_USE_DLOPEN $(SQLITE_FLAGS) $(CPPFLAGS)

# Custom defines
lua/liblua.a: CPPFLAGS=$(lua_CPPFLAGS)

lua/liblua.a: $(lua_OBJS)
	$(AR) $(ARFLAGS) $@ $^

# This directory depends on parse.h from sqlite
$(lua_OBJS): sqlite/parse.h

$(lua_OBJS): $(lua_MEMGEN)
