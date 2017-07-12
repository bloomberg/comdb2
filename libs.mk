DLMALLOC_BIN=$(SRCHOME)/dlmalloc/libdlmalloc.a
UTIL_BIN=$(SRCHOME)/util/libutil.a
NET_BIN=$(SRCHOME)/net/libnet.a
SQLITE_BIN=$(SRCHOME)/sqlite/libsqlite.a
DATETIME_BIN=$(SRCHOME)/datetime/libtz.a
LUA_BIN=$(SRCHOME)/lua/liblua.a
PROTOBUF_BIN=$(SRCHOME)/protobuf/libcdb2protobuf.a
COMDB2RLE_BIN=$(SRCHOME)/comdb2rle/libcomdb2rle.a
CRC32C_BIN=$(SRCHOME)/crc32c/libcrc32c.a
DFPAL_BIN=$(SRCHOME)/dfp/decNumber/libdfpal.a
CMACC2_BIN=$(SRCHOME)/csc2/libcsc2lib.a
BDB_BIN=$(SRCHOME)/bdb/libbdblib.a
BERKDB_BIN=$(SRCHOME)/berkdb/libdb.a
BBLIB_BIN=$(SRCHOME)/bb/libbb.a
CDB2API_BIN=$(SRCHOME)/cdb2api/libcdb2api.a
SCHEMA_BIN=$(SRCHOME)/schemachange/libschemachange.a
CSON_BIN=$(SRCHOME)/cson/libcson.a
SOCKPOOL_BIN=$(SRCHOME)/sockpool/libsockpool.a
CDB2API_BIN_SO=$(SRCHOME)/cdb2api/libcdb2api.so
PROTOBUF_BIN_SO=$(SRCHOME)/protobuf/libcdb2protobuf.so

LCLFLAGS+=-L$(SRCHOME)/dlmalloc
LCLFLAGS+=-L$(SRCHOME)/net
LCLFLAGS+=-L$(SRCHOME)/sqlite
LCLFLAGS+=-L$(SRCHOME)/datetime
LCLFLAGS+=-L$(SRCHOME)/lua
LCLFLAGS+=-L$(SRCHOME)/comdb2rle
LCLFLAGS+=-L$(SRCHOME)/crc32c
LCLFLAGS+=-L$(SRCHOME)/dfp/decNumber
LCLFLAGS+=-L$(SRCHOME)/csc2
LCLFLAGS+=-L$(SRCHOME)/bdb
LCLFLAGS+=-L$(SRCHOME)/berkdb
LCLFLAGS+=-L$(SRCHOME)/bb
LCLFLAGS+=-L$(SRCHOME)/cdb2api
LCLFLAGS+=-L$(SRCHOME)/schemachange
LCLFLAGS+=-L$(SRCHOME)/protobuf
LCLFLAGS+=-L$(SRCHOME)/cson
LCLFLAGS+=-L$(SRCHOME)/sockpool

DLMALLOC=-ldlmalloc
NET=-lnet
SQLITE=-lsqlite
DATETIME=-ltz
LUA=-llua
COMDB2RLE=-lcomdb2rle
CRC32C=-lcrc32c
DFPAL=-ldfpal
CMACC2=-lcsc2lib
BDB=-lbdblib
BERKDB=-ldb
BBLIB=-lbb
CDB2API=$(CDB2API_BIN)
SCHEMA=-lschemachange
PROTOBUF=$(PROTOBUF_BIN)
CSON=-lcson
SOCKPOOL=-lsockpool

LIBS_BIN=$(BDB_BIN) $(BERKDB_BIN) $(SQLITE_BIN) $(CMACC2_BIN)	\
     $(DATETIME_BIN) $(LUA_BIN) $(NET_BIN) $(DLMALLOC_BIN)	\
     $(CRC32C_BIN) $(COMDB2RLE_BIN) $(DFPAL_BIN) $(BBLIB_BIN)	\
     $(CDB2API_BIN) $(SCHEMA_BIN) $(PROTOBUF_BIN) $(CSON_BIN)   \
     $(CDB2API_BIN_SO) $(SOCKPOOL_BIN) $(PROTOBUF_BIN_SO)

LIB_DEPS=dlmalloc net sqlite datetime lua comdb2rle crc32c lz4 dfpal csc2 bb bdb berkdb bblib cdb2api cson protobuf schemachange sockpool

LCLLIBS=$(SCHEMA) $(BDB) $(BERKDB) $(SQLITE) $(CMACC2) $(DATETIME)	\
     $(LUA) $(NET) $(CRC32C) $(COMDB2RLE) $(DFPAL) $(BBLIB)		\
     $(CDB2API) $(DLMALLOC) $(PROTOBUF) $(CSON) $(SOCKPOOL)
