# Local defs
sc_MEMGEN:=schemachange/mem_schemachange.h
sc_MEMGENOBJ=$(patsubst %.c,%.o)
sc_OBJS=			\
schemachange/schemachange.o	\
schemachange/sc_alter_table.o	\
schemachange/sc_add_table.o	\
schemachange/sc_callbacks.o	\
schemachange/sc_csc2.o		\
schemachange/sc_drop_table.o	\
schemachange/sc_fastinit_table.o \
schemachange/sc_global.o	\
schemachange/sc_logic.o		\
schemachange/sc_lua.o		\
schemachange/sc_queues.o	\
schemachange/sc_records.o	\
schemachange/sc_schema.o	\
schemachange/sc_stripes.o	\
schemachange/sc_struct.o	\
schemachange/sc_util.o		\
schemachange/sc_sequences.o

sc_CPPFLAGS+=$(SQLITE_FLAGS) \
-I.						\
-I$(SRCHOME)/db 				\
-I$(SRCHOME)/protobuf 				\
-I$(SRCHOME)/sqlite 				\
-I$(SRCHOME)/bdb 				\
-I$(SRCHOME)/net 				\
-I$(SRCHOME)/csc2 				\
-I$(SRCHOME)/lua 				\
-I$(SRCHOME)/dlmalloc 				\
-I$(SRCHOME)/datetime 				\
-I$(SRCHOME)/crc32c                 \
-I$(SRCHOME)/dfp/decNumber 			\
-I$(SRCHOME)/dfp/dfpal				\
-I$(SRCHOME)/cdb2api				\
$(OPTBBINCLUDE)

# Defined in the top level makefile
ARS+=schemachange/libschemachange.a
OBJS+=$(sc_OBJS)
GENH+=$(sc_MEMGEN)

schemachange/libschemachange.a: CPPFLAGS+=$(sc_CPPFLAGS)

schemachange/libschemachange.a: $(sc_OBJS)
	$(AR) $(ARFLAGS) $@ $^

$(sc_OBJS): $(sc_MEMGEN)

# This directory depends on parse.h from sqlite
$(sc_OBJS): sqlite/parse.h

