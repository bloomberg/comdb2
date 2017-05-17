# Local defs
comdb2rle_MEMGEN=comdb2rle/mem_comdb2rle.h
comdb2rle_OBJS:=comdb2rle/comdb2rle.o

# Defined in the top level makefile
ARS+=comdb2rle/libcomdb2rle.a
OBJS+=$(comdb2rle_OBJS)
GENH+=$(comdb2rle_MEMGEN)

# Custom defines
comdb2rle/libcomdb2rle.a: CPPFLAGS+=-DVERIFY_CRLE $(CFLAGS_OPT)

comdb2rle/libcomdb2rle.a: $(comdb2rle_OBJS)
	$(AR) $(ARFLAGS) $@ $^

$(comdb2rle_OBJS): $(comdb2rle_MEMGEN)
