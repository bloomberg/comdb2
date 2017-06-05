# Local defs
comdb2rle_OBJS:=comdb2rle/comdb2rle.o

# Defined in the top level makefile
ARS+=comdb2rle/libcomdb2rle.a
OBJS+=$(comdb2rle_OBJS)

# Custom defines
comdb2rle/libcomdb2rle.a: CPPFLAGS+=-DVERIFY_CRLE
comdb2rle/libcomdb2rle.a: $(comdb2rle_OBJS)
	$(AR) $(ARFLAGS) $@ $^
