# Local defs
sockpool_OBJS:=sockpool/sockpool.o

ARS+=sockpool/libsockpool.a
OBJS+=$(sockpool_OBJS)

sockpool/libsockpool.a: CPPFLAGS+=$(OPTBBINCLUDE)
sockpool/libsockpool.a: $(sockpool_OBJS)
	$(AR) $(ARFLAGS) $@ $^
