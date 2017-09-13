# Local defs
cdb2api_OBJS:=cdb2api/cdb2api.o

cdb2api/libcdb2api.so: $(cdb2api_OBJS) $(pbuf_OBJS)
	$(CC) $(CFLAGS) $(SHARED) $^ -o $@

ARS+=cdb2api/libcdb2api.a
ARS+=cdb2api/libcdb2api.so
OBJS+=$(cdb2api_OBJS)

cdb2api/libcdb2api.a: CFLAGS+=-I$(SRCHOME)/protobuf -I${SRCHOME}/bb $(OPTBBINCLUDE) -DSBUF2_SERVER=0
cdb2api/libcdb2api.a: $(cdb2api_OBJS) $(pbuf_OBJS)
	$(AR) $(ARFLAGS) $@ $^
