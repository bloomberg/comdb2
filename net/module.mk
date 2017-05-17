# Local defs
net_MEMGEN=net/mem_net.h
net_OBJS:=net/net.o net/info.o net/trace.o

# Defined in the top level makefile
ARS+=net/libnet.a
OBJS+=$(net_OBJS)
GENH+=$(net_MEMGEN)

net_CPPFLAGS+=-I$(SRCHOME)/bdb
net/libnet.a: CPPFLAGS+=$(net_CPPFLAGS)
net/libnet.a: $(net_OBJS) 
	$(AR) $(ARFLAGS) $@ $^

$(net_OBJS): $(net_MEMGEN)
