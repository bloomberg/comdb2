INS_OBJS=insert.o 
REG_OBJS=register.o
BRK_OBJS=breakloop.o
SHR_OBJS=nemesis.o testutil.o 
CDB2APIDIR=../../../cdb2api
CDB2APILIB=$(CDB2APIDIR)/libcdb2api.a


LIBS=$(CDB2APILIB) -L/opt/bb/lib -lprotobuf-c -lpthread
CFLAGS=-Wall -g -std=c99 -I. -I$(CDB2APIDIR) -I../../protobuf -D_XOPEN_SOURCE=500

all: insert register breakloop

insert: $(INS_OBJS) $(SHR_OBJS)
	gcc -o $@ $^ $(LDFLAGS) $(LIBS) -lssl -lcrypto

register: $(REG_OBJS) $(SHR_OBJS)
	gcc -o $@ $^ $(LDFLAGS) $(LIBS) -lssl -lcrypto

breakloop: $(BRK_OBJS) $(SHR_OBJS)
	gcc -o $@ $^ $(LDFLAGS) $(LIBS) -lssl -lcrypto

%.o: %.c
	gcc -c $(CFLAGS) $< -o $@

clean:  
	rm -f $(INS_OBJS) $(REG_OBJS) $(BRK_OBJS) $(SHR_OBJS) insert register breakloop
