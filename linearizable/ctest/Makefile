INS_OBJS=insert.o 
REG_OBJS=register.o
SHR_OBJS=nemesis.o testutil.o

LIBS=../../cdb2api/libcdb2api.a ../../protobuf/libcdb2protobuf.a -L/opt/bb/lib -lprotobuf-c -lpthread
CFLAGS=-Wall -g -std=c99 -I. -I../../cdb2api -I../../protobuf -D_XOPEN_SOURCE=500

all: insert register

insert: $(INS_OBJS) $(SHR_OBJS)
	gcc -o $@ $^ $(LDFLAGS) $(LIBS) -lssl -lcrypto

register: $(REG_OBJS) $(SHR_OBJS)
	gcc -o $@ $^ $(LDFLAGS) $(LIBS) -lssl -lcrypto

%.o: %.c
	gcc -c $(CFLAGS) $< -o $@

clean:  
	rm -f $(INS_OBJS) $(REG_OBJS) $(SHR_OBJS) insert register
