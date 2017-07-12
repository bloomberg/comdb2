OBJS=test.o
BINARY=test

LIBS=../../cdb2api/libcdb2api.a ../../protobuf/libcdb2protobuf.a -L/opt/bb/lib -lprotobuf-c -lpthread
CFLAGS=-Wall -g -std=c99 -I../../cdb2api -I../../protobuf -D_XOPEN_SOURCE=500

$(BINARY): $(OBJS)
	gcc -o $(BINARY) $(OBJS) $(LDFLAGS) $(LIBS)

%.o: %.c
	gcc -c $(CFLAGS) $< -o $@

clean:  
	rm -f $(BINARY) $(OBJS)
