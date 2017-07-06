ALL=hatest selectv overflow_blobtest recom stepper serial bound localrep utf8 crle
all:$(ALL)

include ../../main.mk

CDB2LIBS=$(SYSPATH) ../../cdb2api/libcdb2api.a ../../protobuf/libcdb2protobuf.a -lprotobuf-c -lpthread -lssl -lcrypto

CFLAGS+=-D_GNU_SOURCE $(CFLAGS_ARCHFLAGS)
CXXFLAGS+=-I../../cdb2api -D__USE_XOPEN2K8

localrep: localrep.o strbuf.o
	$(CXX) -o localrep $^ $(LDFLAGS) $(CDB2LIBS) -lpthread

bound: bound.o
	$(CXX) -o bound $< $(LDFLAGS) $(CDB2LIBS) -lpthread

utf8: utf8.o
	$(CC) -o utf8 $< $(LDFLAGS) $(CDB2LIBS) -lpthread

hatest: hatest.o
	$(CC) -o hatest $< $(LDFLAGS) $(CDB2LIBS) -lpthread -lreadline

selectv: selectv.o
	$(CC) -o selectv $< $(LDFLAGS) $(CDB2LIBS) -lpthread

overflow_blobtest: overflow_blobtest.o
	$(CC) -o overflow_blobtest $< $(LDFLAGS) $(CDB2LIBS) -lsqlite3 -lpthread

recom: recom.o
	$(CC) -o recom $< $(LDFLAGS) $(CDB2LIBS) -lsqlite3 -lpthread

stepper: stepper.o stepper_client.o 
	$(CC) -o stepper $^ $(LDFLAGS) $(CDB2LIBS) -L../../bb -lbb -L../../dlmalloc -ldlmalloc -lm -lrt

serial: serial.o
	$(CC) -o serial $^ $(LDFLAGS) $(CDB2LIBS) -lpthread

ptrantest: ptrantest.o
	$(CC) -o $@ $^ $(LDFLAGS) $(CDB2LIBS) -lsqlite3 -lpthread

crle:CFLAGS+=-I../../comdb2rle

clean:
	@rm -f *.o $(ALL)

%.o: %.c
	$(CC) -o $@ -c $< $(CFLAGS) -I../../cdb2api -I../../bbinc
