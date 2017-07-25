include $(TESTSROOTDIR)/testcase.mk
export TEST_TIMEOUT=1m


CFLAGS+=-I$(SRCHOME)/cdb2api -I$(SRCHOME)/bbinc

CDB2LIBS=$(SYSPATH) $(SRCHOME)/cdb2api/libcdb2api.a $(SRCHOME)/protobuf/libcdb2protobuf.a -lprotobuf-c -lpthread -lssl -lcrypto -L$(SRCHOME)/bb -lbb



tool: 
	make client

client: client.o
	$(CC) -g -o $@ $< $(CDB2LIBS) 

%.o: %.c
	echo $(CC) -o $@ -c $< $(CFLAGS) 
	$(CC) -g -o $@ -c $< $(CFLAGS) 
