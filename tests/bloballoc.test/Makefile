include $(TESTSROOTDIR)/testcase.mk
export TEST_TIMEOUT=1m

CFLAGS+=-I$(SRCHOME)/cdb2api -I$(SRCHOME)/bbinc

CDB2LIBS=$(SYSPATH) -L$(SRCHOME)/cdb2api -l:libcdb2api.a -L$(SRCHOME)/protobuf/ -lcdb2protobuf -lprotobuf-c -lpthread -lssl -lcrypto -L$(SRCHOME)/bb -lbb




tool: 
	make blob

blob: blob.o
	echo $(CC) -o $@ $< $(CDB2LIBS) 
	$(CC) -o $@ $< $(CDB2LIBS) 
	
%.o: %.c
	echo $(CC) -o $@ -c $< $(CFLAGS) 
	$(CC) -o $@ -c $< $(CFLAGS) 
