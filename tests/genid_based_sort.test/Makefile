include $(TESTSROOTDIR)/testcase.mk
export TEST_TIMEOUT=5m

CDB2LIBS=$(SYSPATH) $(SRCHOME)/cdb2api/libcdb2api.a -lprotobuf-c -lpthread -lssl -lcrypto
CFLAGS+=-D_GNU_SOURCE -Wall -I$(SRCHOME)/cdb2api

tool: genidsort

genidsort: genidsort.c
	$(CC) -g -o $@ $< $(CFLAGS) $(CDB2LIBS)
