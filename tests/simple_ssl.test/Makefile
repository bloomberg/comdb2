ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
export TEST_TIMEOUT=5m

CDB2LIBS=$(SYSPATH) $(SRCHOME)/cdb2api/libcdb2api.a -lprotobuf-c -lpthread -lssl -lcrypto
CFLAGS+=-D_GNU_SOURCE -Wall -I$(SRCHOME)/cdb2api -I$(SRCHOME)/bbinc

tool: ssl_bench
	$(shell ${TESTSROOTDIR}/simple_ssl.test/genbadkey.sh ${TESTDIR})
	@echo KEYS GENERATED

ssl_bench: ssl_bench.c
	$(CC) -g -o $@ $< $(CFLAGS) $(CDB2LIBS)
