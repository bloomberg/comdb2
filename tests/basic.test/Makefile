ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
export TEST_TIMEOUT=1m

tool: cdb2api_caller

CDB2LIBS=$(SYSPATH) $(SRCHOME)/cdb2api/libcdb2api.a -lprotobuf-c -lpthread -lssl -lcrypto
CXXFLAGS+=-D_GNU_SOURCE -Wall -I$(SRCHOME)/cdb2api -I$(SRCHOME)/bbinc
cdb2api_caller: cdb2api_caller.o
	$(CXX) -o cdb2api_caller $< $(LDFLAGS) $(CDB2LIBS) -lpthread

