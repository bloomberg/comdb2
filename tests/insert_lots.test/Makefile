ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
export TEST_TIMEOUT=5m

CDB2LIBS=$(SYSPATH) $(SRCHOME)/cdb2api/libcdb2api.a -lprotobuf-c -lpthread -lssl -lcrypto
CXXFLAGS+=-D_GNU_SOURCE -Wall -I$(SRCHOME)/cdb2api -I$(SRCHOME)/bbinc

tool: insert_lots_mt

insert_lots_mt: insert_lots_mt.o
	$(CXX) -g -o $@ $< $(CDB2LIBS) 
