ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
export TEST_TIMEOUT=1m

CDB2LIBS=$(SYSPATH) ../../cdb2api/libcdb2api.a -lprotobuf-c -lpthread -lssl -lcrypto
CXXFLAGS+=-I../../cdb2api -D__USE_XOPEN2K8
cdb2api_caller: cdb2api_caller.o
	$(CXX) -o cdb2api_caller $< $(LDFLAGS) $(CDB2LIBS) -lpthread

