ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
ifeq ($(TEST_TIMEOUT),)
  export TEST_TIMEOUT=1m
endif

ifneq (,$(findstring singlenode_generated,$(TESTCASE)))
  # var TESTCASE contains substring singlenode_generated
  # note that could have put this as a rule in the parent directory's Makefile:
  #%singlenode_generated: basicops
  #	@$(MAKE) CLUSTER=$(word 1,$(CLUSTER)) -skC $(TESTDIR)/$@.test
  CLUSTER_SV := $(word 1,$(CLUSTER))
  override CLUSTER := $(CLUSTER_SV)
endif

tool:
	make -skC $(TESTSROOTDIR)/tools cdb2api_caller

CDB2LIBS=$(SYSPATH) $(SRCHOME)/cdb2api/libcdb2api.a -lprotobuf-c -lpthread -lssl -lcrypto
CXXFLAGS+=-D_GNU_SOURCE -Wall -I$(SRCHOME)/cdb2api -I$(SRCHOME)/bbinc

