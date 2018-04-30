ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=1m
endif

# this is a local test, don't need cluster
unexport CLUSTER
export COMDB2_UNITTEST=1
