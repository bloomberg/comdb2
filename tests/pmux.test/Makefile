ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
export TEST_TIMEOUT=3m

# this is a local test, don't need cluster
export CLUSTER=
export COMDB2_UNITTEST=1
