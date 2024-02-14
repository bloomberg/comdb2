ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
unexport CLUSTER
ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=5m
endif
