ifeq ($(TESTSROOTDIR),)
	include ../testcase.mk
else
	include $(TESTSROOTDIR)/testcase.mk
endif
ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=2m
endif

unexport CLUSTER
#export CLUSTER=$(word 1,$(CLUSTER) )
