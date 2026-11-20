# Runs on the UBSan comdb2 (build-ubsan/); 'make sc_add_vutf8 COMDB2_UBSAN=0'
# runs it on the regular one.
export COMDB2_UBSAN?=1
ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=1m
endif
