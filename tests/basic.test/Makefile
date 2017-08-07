ifeq ($(TESTSROOTDIR),)
  export TESTSROOTDIR=$(shell readlink -f $(PWD)/..)
  export SKIPSSL=1
endif
include $(TESTSROOTDIR)/testcase.mk
export TEST_TIMEOUT=1m
