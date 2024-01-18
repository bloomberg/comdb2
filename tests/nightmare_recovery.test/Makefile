ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif

ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=7m
endif

export DUMPLOCK_ON_TIMEOUT=1
export CORE_ON_TIMEOUT=1
