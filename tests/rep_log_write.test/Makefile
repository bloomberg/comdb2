ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif

ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=3m
endif

# This will still look for cores
export CHECK_DB_AT_FINISH=0
export DUMPLOCK_ON_TIMEOUT=1
export CORE_ON_TIMEOUT=1
