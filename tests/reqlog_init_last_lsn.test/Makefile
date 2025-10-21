# Skip auto-starting a database, we will handle it in the test case.
COMDB2_UNITTEST=1

ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif

ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=1m
endif
