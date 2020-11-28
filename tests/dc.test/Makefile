# cdb2api DIRECT CPU tests.
# We need a database name (which is derived from the test name) short enough
# so we can fit it in the sockpool type string.

ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif

ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=1m
endif
