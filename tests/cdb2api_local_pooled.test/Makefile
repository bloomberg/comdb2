ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=3m
endif

# Keep the typestr (comdb2/<dbname>/<tier>/newsql/<policy>) short
export DBNAME=lpool$(TESTID)
