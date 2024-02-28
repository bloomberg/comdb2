ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
export VERIFY_DB_AT_FINISH=0
ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=5m
endif
