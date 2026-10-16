ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
ifeq ($(TEST_TIMEOUT),)
  export TEST_TIMEOUT=2m
endif
export VERIFY_DB_AT_FINISH=0
