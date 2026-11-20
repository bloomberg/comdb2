ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
ifeq ($(TEST_TIMEOUT),)
  export TEST_TIMEOUT=600m
endif
# a benchmark, not a correctness test: skip the harness's file-by-file verify of every table on every node at the end
export CHECK_DB_AT_FINISH=0
