export SECONDARY_DB_PREFIX=second
ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=4m
endif


tool:
	make -skC $(TESTSROOTDIR)/tools cdb2api_caller
