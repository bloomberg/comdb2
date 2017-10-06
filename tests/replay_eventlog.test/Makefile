ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
export TEST_TIMEOUT=4m


tool:
	make -skC $(TESTSROOTDIR)/tools cdb2api_caller
