ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
export TEST_TIMEOUT=3m


tool:
	make -skC $(TESTSROOTDIR)/tools ptrantest
