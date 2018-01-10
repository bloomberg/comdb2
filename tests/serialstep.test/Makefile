ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif

tool:
	 make -skC $(TESTSROOTDIR)/tools stepper
