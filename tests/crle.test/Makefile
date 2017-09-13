COMDB2_UNITTEST=1
ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif

tool:
	make -skC $(TESTSROOTDIR)/tools crle

