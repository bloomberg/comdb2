COMDB2_UNITTEST=1
ifeq ($(TESTSROOTDIR),)
	include ../testcase.mk
else
	include $(TESTSROOTDIR)/testcase.mk
endif
ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=2m
endif

tool:
	make -skC $(TESTSROOTDIR)/tools verify_atomics_work
