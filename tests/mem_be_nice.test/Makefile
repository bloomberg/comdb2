ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=1m
endif
export MALLOC_ARENA_MAX=4
$(shell echo "export MALLOC_ARENA_MAX=4" >> ${TESTDIR}/replicant_vars )
