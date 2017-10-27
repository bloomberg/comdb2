ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
export TEST_TIMEOUT=1m
export MALLOC_ARENA_MAX=4
$(shell echo "export MALLOC_ARENA_MAX=4" >> ${TESTDIR}/replicant_vars )
