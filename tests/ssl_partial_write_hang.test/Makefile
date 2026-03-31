export LD_PRELOAD=${TESTSBUILDDIR}/libpartialwrite.so
ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=1m
endif
$(shell mkdir -p ${DBDIR}/; echo "export LD_PRELOAD=${TESTSBUILDDIR}/libpartialwrite.so" >> ${DBDIR}/replicant_env_vars )
