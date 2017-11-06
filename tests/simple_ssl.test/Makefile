ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=5m
endif

tool:
	$(shell ${TESTSROOTDIR}/simple_ssl.test/genbadkey.sh ${TESTDIR})
	@echo KEYS GENERATED
