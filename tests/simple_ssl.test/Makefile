ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
export TEST_TIMEOUT=5m

tool:
	$(shell ${TESTSROOTDIR}/simple_ssl.test/genbadkey.sh ${TESTDIR})
	@echo KEYS GENERATED
