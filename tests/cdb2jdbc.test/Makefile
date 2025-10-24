export SECONDARY_DB_PREFIX=nossl
# The test uses a server certificate with wrong CN
$(shell rm -f ${TESTDIR}/*.key)
$(shell TESTDIR="${TESTDIR}" CLUSTER="${CLUSTER}" SKIPSSL="${SKIPSSL}" SCN="example.com" $(TESTSROOTDIR)/tools/keygen.sh)

ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=15m
endif
