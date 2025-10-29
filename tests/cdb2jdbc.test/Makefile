export SECONDARY_DB_PREFIX=nossl
export SSL_CERT_PATH=${TESTDIR}/certs
# The test uses a server certificate with wrong CN
$(shell mkdir -p ${TESTDIR}/certs)
$(shell TESTDIR="${TESTDIR}/certs" CLUSTER="${CLUSTER}" SKIPSSL="${SKIPSSL}" SCN="example.com" $(TESTSROOTDIR)/tools/keygen.sh)

ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=15m
endif
