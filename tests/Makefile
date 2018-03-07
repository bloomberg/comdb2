export TESTSROOTDIR:=$(shell pwd)

BUILDDIR:=$(shell readlink -f $(TESTSROOTDIR)/../build )
SRCHOME:=$(shell readlink -f $(TESTSROOTDIR)/../ )
ifeq ($(SRCHOME),)
  SRCHOME:=$(TESTSROOTDIR)/..
endif
export SRCHOME

export TESTID:=$(shell tools/get_random.sh)
export TESTDIR:=$(TESTSROOTDIR)/test_$(TESTID)
# List of disabled tests
export DISABLED_TESTS:=$(shell awk '/^DISABLED/{f=1;next};/<END>/{f=0}f' TODO | cut -d' ' -f1)
# List of all tests that need to run. This is essentially a list of all
# non-disabled tests.
export ALL_TESTS:=$(patsubst %.test, %, $(filter-out $(DISABLED_TESTS), \
    $(shell tools/get_tests_inorder.sh)))

TOTAL=$(words $(MAKECMDGOALS))
ifeq ($(TOTAL),0)
TOTAL=$(words $(ALL_TESTS))
endif

# get md5sum of executable use check when running every test in a series
export COMDB2AR_EXE:=$(TESTDIR)/comdb2ar
export COMDB2_EXE:=$(TESTDIR)/comdb2
export CDB2SQL_EXE:=$(TESTDIR)/cdb2sql
export COPYCOMDB2_EXE:=$(TESTDIR)/copycomdb2
export CDB2_SQLREPLAY_EXE:=$(TESTDIR)/cdb2_sqlreplay
export PMUX_EXE:=$(TESTSROOTDIR)/pmux

all: test_exe basicops all_tests

test_exe:
	@$(MAKE) -C $(BUILDDIR)
	@$(MAKE) -C $(BUILDDIR) test_exe
	@mkdir -p ${TESTDIR}
	@ln -f ${BUILDDIR}/tests/tools/blob ${TESTSROOTDIR}/bloballoc.test/blob
	@ln -f ${BUILDDIR}/tests/tools/bound ${TESTSROOTDIR}/tools/bound
	@ln -f ${BUILDDIR}/tests/tools/cdb2_client ${TESTSROOTDIR}/cdb2api_so.test/cdb2_client
	@ln -f ${BUILDDIR}/tests/tools/cdb2api_caller ${TESTSROOTDIR}/tools/cdb2api_caller
	@ln -f ${BUILDDIR}/tests/tools/cdb2bind ${TESTSROOTDIR}/cdb2bind.test/cdb2bind
	@ln -f ${BUILDDIR}/tests/tools/comdb2_blobtest ${TESTSROOTDIR}/blob_size_limit.test/comdb2_blobtest
	@ln -f ${BUILDDIR}/tests/tools/comdb2_sqltest ${TESTSROOTDIR}/sqlite.test/comdb2_sqltest 
	@ln -f ${BUILDDIR}/tests/tools/crle ${TESTSROOTDIR}/tools/crle
	@ln -f ${BUILDDIR}/tests/tools/hatest ${TESTSROOTDIR}/tools/hatest
	@ln -f ${BUILDDIR}/tests/tools/insert_lots_mt ${TESTSROOTDIR}/insert_lots.test/insert_lots_mt
	@ln -f ${BUILDDIR}/tests/tools/leakcheck ${TESTSROOTDIR}/leakcheck.test/leakcheck
	@ln -f ${BUILDDIR}/tests/tools/localrep ${TESTSROOTDIR}/tools/localrep
	@ln -f ${BUILDDIR}/tests/tools/overflow_blobtest ${TESTSROOTDIR}/tools/overflow_blobtest 
	@ln -f ${BUILDDIR}/tests/tools/ptrantest ${TESTSROOTDIR}/tools/ptrantest
	@ln -f ${BUILDDIR}/tests/tools/recom ${TESTSROOTDIR}/tools/recom
	@ln -f ${BUILDDIR}/tests/tools/selectv ${TESTSROOTDIR}/tools/selectv
	@ln -f ${BUILDDIR}/tests/tools/serial ${TESTSROOTDIR}/tools/serial
	@ln -f ${BUILDDIR}/tests/tools/sicountbug ${TESTSROOTDIR}/sicountbug.test/sicountbug
	@ln -f ${BUILDDIR}/tests/tools/sirace ${TESTSROOTDIR}/sirace.test/sirace
	@ln -f ${BUILDDIR}/tests/tools/multithd ${TESTSROOTDIR}/multithd.test/multithd
	@ln -f ${BUILDDIR}/tests/tools/simple_ssl ${TESTSROOTDIR}/simple_ssl.test/simple_ssl
	@ln -f ${BUILDDIR}/tests/tools/stepper ${TESTSROOTDIR}/tools/stepper
	@ln -f ${BUILDDIR}/tests/tools/utf8 ${TESTSROOTDIR}/tools/utf8
	@ln -f ${BUILDDIR}/tests/tools/insert ${TESTSROOTDIR}/tools/insert
	@ln -f ${BUILDDIR}/tests/tools/register ${TESTSROOTDIR}/tools/register
	@ln -f ${BUILDDIR}/tests/tools/breakloop ${TESTSROOTDIR}/tools/breakloop
	@ln -f ${BUILDDIR}/tests/tools/cdb2_open ${TESTSROOTDIR}/tools/cdb2_open
	@ln -f ${BUILDDIR}/tests/tools/verify_atomics_work ${TESTSROOTDIR}/tools/verify_atomics_work
	@ln -f ${BUILDDIR}/db/comdb2 ${TESTDIR}/comdb2
	@ln -f ${BUILDDIR}/db/comdb2 ${TESTDIR}/cdb2_dump
	@ln -f ${BUILDDIR}/db/comdb2 ${TESTDIR}/cdb2_printlog
	@ln -f ${BUILDDIR}/db/comdb2 ${TESTDIR}/cdb2_stat
	@ln -f ${BUILDDIR}/db/comdb2 ${TESTDIR}/cdb2_verify
	@ln -f ${BUILDDIR}/db/copycomdb2 ${TESTDIR}/copycomdb2
	@ln -f ${BUILDDIR}/tools/cdb2_sqlreplay/cdb2_sqlreplay ${TESTDIR}/cdb2_sqlreplay
	@ln -f ${BUILDDIR}/tools/cdb2sockpool/cdb2sockpool ${TESTDIR}/cdb2sockpool
	@ln -f ${BUILDDIR}/tools/cdb2sql/cdb2sql ${TESTDIR}/cdb2sql
	@ln -f ${BUILDDIR}/tools/comdb2ar/comdb2ar ${TESTDIR}/comdb2ar
	@ln -f ${BUILDDIR}/tools/pmux/pmux ${TESTSROOTDIR}/pmux
	@command -v ${COMDB2_EXE} > /dev/null || (echo "${COMDB2_EXE} missing "; exit 1)
	@command -v ${CDB2_SQLREPLAY_EXE} > /dev/null || (echo "${CDB2_SQLREPLAY_EXE} missing "; exit 1)
	@command -v ${CDB2SQL_EXE} > /dev/null || (echo "${CDB2SQL_EXE} missing "; exit 1)
	@command -v ${COMDB2AR_EXE} > /dev/null || (echo "${COMDB2AR_EXE} missing "; exit 1)
	@command -v ${COPYCOMDB2_EXE} > /dev/null || (echo "${COPYCOMDB2_EXE} missing "; exit 1)
	@command -v ${PMUX_EXE} > /dev/null || (echo "${PMUX_EXE} missing "; exit 1)


# Run all non-disabled tests.
all_tests: $(ALL_TESTS)

basicops_nokey:
	$(shell which git > /dev/null 2>&1 && git rev-parse --abbrev-ref HEAD | xargs echo "Branch" >> ${TESTDIR}/test.log)
	$(shell which git > /dev/null 2>&1 && git rev-parse --short HEAD | xargs echo "SHA " >> ${TESTDIR}/test.log)
	$(shell echo "TESTID=${TESTID} " >> ${TESTDIR}/test.log)
	$(shell echo "CLUSTER=${CLUSTER} " >> ${TESTDIR}/test.log)
	$(shell echo "SRCHOME=${SRCHOME} " >> ${TESTDIR}/test.log)
	$(shell echo "TESTSROOTDIR=${TESTSROOTDIR} " >> ${TESTDIR}/test.log)
	$(shell echo "TESTDIR=${TESTDIR} " >> ${TESTDIR}/test.log)
	$(shell echo "DISABLED= $(sort ${DISABLED_TESTS}) " >> ${TESTDIR}/test.log)
	$(shell echo "" >> ${TESTDIR}/test.log)
	$(shell echo ${MAKECMDGOALS} >> ${TESTDIR}/test.log)


basicops: basicops_nokey
	$(shell TESTDIR="${TESTDIR}" CLUSTER="${CLUSTER}" SKIPSSL="${SKIPSSL}" tools/keygen.sh)
	$(shell TOTAL="${TOTAL}" tools/smalldelay.sh)


.PHONY: tools

showparams: 
	@echo TESTSROOTDIR=${TESTSROOTDIR}
	@echo SRCHOME=${SRCHOME}
	@echo TESTID=${TESTID}
	@echo TESTDIR=${TESTDIR}
	@echo CLUSTER=${CLUSTER}

tools:
	@$(MAKE) -sC tools

$(patsubst %.test,%,$(shell ls -d *.test | grep -v crle)): test_exe basicops tools

crle: basicops_nokey tools
	@$(MAKE) -sC crle.test

%: %.test
	@N=`tools/get_test_counter.sh` && echo TESTID=${TESTID} running in $(TESTDIR) $< $$N/${TOTAL}
	@cp -r $< $(TESTDIR)/
	@$(MAKE) -skC $(TESTDIR)/$<

%.clean: %.test
	+$(MAKE) -C $(patsubst %.clean,%,$<) clean

%.setup: %.test
	+$(MAKE) -C $(patsubst %.setup,%,$<) setup

%.tool: %.test
	+$(MAKE) -C $(patsubst %.tool,%,$<) tool

stop:
	@./stopall

kill:
	@./dokillall

testtotalclean: $(patsubst %.test,%.clean,$(shell ls -d *.test))
	rm -f logs/*
	rm -f *.test/*.res
	rm -f *.test/comdb2db.cfg
	rm -f .*.failed
	rm -fr dbs
	rm -f $(TMPDIR)/cdb2test*
	$(MAKE) -C tools clean

clean: testclean

testclean:
	@rm -rf test_[0-9]*
	@$(foreach n, $(CLUSTER), ssh $(n) 'rm -rf *.trap; rm -rf $(TESTSROOTDIR)/test_[0-9]*' < /dev/null;)
	@$(MAKE) -C tools clean
