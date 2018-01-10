all: basicops all_tests

export TESTSROOTDIR:=$(shell pwd)

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

# Run all non-disabled tests.
all_tests: $(ALL_TESTS)

# get md5sum of executable use check when running every test in a series
export COMDB2AR_EXE:=$(SRCHOME)/comdb2ar
export COMDB2_EXE:=$(SRCHOME)/comdb2
export CDB2SQL_EXE:=$(SRCHOME)/cdb2sql
export COPYCOMDB2_EXE:=$(SRCHOME)/db/copycomdb2
export CDB2_SQLREPLAY_EXE:=$(SRCHOME)/cdb2_sqlreplay
export PMUX_EXE:=$(SRCHOME)/pmux

ifeq ($(wildcard $(COMDB2_EXE)),)
  $(error $(COMDB2_EXE) missing, please run make in $(SRCHOME))
endif
ifeq ($(wildcard $(COMDB2AR_EXE)),)
  $(error $(COMDB2AR_EXE) missing, please run make in $(SRCHOME))
endif
ifeq ($(wildcard $(CDB2SQL_EXE)),)
  $(error $(CDB2SQL_EXE) missing, please run make in $(SRCHOME))
endif
ifeq ($(wildcard $(CDB2_SQLREPLAY_EXE)),)
  $(error $(CDB2_SQLREPLAY_EXE) missing, please run make in $(SRCHOME))
endif
ifeq ($(wildcard $(COPYCOMDB2_EXE)),)
  $(error $(COPYCOMDB2_EXE) missing, please ensure it exists)
endif
ifeq ($(wildcard $(PMUX_EXE)),)
  $(error $(PMUX_EXE) missing, please run make in $(SRCHOME))
endif

export COMDB2MD5SUM:=$(shell md5sum ${COMDB2_EXE} | cut -d ' ' -f1)

basicops_nokey: executables
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

executables: execomdb2 exesqlreplay execdb2sql execomdb2ar exepmux
	@mkdir -p ${TESTDIR}
	@mv ${SRCHOME}/comdb2 ${SRCHOME}/cdb2sql ${SRCHOME}/comdb2ar ${SRCHOME}/cdb2_sqlreplay ${SRCHOME}/pmux ${TESTDIR}/

execomdb2: ; @command -v ${SRCHOME}/comdb2 &> /dev/null
exesqlreplay: ; @command -v ${SRCHOME}/cdb2_sqlreplay &> /dev/null
execdb2sql: ; @command -v ${SRCHOME}/cdb2sql &> /dev/null
execomdb2ar: ; @command -v ${SRCHOME}/comdb2ar &> /dev/null
exepmux: ; @command -v ${SRCHOME}/pmux &> /dev/null


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

$(patsubst %.test,%,$(shell ls -d *.test | grep -v crle)): basicops tools

crle: basicops_nokey tools
	@$(MAKE) -sC crle.test

%: %.test
	@N=`tools/get_test_counter.sh` && echo TESTID=${TESTID} running $< $$N/${TOTAL}
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
