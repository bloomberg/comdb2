all: basicops $(patsubst %.test,%,$(shell tools/get_tests_inorder.sh))


export TESTSROOTDIR:=$(shell pwd)

SRCHOME:=$(shell readlink -f $(TESTSROOTDIR)/../ )
ifeq ($(SRCHOME),)
SRCHOME:=$(TESTSROOTDIR)/..
endif
export SRCHOME

export TESTID:=$(shell tools/get_random.sh)
CURRDIR:=$(shell pwd)
export TESTDIR:=$(CURRDIR)/test_$(TESTID)


basicops_nokey: executables cdb2_sqlreplay cdb2sql comdb2ar pmux comdb2
	$(shell mkdir -p ${TESTDIR})
	$(shell which git > /dev/null 2>&1 && git rev-parse --abbrev-ref HEAD | xargs echo "Branch" >> ${TESTDIR}/test.log)
	$(shell which git > /dev/null 2>&1 && git rev-parse --short HEAD | xargs echo "SHA " >> ${TESTDIR}/test.log)
	$(shell echo "TESTID=${TESTID} " >> ${TESTDIR}/test.log)
	$(shell echo "CLUSTER=${CLUSTER} " >> ${TESTDIR}/test.log)
	$(shell echo "SRCHOME=${SRCHOME} " >> ${TESTDIR}/test.log)
	$(shell echo "TESTSROOTDIR=${TESTSROOTDIR} " >> ${TESTDIR}/test.log)
	$(shell echo "TESTDIR=${TESTDIR} " >> ${TESTDIR}/test.log)
	$(shell echo "" >> ${TESTDIR}/test.log)
	$(shell echo ${MAKECMDGOALS} >> ${TESTDIR}/test.log)

EXECOMDB2:=$(shell command -v ${SRCHOME}/comdb2 2> /dev/null)
EXESQLREPLAY:=$(shell command -v ${SRCHOME}/cdb2_sqlreplay 2> /dev/null)
EXECDB2SQL:=$(shell command -v ${SRCHOME}/cdb2sql 2> /dev/null)
EXECOMDB2AR:=$(shell command -v ${SRCHOME}/comdb2ar 2> /dev/null)
EXEPMUX:=$(shell command -v ${SRCHOME}/pmux 2> /dev/null)

executables: 
	ifndef EXECOMDB2
		$(error "${SRCHOME}/comdb2 is not available please run make")
	endif
	ifndef EXESQLREPLAY
		$(error "${SRCHOME}/cdb2_sqlreplay is not available please run make")
	endif
	ifndef EXECDB2SQL
		$(error "${SRCHOME}/cdb2sql is not available please run make")
	endif
	ifndef EXECOMDB2AR
		$(error "${SRCHOME}/comdb2ar is not available please run make")
	endif
	ifndef EXEPMUX
		$(error "${SRCHOME}/pmux is not available please run make")
	endif

basicops: basicops_nokey
	$(shell TESTDIR="${TESTDIR}" CLUSTER="${CLUSTER}" SKIPSSL="${SKIPSSL}" tools/keygen.sh)

TOTAL=$(words $(MAKECMDGOALS))
ifeq ($(TOTAL),0)
TOTAL=$(shell ls -1d *.test | wc -l)
endif


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
	@$(foreach n, $(CLUSTER), ssh $(n) 'rm -rf *.trap; rm -rf $(TESTSROOTDIR)/test_[0-9]*';)
	@$(MAKE) -C tools clean
