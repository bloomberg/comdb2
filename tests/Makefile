export TESTSROOTDIR:=$(shell pwd)

SRCHOME:=$(shell readlink -f $(TESTSROOTDIR)/../ 2> /dev/null || realpath $(TESTSROOTDIR)/../ )
ifeq ($(SRCHOME),)
  SRCHOME:=$(TESTSROOTDIR)/..
endif

export SRCHOME
ifeq ($(TESTID),)
  export TESTID:=$(shell $(TESTSROOTDIR)/tools/get_random.sh)
endif

include $(TESTSROOTDIR)/Makefile.common

# List of disabled tests
DISABLED_TESTS:=$(shell awk '/^DISABLED TESTS/{f=1;next};/^END DISABLED/{f=0}f' TODO | cut -d' ' -f1)

# List of serial tests
SERIAL_TESTS:=$(shell awk '/^SERIAL TESTS/{f=1;next};/^END SERIAL/{f=0}f' TODO | cut -d' ' -f1)

# When SKIPSSL is on, skip running tests which rely on SSL
ifeq ($(SKIPSSL),1)
  SKIPS:=$(shell grep ssl *.test/lrl*  | grep 'ssl_client_mode VERIFY' | cut -f1 -d'/')
  DISABLED_TESTS:="${DISABLED_TESTS} ${SKIPS}"
endif

# Skip tests that need tcl
ifeq ($(wildcard $(BUILDDIR)/tcl/libtclcdb2.so),)
  #if we have not compiled with tcl, skip tests that require tclcdb2
  SKIPS:=$(shell ls *.test/*.tcl | xargs grep 'package require tclcdb2' | cut -f1 -d'/')
  DISABLED_TESTS:="${DISABLED_TESTS} ${SKIPS}"
endif

# Skip any tests that we specifically want by reading 'tests/excluded.txt'
# which should contain one line per test (line can be testname or testname.test)
ifeq ($(wildcard excluded.txt),excluded.txt)
  SKIPS:=$(shell cat excluded.txt)
  DISABLED_TESTS:="${DISABLED_TESTS} ${SKIPS}"
endif

DOALL:=0
ifeq ($(subst all,,$(MAKECMDGOALS)),) #if cmdgoals empty or 'all'
  # only used to assign TOTAL below, and to print as debug info in target showparams
  DOALL:=1
endif

.PHONY: all all_tests all_nonserial_tests all_serial_tests

all_tests: all_nonserial_tests all_serial_tests
all: init basicops all_tests

CANON_SERIAL := $(patsubst %.test,%, $(SERIAL_TESTS))

# Read alltests.txt if present
ifeq ($(wildcard alltests.txt),alltests.txt)
  SELECTED := $(patsubst %.test,%, \
    $(shell awk 'NF && $$1 !~ /^#/' alltests.txt | sed 's/[[:space:]]\+.*//' ))
  # Ignore DISABLED if using alltests.txt
  ALL_SERIAL_TESTS := $(filter $(CANON_SERIAL),$(SELECTED))     # preserves order of SELECTED
  ALL_TESTS        := $(filter-out $(ALL_SERIAL_TESTS),$(SELECTED))
else
  DISABLED_TESTS   := $(strip $(sort $(DISABLED_TESTS)))
  CANON_DISABLED   := $(patsubst %.test,%, $(DISABLED_TESTS))
  ifeq ($(INCLUDE_LONG_RUNNING),1)
    FLG=
  else
    # by default exclude all tests which run longer than 1 hour
    FLG=--skip-long-running
  endif
  ALL_CANDIDATES   := $(patsubst %.test,%, $(shell $(TESTSROOTDIR)/tools/get_tests_inorder.sh $(FLG)))

  ALL_SERIAL_TESTS := $(filter-out $(CANON_DISABLED), $(CANON_SERIAL))
  ALL_TESTS        := $(filter-out $(ALL_SERIAL_TESTS) $(CANON_DISABLED), $(ALL_CANDIDATES))
endif

# ----- execution -----
all_nonserial_tests: $(ALL_TESTS)
	@echo "Non-serial tests complete"

all_serial_tests: all_nonserial_tests
	@echo "Running serial tests: $(ALL_SERIAL_TESTS)"
	@set -e; \
	for t in $(ALL_SERIAL_TESTS); do \
	  $(MAKE) --no-print-directory $$t; \
	done

# get number of TOTAL tests running: used for printing progress report of tests
TOTAL=$(words $(MAKECMDGOALS) )
ifeq ($(DOALL),1)
  TOTAL=$(words $(ALL_TESTS) )
endif


init:
	@mkdir -p ${TESTDIR}

basicops_nokey: init
	$(shell which git > /dev/null 2>&1 && git rev-parse --abbrev-ref HEAD | xargs echo "Branch" >> ${TESTDIR}/test.log)
	$(shell which git > /dev/null 2>&1 && git rev-parse --short HEAD | xargs echo "SHA " >> ${TESTDIR}/test.log)
	$(shell echo "TESTID=${TESTID} " >> ${TESTDIR}/test.log)
	$(shell echo "CLUSTER=${CLUSTER} " >> ${TESTDIR}/test.log)
	$(shell echo "SRCHOME=${SRCHOME} " >> ${TESTDIR}/test.log)
	$(shell echo "TESTSROOTDIR=${TESTSROOTDIR} " >> ${TESTDIR}/test.log)
	$(shell echo "TESTDIR=${TESTDIR} " >> ${TESTDIR}/test.log)
	$(shell echo "DISABLED= ${DISABLED_TESTS} " >> ${TESTDIR}/test.log)
	$(shell echo "MAKECMDGOALS=${MAKECMDGOALS}" >> ${TESTDIR}/test.log)
	$(shell echo "" >> ${TESTDIR}/test.log)

basicops: basicops_nokey
	$(shell TESTDIR="${TESTDIR}" CLUSTER="${CLUSTER}" SKIPSSL="${SKIPSSL}" $(TESTSROOTDIR)/tools/keygen.sh)
	$(shell TOTAL="${TOTAL}" $(TESTSROOTDIR)/tools/smalldelay.sh)
	$(shell TESTDIR="${TESTDIR}" CLUSTER="${CLUSTER}" TESTSROOTDIR="${TESTSROOTDIR}" COMDB2_EXE=${COMDB2_EXE} CDB2SQL_EXE=${CDB2SQL_EXE} COPYCOMDB2_EXE=${COPYCOMDB2_EXE} COMDB2AR_EXE=${COMDB2AR_EXE} CDB2VERIFY_EXE=${CDB2VERIFY_EXE} PMUX_EXE=${PMUX_EXE} PMUXPORT=${PMUXPORT} SKIP_COPY_EXE="${SKIP_COPY_EXE}" ${TESTSROOTDIR}/tools/copy_files_to_cluster.sh > ${TESTDIR}/copy_files_to_cluster.log 2>&1 || echo "exit 1 copy_files_to_cluster failed, see ${TESTDIR}/copy_files_to_cluster.log" )

showparams: 
	@echo MAKECMDGOALS=${MAKECMDGOALS}
	@echo DOALL=${DOALL}
	@echo TESTSROOTDIR=${TESTSROOTDIR}
	@echo SRCHOME=${SRCHOME}
	@echo TESTID=${TESTID}
	@echo TESTDIR=${TESTDIR}
	@echo CLUSTER=${CLUSTER}
	@echo BUILDDIR=${BUILDDIR}
	@echo COMDB2AR_EXE=${COMDB2AR_EXE}
	@echo CDB2VERIFY_EXE=${CDB2VERIFY_EXE}
	@echo COMDB2_EXE=${COMDB2_EXE}
	@echo CDB2SQL_EXE=${CDB2SQL_EXE}
	@echo COPYCOMDB2_EXE=${COPYCOMDB2_EXE}
	@echo CDB2_SQLREPLAY_EXE=${CDB2_SQLREPLAY_EXE}
	@echo PMUX_EXE=${PMUX_EXE}

%_generated: basicops
	@N=`$(TESTSROOTDIR)/tools/get_test_counter.sh` && echo TESTID=${TESTID} running in $(TESTDIR) $@ $$N/${TOTAL}
	@tools/create_generated_tests.sh `tools/get_testopts_path.sh $@` $(TESTDIR)
	@$(MAKE) -skC $(TESTDIR)/$@.test

$(patsubst %.test,%,$(ALL_TESTS)): basicops

%: %.test
	@N=`$(TESTSROOTDIR)/tools/get_test_counter.sh` && echo TESTID=${TESTID} running in $(TESTDIR) $* $$N/${TOTAL}
	@cp -r $< $(TESTDIR)/
	@$(MAKE) -skC $(TESTDIR)/$<

%.clean: %.test
	+$(MAKE) -C $(patsubst %.clean,%,$<) clean

%.setup: %.test
	+$(MAKE) -C $(patsubst %.setup,%,$<) setup

generated_tests:
	$(shell $(TESTSROOTDIR)/tools/create_generated_tests.sh)

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

clean: testclean

testclean:
	@rm -rf test_[0-9]* $(TESTDIR)/ *_generated.test/
	@$(foreach n, $(CLUSTER), ssh $(n) 'rm -rf $(TESTSROOTDIR)/test_[0-9]* $(TESTDIR)/' < /dev/null;)
