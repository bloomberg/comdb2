# This file is included for every individual test
# Many of the variables here are set in Makefile in the comdb2/tests/ directory
# if they are not set it means that we are running make from within 
# the individual test directory thus we will need to define them


ifeq ($(TESTSROOTDIR),)
  # TESTSROOTDIR is not set so we assume ths was called from within 
  # a specific test directory (will check assumption few lines later)
  # needs to expand to a full path, otherwise it propagates as '../'
  export TESTSROOTDIR=$(shell readlink -f $(PWD)/..)
  export SKIPSSL=1
endif

ifeq ($(wildcard ${TESTSROOTDIR}/setup),)
  # check that we indeed have the correct dir in TESTSROOTDIR
  $(error TESTSROOTDIR is set incorrectly to ${TESTSROOTDIR} )
endif

ifeq ($(TESTID),)
  # will need a testid, unless one is provided
  export TESTID:=$(shell $(TESTSROOTDIR)/tools/get_random.sh)
endif

export CURRDIR?=$(shell pwd)
export TESTCASE=$(patsubst %.test,%,$(shell basename $(CURRDIR)))
export SRCHOME?=$(shell readlink -f $(TESTSROOTDIR)/../)
export TESTDIR?=$(TESTSROOTDIR)/test_$(TESTID)
#comdb2 does not allow db names with '_' underscore in them
export DBNAME=$(subst _,,$(TESTCASE))$(TESTID)
export DBDIR=$(TESTDIR)/$(DBNAME)
export TMPDIR=$(TESTDIR)/tmp
export CDB2_CONFIG=$(abspath $(DBDIR)/comdb2db.cfg)
export CDB2_OPTIONS=--cdb2cfg $(CDB2_CONFIG)
export COMDB2_ROOT=$(TESTDIR)
export COMDB2_UNITTEST?=0

export COMDB2_EXE?=$(SRCHOME)/comdb2
export COMDB2AR_EXE?=$(SRCHOME)/comdb2ar
export CDB2SQL_EXE?=$(SRCHOME)/cdb2sql
export COPYCOMDB2_EXE?=$(SRCHOME)/db/copycomdb2
export CDB2_SQLREPLAY_EXE?=$(SRCHOME)/cdb2_sqlreplay
export PMUX_EXE?=$(SRCHOME)/pmux

ifeq ($(COMDB2MD5SUM),)
  # record md5sum so we can verify from setup of each individual test
  export COMDB2MD5SUM:=$(shell md5sum ${COMDB2_EXE} | cut -d ' ' -f1)
endif


test:: tool unit
	@mkdir -p ${TESTDIR}/
	echo "Working from dir `pwd`" >> $(TESTDIR)/test.log
	$(TESTSROOTDIR)/runtestcase

clean::
	rm -f *.res

setup: tool
	@mkdir -p ${TESTDIR}/logs/
	@$(TESTSROOTDIR)/setup -debug
	@echo Ready to run

stop:
	@$(TESTSROOTDIR)/unsetup


tool:

unit:
