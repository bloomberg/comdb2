# This file is included for every individual test
# Many of the variables here are set in Makefile in the comdb2/tests/ directory
# if they are not set it means that we are running make from within 
# the individual test directory thus we will need to define them


ifeq ($(TESTSROOTDIR),)
  # TESTSROOTDIR is not set when make is issued from within a test directory 
  # (will check assumption few lines later)
  # needs to expand to a full path, otherwise it propagates as '../'
  export TESTSROOTDIR=$(shell readlink -f $(PWD)/..)
  export SKIPSSL=1   #force SKIPSSL for local test -- easier to debug
endif

# check that we indeed have the correct dir in TESTSROOTDIR
ifeq ($(wildcard ${TESTSROOTDIR}/setup),)
  $(error TESTSROOTDIR is set incorrectly to ${TESTSROOTDIR} )
endif

ifeq ($(TESTID),)
  # will need a testid, unless one is provided
  export TESTID:=$(shell $(TESTSROOTDIR)/tools/get_random.sh)
endif

ifeq ($(TESTDIR),)
  #if we don't have a testdir it means we are running from within a test dir
  #we should export some globals and copy executable
  export TESTDIR:=$(TESTSROOTDIR)/test_$(TESTID)
  export SRCHOME:=$(shell readlink -f $(TESTSROOTDIR)/../)
  #also defined in Makefile -- needed here because we can run tests from .test/
  export COMDB2_EXE:=$(TESTDIR)/comdb2
  export COMDB2AR_EXE:=$(TESTDIR)/comdb2ar
  export CDB2SQL_EXE:=$(TESTDIR)/cdb2sql
  export COPYCOMDB2_EXE:=$(SRCHOME)/db/copycomdb2
  export CDB2_SQLREPLAY_EXE:=$(SRCHOME)/cdb2_sqlreplay
  export PMUX_EXE:=$(SRCHOME)/pmux
  $(shell mkdir -p ${TESTDIR}/ )
  $(shell cp $(SRCHOME)/comdb2ar $(COMDB2AR_EXE) )
  $(shell cp $(SRCHOME)/comdb2 $(COMDB2_EXE) )
  $(shell cp $(SRCHOME)/cdb2sql $(CDB2SQL_EXE) )
endif


export CURRDIR?=$(shell pwd)
export TESTCASE=$(patsubst %.test,%,$(shell basename $(CURRDIR)))
#comdb2 does not allow db names with '_' underscore in them
export DBNAME=$(subst _,,$(TESTCASE))$(TESTID)
export DBDIR=$(TESTDIR)/$(DBNAME)
export TMPDIR=$(TESTDIR)/tmp
export CDB2_CONFIG=$(abspath $(DBDIR)/comdb2db.cfg)
export CDB2_OPTIONS=--cdb2cfg $(CDB2_CONFIG)
export COMDB2_ROOT=$(TESTDIR)
export COMDB2_UNITTEST?=0


test:: tool unit
	echo "Working from dir `pwd`" >> $(TESTDIR)/test.log
	$(TESTSROOTDIR)/runtestcase
	$(MAKE) stop

clean::
	rm -f *.res

setup: tool
	@mkdir -p ${TESTDIR}/logs/
	@$(TESTSROOTDIR)/setup -debug
	@echo Ready to run

stop:

tool:

unit:
