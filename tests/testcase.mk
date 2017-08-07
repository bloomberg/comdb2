ifeq ($(TESTSROOTDIR),)
  $(error TESTSROOTDIR not set correctly.)
endif

export CURRDIR?=$(shell pwd)
export TESTCASE=$(patsubst %.test,%,$(shell basename $(CURRDIR)))
export SRCHOME?=$(shell readlink -f $(TESTSROOTDIR)/../)
export TESTDIR?=$(TESTSROOTDIR)/test_$(TESTID)
#comdb2 does not allow db names with '_' underscore in them
export DBNAME=$(subst _,,$(TESTCASE))$(TESTID)
export DBDIR=$(TESTDIR)/$(DBNAME)
export TMPDIR=$(TESTDIR)/tmp
export CDB2_CONFIG=$(DBDIR)/comdb2db.cfg
export CDB2_OPTIONS=--cdb2cfg $(CDB2_CONFIG)
export COMDB2_ROOT=$(TESTDIR)
export comdb2ar=$(SRCHOME)/comdb2ar
export comdb2task=$(SRCHOME)/comdb2
export COMDB2_UNITTEST?=0
ifeq ($(TESTID),)
  export TESTID:=$(shell $(TESTSROOTDIR)/tools/get_random.sh)
endif
ifeq ($(COMDB2MD5SUM),)
  export COMDB2MD5SUM:=$(shell md5sum ${SRCHOME}/comdb2 | cut -d ' ' -f1)
endif


test:: tool unit
	@mkdir -p ${TESTDIR}/
	echo "Working from dir `pwd`" >> $(TESTDIR)/test.log
	$(TESTSROOTDIR)/runtestcase

clean::
	rm -f *.res

setup:
	@mkdir -p ${TESTDIR}/logs/
	@$(TESTSROOTDIR)/setup -debug
	@echo Ready to run

stop:
	@$(TESTSROOTDIR)/unsetup


tool:

unit:
