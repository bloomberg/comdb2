ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif

ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=120m
endif

export DUMPLOCK_ON_TIMEOUT=1
export CORE_ON_TIMEOUT=1
export SP_OPTIONS=-s --cdb2cfg $(CDB2_CONFIG) $(DBNAME) default
