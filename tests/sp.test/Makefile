ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
ifeq ($(TEST_TIMEOUT),)
  export TEST_TIMEOUT=15m
endif

export SP_OPTIONS=-s --cdb2cfg $(CDB2_CONFIG) $(DBNAME) default

tool:
	make -skC $(TESTSROOTDIR)/tools bound
