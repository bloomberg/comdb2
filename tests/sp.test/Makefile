ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif

export SP_OPTIONS=-s --cdb2cfg $(CDB2_CONFIG) $(DBNAME) default
