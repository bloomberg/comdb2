ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
export ARG=-s --cdb2cfg $(CDB2_CONFIG) $(DBNAME) default
