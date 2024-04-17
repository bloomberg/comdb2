ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=1m
endif

LOCRAND=$(shell echo $(TESTID) | sha256sum | cut -f1 -d' ')
export DBNAME=$(shell echo "longdbname$(TESTID)$(LOCRAND)" | cut -b 1-63)
