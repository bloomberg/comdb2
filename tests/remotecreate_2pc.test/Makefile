export SECONDARY_DB_PREFIX=s1
export TERTIARY_DB_PREFIX=s2
export QUATERNARY_DB_PREFIX=s3

ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=10m
endif
ifeq ($(SETUP_TIMEOUT),)
	export SETUP_TIMEOUT=3m
endif
