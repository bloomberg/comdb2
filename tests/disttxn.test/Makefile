export SECONDARY_DB_PREFIX=r1
export TERTIARY_DB_PREFIX=r2
export QUATERNARY_DB_PREFIX=r3
export QUINARY_DB_PREFIX=r4
export SENARY_DB_PREFIX=cdb2

export CHECK_DB_AT_FINISH=0
ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif

ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=30m
endif
