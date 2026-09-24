# Sockpool wrong-db-name test.
# The test name is short on purpose.  The database name comes from the test
# name, and it has to fit in the 48 byte sockpool type string
# "comdb2/<dbname>/<tier>/newsql/random_room".

ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif

ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=2m
endif
