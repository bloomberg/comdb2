ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
# Loading 200k rows plus three timed arms plus a retroactive partitioning
# schema change.  30m is comfortable for the defaults and still under the 120m
# cutoff in tools/get_tests_inorder.sh, so this stays in the default sweep.
ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=30m
endif
