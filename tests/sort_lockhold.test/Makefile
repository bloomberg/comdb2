ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
# Loading 300k rows of 192 tying columns, then four in-memory arms each running
# a sort of several seconds, and two spill arms.  Takes three to four minutes
# here; 10m leaves room for slower machines while staying well under the 120m
# cutoff in tools/get_tests_inorder.sh, so this stays in the default sweep.
ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=10m
endif
