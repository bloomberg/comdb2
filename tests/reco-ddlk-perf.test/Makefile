ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
# Two full arms of the reco-ddlk-sql workload back to back.  Default runs
# (RECO_DDLK_PERF_RUNS=1) measured 7.5, 9.0, 9.2 and 12.0 minutes single-node,
# so 15m catches a stall quickly but leaves only a few minutes of headroom over
# the slowest of those -- raise it if the sweep starts timing out here rather
# than failing for a real reason.  Staying under 120m also keeps the test in
# $(ALL_TESTS), so it runs in the default sweep and `make reco-ddlk-perf` works
# without extra flags -- see tools/get_tests_inorder.sh, which drops tests at
# or above 120.
#
# Raise the timeout on the command line when asking for more cycles, since
# RECO_DDLK_PERF_RUNS=N multiplies the runtime by N.  Do not spell the timeout
# variable out again in this file: get_tests_inorder.sh greps the Makefile for
# it and does not skip comments, so a second mention breaks the test listing.
ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=15m
endif
