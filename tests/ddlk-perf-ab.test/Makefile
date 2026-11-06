ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
# Two full arms of the reco-ddlk-sql workload back to back.  Default runs
# (DDLK_RUNS=1) measured 7.5 to 12 minutes single-node and about 6 on an idle
# 3-node cluster, but the reader scans slow down 5-6x in the parallel robo
# sweep, where the off arm alone took 12 minutes and 15m timed out -- hence
# 45m.  Staying under 120m also keeps the test in
# $(ALL_TESTS), so it runs in the default sweep and `make ddlk-perf-ab` works
# without extra flags -- see tools/get_tests_inorder.sh, which drops tests at
# or above 120.
#
# Raise the timeout on the command line when asking for more cycles, since
# DDLK_RUNS=N multiplies the runtime by N.  Do not spell the timeout
# variable out again in this file: get_tests_inorder.sh greps the Makefile for
# it and does not skip comments, so a second mention breaks the test listing.
ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=45m
endif
