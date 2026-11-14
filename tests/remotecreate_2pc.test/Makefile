export SECONDARY_DB_PREFIX=s1
export TERTIARY_DB_PREFIX=s2
export QUATERNARY_DB_PREFIX=s3
export CHECK_DB_AT_FINISH=0

# The tier dbs reach each other through; must match COORDINATOR_TIER in runit
ifeq ($(CLUSTER),)
  export CUSTOMLRLPATH=$(CURDIR)/tier_local.lrl
else
  export CUSTOMLRLPATH=$(CURDIR)/tier_cluster.lrl
endif

ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
# about 4 minutes on one node; node restarts take longer on a cluster.
# tools/get_tests_inorder.sh greps the single export line below.
ifeq ($(TEST_TIMEOUT),)
  export TEST_TIMEOUT=10m
  ifneq ($(CLUSTER),)
    TEST_TIMEOUT:=20m
  endif
endif
ifeq ($(SETUP_TIMEOUT),)
	export SETUP_TIMEOUT=3m
endif
