export SECONDARY_DB_PREFIX=s1
export TERTIARY_DB_PREFIX=s2
export QUATERNARY_DB_PREFIX=s3

# The 2pc variant (twopc.testopts) picks the tier the dbs reach each other through;
# must match COORDINATOR_TIER in runit
ifneq ($(shell grep -s enable_2pc $(CURDIR)/lrl.options),)
  ifeq ($(CLUSTER),)
    export CUSTOMLRLPATH=$(CURDIR)/twopc_tier_local.lrl
  else
    export CUSTOMLRLPATH=$(CURDIR)/twopc_tier_cluster.lrl
  endif
endif

ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=5m
endif
