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
ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=30m
endif
ifeq ($(SETUP_TIMEOUT),)
	export SETUP_TIMEOUT=3m
endif
