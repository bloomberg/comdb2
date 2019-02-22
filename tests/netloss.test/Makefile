ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
# Downloading base OS and packages can take quite a while.
export TEST_TIMEOUT=20m

# this is a local test, don't need cluster
unexport CLUSTER
export COMDB2_UNITTEST=1
