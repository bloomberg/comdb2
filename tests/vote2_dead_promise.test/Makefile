ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
override NUMNODESTOUSE := 5
export NUMNODESTOUSE
ifeq ($(TEST_TIMEOUT),)
  export TEST_TIMEOUT=15m
endif
