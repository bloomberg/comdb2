ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif

# Make stale borrowed strings fail deterministically on the system allocators
# used by CI (glibc) and local macOS development.
export MALLOC_PERTURB_=165
export MallocScribble=1

ifeq ($(TEST_TIMEOUT),)
  export TEST_TIMEOUT=3m
endif
