COMDB2_UNITTEST=1
ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif

unit:
	gcc $(CURRDIR)/test.c $(SRCHOME)/util/int_overflow.c -o test -I$(SRCHOME)/bbinc
