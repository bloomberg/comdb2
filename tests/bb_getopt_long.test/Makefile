COMDB2_UNITTEST=1
ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif

ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=1m
endif

glgetopt: getopt_test.c $(SRCHOME)/util/bb_getopt_long.c $(SRCHOME)/util/bb_getopt_long.h
	gcc -g -DNORMAL_GETOPT -std=c99 $(CURRDIR)/getopt_test.c $(SRCHOME)/util/bb_getopt_long.c -I$(SRCHOME)/util -o glgetopt

bbgetopt: getopt_test.c $(SRCHOME)/util/bb_getopt_long.c $(SRCHOME)/util/bb_getopt_long.h
	gcc -g -std=c99 $(CURRDIR)/getopt_test.c $(SRCHOME)/util/bb_getopt_long.c -I$(SRCHOME)/util -o bbgetopt

unit: glgetopt bbgetopt
	
clean::
	rm -f glgetopt bbgetopt *bb *gl
