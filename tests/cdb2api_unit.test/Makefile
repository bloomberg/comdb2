COMDB2_UNITTEST=1
ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif

ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=1m
endif


#test_exe: test.c $(SRCHOME)/cdb2api/cdb2api.c $(SRCHOME)/cdb2api/cdb2api.h $(SRCHOME)/build/protobuf/sqlquery.pb-c.h
#	gcc -D_LINUX_SOURCE -g -std=c99 $(CURRDIR)/test.c -I$(SRCHOME)/bb -I$(SRCHOME)/bbinc -I$(SRCHOME)/build/protobuf -I$(SRCHOME)/cdb2api -o runit

unit: tools/cdb2_unit
