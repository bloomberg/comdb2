ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=1m
endif

LOCRAND=$(shell echo $RANDOM | md5sum | cut -f1 -d' ')
export DBNAME=reallylongnametotestfunctioning$(LOCRAND)
export PMUXPORT=12345

stop:
	@$(TESTSROOTDIR)/unsetup
	#shutdown pmux LOCRAND=$(shell echo $RANDOM | md5sum | cut -f1 -d' ')


tool:
	make -skC $(TESTSROOTDIR)/tools cdb2api_caller

CDB2LIBS=$(SYSPATH) $(SRCHOME)/cdb2api/libcdb2api.a -lprotobuf-c -lpthread -lssl -lcrypto
CXXFLAGS+=-D_GNU_SOURCE -Wall -I$(SRCHOME)/cdb2api -I$(SRCHOME)/bbinc

