ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=1m
endif

LOCRAND=$(shell echo $(TESTID) | md5sum | cut -f1 -d' ')
export DBNAME=reallylongnametotestfunctioning$(LOCRAND)
export PMUXPORT=12345

stop:
	# shutdown pmux that was started for us at port $PMUXPORT
	$(shell $(TESTSROOTDIR)/tools/send_msg_port.sh "exit" $(PMUXPORT) )

tool:
	make -skC $(TESTSROOTDIR)/tools cdb2api_caller

CDB2LIBS=$(SYSPATH) $(SRCHOME)/cdb2api/libcdb2api.a -lprotobuf-c -lpthread -lssl -lcrypto
CXXFLAGS+=-D_GNU_SOURCE -Wall -I$(SRCHOME)/cdb2api -I$(SRCHOME)/bbinc

