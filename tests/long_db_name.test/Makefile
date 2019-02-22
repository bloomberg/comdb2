ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif
ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=1m
endif

LOCRAND=$(shell echo $(TESTID) | sha256sum | cut -f1 -d' ')
export DBNAME=$(shell echo "longdbname$(TESTID)$(LOCRAND)" | cut -b 1-63)
#will start it's own pmux
#export PMUXPORT=12345

stop:
	# shutdown pmux that was started for us at port $PMUXPORT
	#$(shell $(TESTSROOTDIR)/tools/send_msg_port.sh "exit" $(PMUXPORT) )
	#@$(foreach n, $(CLUSTER), ssh $(n) bash 'exec 3<>/dev/tcp/localhost/${PMUXPORT} && echo exit >&3' < /dev/null;)
	#@$(foreach n, $(CLUSTER), ssh $(n) 'rm -rf /tmp/pmux.socket.$(PMUXPORT)' < /dev/null;)

tool:
	make -skC $(TESTSROOTDIR)/tools cdb2api_caller

CDB2LIBS=$(SYSPATH) $(SRCHOME)/cdb2api/libcdb2api.a -lprotobuf-c -lpthread -lssl -lcrypto
CXXFLAGS+=-D_GNU_SOURCE -Wall -I$(SRCHOME)/cdb2api -I$(SRCHOME)/bbinc

