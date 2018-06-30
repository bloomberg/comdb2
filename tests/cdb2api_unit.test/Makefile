COMDB2_UNITTEST=1
ifeq ($(TESTSROOTDIR),)
  include ../testcase.mk
else
  include $(TESTSROOTDIR)/testcase.mk
endif

ifeq ($(TEST_TIMEOUT),)
	export TEST_TIMEOUT=1m
endif


#test_exe: test.c $(SRCHOME)/cdb2api/cdb2api.c $(SRCHOME)/cdb2api/cdb2api.h $(BUILDDIR)/protobuf/sqlquery.pb-c.h
#	gcc -D_LINUX_SOURCE -g -std=c99 $(CURRDIR)/test.c -I$(SRCHOME)/util -I$(SRCHOME)/bbinc -I$(BUILDDIR)/protobuf -I$(SRCHOME)/cdb2api -o runit

unit: test_get_comdb2db_hosts

test_get_comdb2db_hosts: unit_get_comdb2db_hosts.c
	> test_get_comdb2db_hosts.c
	echo "#include <stdint.h>" >> test_get_comdb2db_hosts.c
	echo '#include "sqlresponse.pb-c.h"' >> test_get_comdb2db_hosts.c
	echo '#include "sqlquery.pb-c.h"' >> test_get_comdb2db_hosts.c
	echo "typedef struct cdb2_query_list_item cdb2_query_list;" >> test_get_comdb2db_hosts.c
	echo "typedef struct sbuf2 SBUF2;" >> test_get_comdb2db_hosts.c

	grep "define MAX_CNONCE_LEN " $(SRCHOME)/cdb2api/cdb2api.c >> test_get_comdb2db_hosts.c
	grep "define MAX_NODES " $(SRCHOME)/cdb2api/cdb2api.c >> test_get_comdb2db_hosts.c
	grep "define MAX_CONTEXTS " $(SRCHOME)/cdb2api/cdb2api.c >> test_get_comdb2db_hosts.c
	grep "define MAX_STACK " $(SRCHOME)/cdb2api/cdb2api.c >> test_get_comdb2db_hosts.c
	sed "/struct context_messages {/,/^}/!d" $(SRCHOME)/cdb2api/cdb2api.c >> test_get_comdb2db_hosts.c
	sed "/struct cdb2_hndl {/,/^}/!d" $(SRCHOME)/cdb2api/cdb2api.c >> test_get_comdb2db_hosts.c
	cat unit_get_comdb2db_hosts.c >> test_get_comdb2db_hosts.c
	sed "/static int get_comdb2db_hosts(/,/^}/!d" $(SRCHOME)/cdb2api/cdb2api.c >> test_get_comdb2db_hosts.c
	gcc -D_LINUX_SOURCE -g -std=c99 -I$(BUILDDIR)/protobuf -I$(SRCHOME)/util -I$(SRCHOME)/bbinc -I$(SRCHOME)/cdb2api $(CURRDIR)/test_get_comdb2db_hosts.c -o test_get_comdb2db_hosts
