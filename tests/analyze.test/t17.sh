#!/bin/bash

source ${TESTSROOTDIR}/tools/runit_common.sh

set -e

main() {
	# Given
	cdb2sql ${CDB2_OPTIONS} ${DBNAME} default "create table t(i int)" > /dev/null
	cdb2sql ${CDB2_OPTIONS} ${DBNAME} default "insert into t values(1)" > /dev/null
	sendtocluster "exec procedure sys.cmd.send('debug_sleep_on_analyze 5')" > /dev/null

	# When
	cdb2sql ${CDB2_OPTIONS} ${DBNAME} default "analyze t" &
	waitpid=$!
	sleep 1
	cdb2sql ${CDB2_OPTIONS} ${DBNAME} default "drop table t" > /dev/null

	# Then
	if ! cdb2sql ${CDB2_OPTIONS} ${DBNAME} default "select 1" > /dev/null;
	then
		echo "Database not reachable"
		return 1
	fi

	if wait ${waitpid};
	then
		echo "Expected to fail analyze but passed"
		return 1
	fi
}

main
