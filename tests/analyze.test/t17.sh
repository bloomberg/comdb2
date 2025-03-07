#!/bin/bash

set -e

run_sql_on_cluster() {
	local -r sql=$1

	if [ -z "${CLUSTER}" ];
	then
		cdb2sql ${CDB2_OPTIONS} ${DBNAME} default "${sql}"
	else
		for node in ${CLUSTER};
		do
			cdb2sql --host ${node} ${CDB2_OPTIONS} ${DBNAME} default "${sql}"
		done
	fi
}

main() {
	# Given
	cdb2sql ${CDB2_OPTIONS} ${DBNAME} default "create table t(i int)" > /dev/null
	cdb2sql ${CDB2_OPTIONS} ${DBNAME} default "insert into t values(1)" > /dev/null
	run_sql_on_cluster "exec procedure sys.cmd.send('debug_sleep_on_analyze 5')" > /dev/null

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
