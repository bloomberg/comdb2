#!/bin/bash

cdb2sql ${CDB2_OPTIONS} ${DBNAME} default "create table t(i int) partitioned by time period 'daily' retention 2 start '2147483647'"
cdb2sql ${CDB2_OPTIONS} ${DBNAME} default "insert into t values(1)"
cdb2sql ${CDB2_OPTIONS} ${DBNAME} default "exec procedure sys.cmd.send('debug_sleep_on_partition_analyze 5')"
cdb2sql ${CDB2_OPTIONS} ${DBNAME} default "analyze t" &
waitpid=$!
sleep 1
cdb2sql ${CDB2_OPTIONS} ${DBNAME} default "drop table t"
wait ${waitpid}
