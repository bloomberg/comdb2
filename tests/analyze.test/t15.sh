#!/bin/bash

function purge_stats
{
    ${CDB2SQL_EXE} ${CDB2_OPTIONS} ${DBNAME} default "truncate sqlite_stat1"
    ${CDB2SQL_EXE} ${CDB2_OPTIONS} ${DBNAME} default "truncate sqlite_stat4"
}
function count_stats 
{
    ${CDB2SQL_EXE} ${CDB2_OPTIONS} ${DBNAME} default "select count(*) from sqlite_stat1"
    ${CDB2SQL_EXE} ${CDB2_OPTIONS} ${DBNAME} default "select count(*) from sqlite_stat4"
}

function setup_table {
${CDB2SQL_EXE} ${CDB2_OPTIONS} ${DBNAME} default "DROP TABLE IF EXISTS t15"
#create partitioned table
${CDB2SQL_EXE} ${CDB2_OPTIONS} ${DBNAME} default "CREATE TABLE t15(a int primary key) PARTITIONED BY MANUAL RETENTION 2 START 1"
#rollout 
${CDB2SQL_EXE} ${CDB2_OPTIONS} ${DBNAME} default "PUT COUNTER t15 INCREMENT"
#insert records
${CDB2SQL_EXE} ${CDB2_OPTIONS} ${DBNAME} default "INSERT INTO t15 SELECT * FROM generate_series(0,49999)"
${CDB2SQL_EXE} ${CDB2_OPTIONS} ${DBNAME} default "PUT COUNTER t15 SET 1"
#insert more records -> after this both partitions should have records
${CDB2SQL_EXE} ${CDB2_OPTIONS} ${DBNAME} default "INSERT INTO t15 SELECT * FROM generate_series(50000, 99999)"
}


#test analyzing partitioned table
setup_table
purge_stats
${CDB2SQL_EXE} ${CDB2_OPTIONS} ${DBNAME} default "ANALYZE t15"
count_stats


#test analyzeing individual shard
SHARDNAME=$(${CDB2SQL_EXE} -tabs ${CDB2_OPTIONS} ${DBNAME} default "select shardname from comdb2_timepartshards where name='t15' limit 1")
purge_stats
${CDB2SQL_EXE} ${CDB2_OPTIONS} ${DBNAME} default "ANALYZE \"$SHARDNAME\""
count_stats
