#!/bin/bash

function purge_stats
{
    ${CDB2SQL_EXE} ${CDB2_OPTIONS} ${DBNAME} local "truncate sqlite_stat1"
    ${CDB2SQL_EXE} ${CDB2_OPTIONS} ${DBNAME} local "truncate sqlite_stat4"
}
function count_stats 
{
    ${CDB2SQL_EXE} ${CDB2_OPTIONS} ${DBNAME} local "select count(*) from sqlite_stat1"
    ${CDB2SQL_EXE} ${CDB2_OPTIONS} ${DBNAME} local "select count(*) from sqlite_stat4"
}

purge_stats
#default behavior
${CDB2SQL_EXE} ${CDB2_OPTIONS} ${DBNAME} local "DROP TABLE IF EXISTS empty"
${CDB2SQL_EXE} ${CDB2_OPTIONS} ${DBNAME} local "CREATE TABLE empty(a int primary key)"
${CDB2SQL_EXE} ${CDB2_OPTIONS} ${DBNAME} local "ANALYZE empty"
count_stats

echo "TURNING ON ANALYZE EMPTY TABLES"
${CDB2SQL_EXE} ${CDB2_OPTIONS} ${DBNAME} local "exec procedure sys.cmd.send('setsqlattr analyze_empty_tables on')"
purge_stats
${CDB2SQL_EXE} ${CDB2_OPTIONS} ${DBNAME} local "ANALYZE empty"
count_stats

echo "INSERTING RECORDS"
${CDB2SQL_EXE} ${CDB2_OPTIONS} ${DBNAME} local "INSERT INTO EMPTY SELECT * FROM generate_series(0,49999)"
echo "RUNNING ANALYZE"
purge_stats
${CDB2SQL_EXE} ${CDB2_OPTIONS} ${DBNAME} local "ANALYZE empty"
count_stats

echo "TURNING OFF ANALYZE EMPTY TABLES"
${CDB2SQL_EXE} ${CDB2_OPTIONS} ${DBNAME} local "exec procedure sys.cmd.send('setsqlattr analyze_empty_tables off')"
echo "RUNNING ANALYZE"
purge_stats
${CDB2SQL_EXE} ${CDB2_OPTIONS} ${DBNAME} local "ANALYZE empty"
count_stats
