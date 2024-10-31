#!/bin/bash

rep=$(${CDB2SQL_EXE} --tabs ${CDB2_OPTIONS} ${DBNAME} default "select host from comdb2_cluster where is_master='N' and coherent_state='coherent' limit 1")
function purge_stats
{
    ${CDB2SQL_EXE} ${DBNAME}  --host $rep "truncate sqlite_stat1"
    ${CDB2SQL_EXE} ${DBNAME}  --host $rep "truncate sqlite_stat4"
}
function count_stats 
{
    ${CDB2SQL_EXE}  ${DBNAME}  --host $rep "select count(*) from sqlite_stat1"
    ${CDB2SQL_EXE}  ${DBNAME}  --host $rep "select count(*) from sqlite_stat4"
}

purge_stats
${CDB2SQL_EXE}  ${DBNAME}  --host $rep "exec procedure sys.cmd.send('setsqlattr analyze_empty_tables off')"
${CDB2SQL_EXE}  ${DBNAME}  --host $rep "DROP TABLE IF EXISTS empty"
${CDB2SQL_EXE}  ${DBNAME}  --host $rep "CREATE TABLE empty(a int primary key)"
${CDB2SQL_EXE}  ${DBNAME}  --host $rep "ANALYZE empty"
count_stats

echo "TURNING ON ANALYZE EMPTY TABLES"
${CDB2SQL_EXE}  ${DBNAME}  --host $rep "exec procedure sys.cmd.send('setsqlattr analyze_empty_tables on')"
purge_stats
${CDB2SQL_EXE}  ${DBNAME}  --host $rep "ANALYZE empty"
count_stats

echo "INSERTING RECORDS"
${CDB2SQL_EXE}  ${DBNAME}  --host $rep "INSERT INTO EMPTY SELECT * FROM generate_series(0,49999)"
echo "RUNNING ANALYZE"
purge_stats
${CDB2SQL_EXE}  ${DBNAME}  --host $rep "ANALYZE empty"
count_stats

echo "TURNING OFF ANALYZE EMPTY TABLES"
${CDB2SQL_EXE}  ${DBNAME}  --host $rep "exec procedure sys.cmd.send('setsqlattr analyze_empty_tables off')"
echo "RUNNING ANALYZE"
purge_stats
${CDB2SQL_EXE}  ${DBNAME}  --host $rep "ANALYZE empty"
count_stats
