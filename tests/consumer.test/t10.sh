#!/bin/bash

set -x 
tier="default"

dbname=$(${TESTSBUILDDIR}/cdb2api_base ${DBNAME} ${tier} "select comdb2_dbname()")
cluster=$(${TESTSBUILDDIR}/cdb2api_base ${DBNAME} ${tier} "select comdb2_sysinfo('cluster')")

# set up pingpong consumer

${TESTSBUILDDIR}/cdb2api_base ${DBNAME} ${tier} 'create table if not exists pingpong (i int)'
${TESTSBUILDDIR}/cdb2api_base ${DBNAME} ${tier} "create procedure pingpong version 'cdb2api_test' {local function main(a, b) local c = db:consumer() c:emit(a .. '-' .. b) end}"
${TESTSBUILDDIR}/cdb2api_base ${DBNAME} ${tier} 'create lua consumer pingpong for (table pingpong on update)'
${TESTSBUILDDIR}/cdb2api_base ${DBNAME} ${tier} 'drop table pingpong'

procout=$(${TESTSBUILDDIR}/cdb2api_base ${DBNAME} ${tier} "exec procedure pingpong($dbname, $cluster)")

if [[ "$procout" != "$dbname-$cluster" ]]; then
    echo "consumer not emitting expected: $dbname-$cluster, got: $procout"
    exit 1
fi
