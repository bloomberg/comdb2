#!/usr/bin/env bash

. ${TESTSROOTDIR}/tools/write_prompt.sh

function drop_table
{
    [[ $debug == "1" ]] && set -x
    typeset func="create_table"
    write_prompt $func "Running $func"
    typeset table=${1:-t1}
    $CDB2SQL_EXE -tabs $CDB2_OPTIONS $DBNAME default "drop table $table"
}

function create_table
{
    [[ $debug == "1" ]] && set -x
    typeset func="create_table"
    write_prompt $func "Running $func"
    typeset table=${1:-t1}
    $CDB2SQL_EXE -tabs $CDB2_OPTIONS $DBNAME default "create table $table(a int)" 
}
