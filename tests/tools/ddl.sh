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

function truncate_table
{
    [[ $debug == "1" ]] && set -x
    typeset func="truncate_table"
    write_prompt $func "Running $func"
    typeset table=${1:-t1}
    $CDB2SQL_EXE -tabs $CDB2_OPTIONS $DBNAME default "truncate table $table"
}

function create_table
{
    [[ $debug == "1" ]] && set -x
    typeset func="create_table"
    write_prompt $func "Running $func"
    typeset table=${1:-t1}
    $CDB2SQL_EXE -tabs $CDB2_OPTIONS $DBNAME default "create table $table(a int)" 
}

function create_index
{
    [[ $debug == "1" ]] && set -x
    typeset func="create_index"
    write_prompt $func "Running $func"
    typeset table=${1:-t1}
    typeset column=${2:-a}
    typeset ixname=${2:-ix1}
    $CDB2SQL_EXE -tabs $CDB2_OPTIONS $DBNAME default "create index $ixname on $table($column)" 
}

function create_unique_index
{
    [[ $debug == "1" ]] && set -x
    typeset func="create_index"
    write_prompt $func "Running $func"
    typeset table=${1:-t1}
    typeset column=${2:-a}
    typeset ixname=${2:-uniqix1}
    $CDB2SQL_EXE -tabs $CDB2_OPTIONS $DBNAME default "create unique index $ixname on $table($column)" 
}
