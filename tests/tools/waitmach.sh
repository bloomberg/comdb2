#!/usr/bin/env bash

function waitmach
{
    [[ "$debug" == 1 ]] && set -x
    typeset func="waitmach"
    write_prompt $func "Running $func"
    typeset inmach=$1
    typeset out=""

    if [[ "$inmach" == "default" ]]; then
        mach="default"
    else
        mach="--host $inmach"
    fi

    while [[ "$out" != "1" ]]; do
        out=$($CDB2SQL_EXE ${CDB2_OPTIONS} --tabs $DBNAME $mach 'select 1' 2> /dev/null)
        sleep 1
    done
}

function wait_up
{
    [[ "$debug" == 1 ]] && set -x
    typeset func="wait_up"
    write_prompt $func "Running $func"

    if [[ -z "$CLUSTER" ]]; then
        waitmach default
    else
        for n in $CLUSTER ; do 
            waitmach $n
        done
    fi
}

