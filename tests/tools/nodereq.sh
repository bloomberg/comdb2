#!/usr/bin/env bash

db=$1
n=$2

if [[ -z "$db" || -z "$n" ]]; then
    echo "Usage: $0 <dbname> <node>"
    exit 1
fi

# Create table
/home/mhannum/comdb2/cdb2sql $db @$n "create table t1 { schema { int a } }" >/dev/null 2>&1

i=0
while :; do
    let i=i+1
    echo "iteration $i $(date)"
    echo $(./runsql.sh $db $n)
    sleep 1
done
