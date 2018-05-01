#!/usr/bin/env bash

while :; do 
    $CDB2SQL_EXE -s ${CDB2_OPTIONS} $dbname default "update load set data=x'2222' where 1 limit 100" &> /dev/null
    $CDB2SQL_EXE -s ${CDB2_OPTIONS} $dbname default "update load set name='yyy' where 1 limit 100" &> /dev/null
    if [[ -f ./testcase.done ]]; then
        exit 0
    fi
done
