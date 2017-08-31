#!/bin/bash

while :; do 
    cdb2sql ${CDB2_OPTIONS} $dbname default "update load set data=x'2222' where 1 limit 100" >/dev/null 2>&1
    cdb2sql ${CDB2_OPTIONS} $dbname default "update load set name='yyy' where 1 limit 100" >/dev/null 2>&1
    if [[ -f ./testcase.done ]]; then
        exit 0
    fi
done
