#!/bin/bash

while :; do 
    cdb2sql ${CDB2_OPTIONS} $dbname default "delete from load where 1 limit 10" >/dev/null 2>&1
    if [[ -f ./testcase.done ]]; then
        exit 0
    fi
done
