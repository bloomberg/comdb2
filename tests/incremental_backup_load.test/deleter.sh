#!/usr/bin/env bash

while :; do 
    cdb2sql -s ${CDB2_OPTIONS} $dbname default "delete from load where 1 limit 10" &> /dev/null
    if [[ -f ./testcase.done ]]; then
        exit 0
    fi
done
