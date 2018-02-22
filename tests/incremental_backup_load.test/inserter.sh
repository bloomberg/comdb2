#!/usr/bin/env bash

insertsql=${TMPDIR}/insert.$$.$RANDOM.sql
echo "BEGIN" > $insertsql
c=0
# If these are too big we run the risk of having an open transaction
# while we are trying to do a checkpoint, which could set the recovertolsn
# value prior to the output that we're trying to verify
while [[ $c -lt 10 ]]; do
    echo "INSERT INTO load VALUES ($c, 'xxx', x'1234', x'1234', x'1234', x'1234', x'1234', x'1234', x'1234', x'1234')" >> $insertsql
    let c=c+1
done
echo "COMMIT" >> $insertsql

while :; do 
    $CDB2SQL_EXE -s ${CDB2_OPTIONS} -f $insertsql $dbname default &> /dev/null
    if [[ -f ./testcase.done ]]; then
        exit 0
    fi
done

