#!/usr/bin/env bash

exec 2>&1
db=$2

for t in t{1,2,3,4,5};
do
    cdb2sql ${CDB2_OPTIONS} $db default "truncate $t"
done
