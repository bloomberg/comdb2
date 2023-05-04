#!/usr/bin/env bash

sp=$1
from=$2
to=$3
logfile="/tmp/queuedb_rollover.$sp"

rm -f $logfile $logfile.rerun

for ((i = $from; i < $to; ++i)); do
    echo "exec procedure $sp()"
done | cdb2sql $SP_OPTIONS - > /dev/null 2> $logfile

#Rerun to compensate for failure due to schemachange
n=$(cat $logfile | wc -l)
for ((i = 0; i < $n; ++i)); do
    echo "exec procedure $sp()"
done | tee $logfile.rerun | cdb2sql $SP_OPTIONS - > /dev/null
