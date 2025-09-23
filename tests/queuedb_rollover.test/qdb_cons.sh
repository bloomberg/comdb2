#!/usr/bin/env bash

sp=$1
from=$2
to=$3
host=$4
logfile="/tmp/queuedb_rollover.$sp"

if [ -z "$host" ];
then
    OPTIONS=$SP_OPTIONS
else
    OPTIONS="--host $host $SP_OPTIONS"
fi

rm -f $logfile $logfile.rerun
touch $logfile

for ((i = $from; i < $to; ++i)); do
    echo "exec procedure $sp()"
done | cdb2sql $OPTIONS - > /dev/null 2> $logfile

#Rerun to compensate for failure due to schemachange
n=$(cat $logfile | wc -l)
for ((i = 0; i < $n; ++i)); do
    echo "exec procedure $sp()"
done | tee $logfile.rerun | cdb2sql $OPTIONS - > /dev/null
