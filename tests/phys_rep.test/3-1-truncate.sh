#!/bin/bash

rewind_time=`date +%s`

for i in $(seq 1 $NRECS); do
    echo "delete from t1 where id=$i"
done | cdb2sql -s ${CDB2_OPTIONS} $dbname default - >/dev/null

# now call truncate
cdb2sql -s ${CDB2_OPTIONS} $dbname default "exec procedure truncate_time($rewind_time)"

