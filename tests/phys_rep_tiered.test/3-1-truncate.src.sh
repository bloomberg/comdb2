#!/usr/bin/env bash

rewind_time=$(date +%s)
# prevent commands from appearing too quick
sleep 2

for i in $(seq 1 $NRECS); do
    echo "delete from t1 where id=$i"
done | ${CDB2SQL_EXE} -s ${CDB2_OPTIONS} $dbname default - >/dev/null

sleep 1

# now call truncate
${CDB2SQL_EXE} -s ${CDB2_OPTIONS} $dbname default "exec procedure sys.cmd.truncate_time($rewind_time)"

