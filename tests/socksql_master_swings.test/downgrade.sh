#!/usr/bin/env bash
dbname=$1

function getmaster {
    cdb2sql --tabs ${CDB2_OPTIONS} $dbname default 'exec procedure sys.cmd.send("bdb cluster")' | grep MASTER | cut -f1 -d":" | tr -d '[:space:]'
}

while true; do
    for node in $CLUSTER ; do
        cdb2sql ${CDB2_OPTIONS} --host $node $dbname "exec procedure sys.cmd.send('downgrade')"
    done
    sleep 10
done
