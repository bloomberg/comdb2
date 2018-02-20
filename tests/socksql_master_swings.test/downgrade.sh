#!/bin/bash
dbname=$1

function getmaster {
    cdb2sql --tabs ${CDB2_OPTIONS} $dbname default 'exec procedure sys.cmd.send("bdb cluster")' | grep MASTER | cut -f1 -d":" | tr -d '[:space:]'
}

while true; do
    cdb2sql ${CDB2_OPTIONS} $dbname default "exec procedure sys.cmd.send('bdb cluster')"
    for i in $(seq 1 3); do
        master=`getmaster`
        echo "master is $master"
        if [[ "$master" != "" ]] ; then
            cdb2sql ${CDB2_OPTIONS} --host $master $dbname "exec procedure sys.cmd.send('downgrade')"
        fi
    done
    sleep 10
done
