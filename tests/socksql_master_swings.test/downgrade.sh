#!/bin/zsh

set -x 

dbname="$1"

while true; do
    currmaster="$(cdb2sql --tabs ${CDB2_OPTIONS} $dbname default 'exec procedure sys.cmd.send("bdb cluster")' | grep MASTER | cut -f1 -d":" | tr -d '[:space:]')"
    if [ "$currmaster" -eq "$hostname" ]; then
        cdb2sql ${CDB2_OPTIONS} $dbname "exec procedure sys.cmd.send('downgrade')"
    fi
done
