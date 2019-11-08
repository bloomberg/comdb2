#!/usr/bin/env bash

if [[ -z "$1" ]]; then
    echo "$0 requires a dbname as the first argument"
    exit 1
fi

db=$1
shift

pathbase=${COMDB2_PATHBASE:-/home/ubuntu/comdb2}
linearizable=$pathbase/linearizable
scripts=$linearizable/scripts
$scripts/heal
ma=$(cdb2sql -tabs $db dev "exec procedure sys.cmd.send('bdb cluster')" | egrep MASTER | egrep lsn) ; m=${ma%%:*}
c=$(ssh $m "/opt/bb/bin/cdb2sql -tabs $db @localhost \"exec procedure sys.cmd.send('bdb cluster')\"")
echo "$c"

r=0
echo "$c" | egrep MASTER
if [[ $? == 0 ]]; then
    echo "$c" | egrep COHERENT
    r=$?
fi

while [[ $r == 0 ]] ; do
    echo "$(date) waiting for $db cluster to become coherent"
    $scripts/heal $db
    sleep 1
    ma=$(cdb2sql -tabs $db dev "exec procedure sys.cmd.send('bdb cluster')" | egrep MASTER | egrep lsn) ; m=${ma%%:*}
    echo "$c"
    echo "$c" | egrep MASTER
    if [[ $? == 0 ]]; then
        echo "$c" | egrep COHERENT
        r=$?
    fi
done
