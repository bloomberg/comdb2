#!/usr/bin/env bash

pathbase=${COMDB2_PATHBASE:-/home/ubuntu/comdb2}
scripts=$pathbase/linearizable/scripts
. $scripts/setvars
clustertest=$pathbase/linearizable/killcluster
ctest=$pathbase/ctest

db=$1
shift

if [[ -z "$db" ]]; then
    echo "Requires a dbname"
    exit 1
fi

sigstop=""
if [[ "$1" == "-p" ]]; then
    sigstop="-p"
fi

echo "inserting initial records"
$ctest/test -d $db -S 2000000 -Y 

iteration=0
while :; do 
    let iteration=iteration+1
    $scripts/blockcoherent.sh $db
    echo "$(date) : running kill-cluster test for $db iteration $iteration"
    $clustertest/killclustertest.sh $db $sigstop
    if [[ $? != 0 ]]; then
        echo "Error in iteration $iteration"
        exit 1
    fi
done
