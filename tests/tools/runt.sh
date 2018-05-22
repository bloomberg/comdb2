#!/usr/bin/env bash

# set -x
export CLUSTER="node1 node2 node3 node4 node5"
export SETUP_WARN=1
export testname=${1:-jepsen_register_nemesis}
export user=$(whoami)
i=0
r=0

# Kill the *TEST* after this amount of time

while [[ $r == 0 ]]; do
    let i=i+1
    echo "iteration $i $(date)"
    make $testname 2>&1 | tee out 

    egrep "success" out > /dev/null 2>&1
    r=$?

    if [[ $r -ne 0 ]]; then
        egrep "timeout" out > /dev/null 2>&1
        if [[ $? == 0 ]]; then 
            echo "Continuing on testcase timeout"
            r=0
        fi
    fi
done

