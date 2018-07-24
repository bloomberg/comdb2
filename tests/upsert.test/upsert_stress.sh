#!/usr/bin/env bash

set -e

CDB2SQL_EXE="cdb2sql -tabs $@ default "
WORK_COUNT=50
WORKER_COUNT=50
RESULT=

# prepare
function prepare {
    echo "prepare: creating table t1.."
    ${CDB2SQL_EXE} "DROP TABLE IF EXISTS t1" 2>&1
    ${CDB2SQL_EXE} "CREATE TABLE t1(i INT UNIQUE, j INT)" 2>&1
}

# teardown
function teardown {
    echo "teardown: dropping table t1.."
    ${CDB2SQL_EXE} "DROP TABLE t1" 2>&1
}

# verify the results
function verify {
    echo "verify: verifying test results.."
    COUNT=`${CDB2SQL_EXE} "SELECT j FROM t1"` 2>&1
    EXPECTED=`echo ${WORK_COUNT}*${WORKER_COUNT} | bc`

    if [ $(echo "$COUNT==$EXPECTED" | bc) -eq 1 ]; then
        echo "    Test passed!"
        RESULT=0
    else
        echo "    Test failed! Got: $COUNT, Expected: $EXPECTED"
        RESULT=1
    fi
}

# the actual work that each worker perform (insert conflicting records)
function do_work {
    for (( i=0; i<$WORK_COUNT; i++ )) {
        #echo "Worker $1 INSERT-ing ${i}.."
        ${CDB2SQL_EXE} "INSERT INTO t1 VALUES(1,1) ON CONFLICT(i) DO UPDATE SET j=j+1" > /dev/null
    }
}

# stress (start the workers)
function stress {
    echo "stress: running load.."
    for (( i=0; i<$WORKER_COUNT; i++ )) {
        do_work $i &
    }
    wait
}

prepare
stress
verify
teardown
exit $RESULT
