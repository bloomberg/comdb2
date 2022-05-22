#!/usr/bin/env bash

set -e

CDB2SQL_EXE="cdb2sql -tabs $@ default "
TRANSACTION_SIZE=5000
TRANSACTION_COUNT=20
SQL_FILE="upsert_effects.sql"
LOG_FILE="upsert_effects.log"
RESULT=

# prepare
function prepare {
    echo "prepare: creating table t1.."
    ${CDB2SQL_EXE} "DROP TABLE IF EXISTS t1" 2>&1
    ${CDB2SQL_EXE} "CREATE TABLE t1(i INT UNIQUE, j INT)" 2>&1

    rm -f ${SQL_FILE}
    rm -f ${LOG_FILE}

    echo "BEGIN" > $SQL_FILE
    for i in `seq 1 $TRANSACTION_SIZE`; do
        echo "INSERT INTO t1 VALUES($i, $i) ON CONFLICT (i) DO UPDATE SET j = $i" >> $SQL_FILE
    done
    echo "COMMIT" >> $SQL_FILE
}

# teardown
function teardown {
    echo "teardown: dropping table t1.."
    ${CDB2SQL_EXE} "DROP TABLE t1" 2>&1
}

# verify the results
function verify {
    echo "verify: verifying test results.."

    GOT=$(cat ${LOG_FILE} | wc -l)
    if [ $GOT -eq $TRANSACTION_COUNT ]; then
        echo "    Test passed!"
        RESULT=0
    else
        echo "    Test failed! Got: $GOT, Expected: $TRANSACTION_COUNT"
        RESULT=1
    fi
}

# stress (execute transactions in parallel)
function stress {
    echo "stress: running load.."
    for (( i=0; i<$TRANSACTION_COUNT; i++ )) {
        cdb2sql -showeffects -f ${SQL_FILE} $@ default | grep "Number of rows affected ${TRANSACTION_SIZE}" >> $LOG_FILE &
    }
    wait
}

prepare
stress $@
verify
teardown
exit $RESULT
