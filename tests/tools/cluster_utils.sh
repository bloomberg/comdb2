#!/usr/bin/env bash

. ${TESTSROOTDIR}/tools/write_prompt.sh
. ${TESTSROOTDIR}/tools/waitmach.sh

if [[ -z "$sleeptime" ]]; then 
    sleeptime=5
fi

function get_master
{
    [[ "$debug" == 1 ]] && set -x
    typeset func="get_master"
    x=$($CDB2SQL_EXE $CDB2_OPTIONS --tabs $DBNAME default 'exec procedure sys.cmd.send("bdb cluster")' | grep MASTER | cut -f1 -d":" | tr -d '[:space:]')
    echo "$x"
}

function bounce_cluster
{
    [[ "$debug" == 1 ]] && set -x
    typeset func="bounce_cluster"
    typeset sleeptime=${1:-5}
    write_prompt $func "Running $func"
    for node in $CLUSTER ; do
        $CDB2SQL_EXE $CDB2_OPTIONS --tabs $DBNAME --host $n "exec procedure sys.cmd.send(\"exit\")"
    done
    sleep $sleeptime

    for node in $CLUSTER ; do
        PARAMS="$DBNAME --no-global-lrl"
        CMD="sleep $sleeptime ; source ${TESTDIR}/replicant_vars ; ${COMDB2_EXE} ${PARAMS} --lrl $DBDIR/${DBNAME}.lrl -pidfile ${TMPDIR}/${DBNAME}.pid"
        if [ $node == $(hostname) ] ; then
            (
                kill -9 $(cat ${TMPDIR}/${DBNAME}.${node}.pid)
                sleep $sleeptime
                ${DEBUG_PREFIX} ${COMDB2_EXE} ${PARAMS} --lrl $DBDIR/${DBNAME}.lrl -pidfile ${TMPDIR}/${DBNAME}.${node}.pid 2>&1 | gawk '{ print strftime("%H:%M:%S>"), $0; fflush(); }' >$TESTDIR/logs/${DBNAME}.${node}.db 2>&1
            ) &
        else
            kill -9 $(cat ${TMPDIR}/${DBNAME}.${node}.pid)
            ssh -o StrictHostKeyChecking=no -tt $node ${DEBUG_PREFIX} ${CMD} 2>&1 </dev/null > >(gawk '{ print strftime("%H:%M:%S>"), $0; fflush(); }' >> $TESTDIR/logs/${DBNAME}.${node}.db) &
            echo $! > ${TMPDIR}/${DBNAME}.${node}.pid
        fi
    done
}

function bounce_local
{
    [[ "$debug" == 1 ]] && set -x
    typeset func="bounce_local"
    typeset sleeptime=${1:-5}
    write_prompt $func "Running $func"
    $CDB2SQL_EXE $CDB2_OPTIONS --tabs $DBNAME default "exec procedure sys.cmd.send(\"exit\")"
    sleep $sleeptime
    (
        PARAMS="$DBNAME --no-global-lrl"
        kill -9 $(cat ${TMPDIR}/${DBNAME}.pid)
        sleep $sleeptime
        ${DEBUG_PREFIX} ${COMDB2_EXE} $PARAMS --lrl $DBDIR/${DBNAME}.lrl -pidfile ${TMPDIR}/${DBNAME}.pid 2>&1 | gawk '{ print strftime("%H:%M:%S>"), $0; fflush(); }' >>$TESTDIR/logs/${DBNAME}.db &
    ) &
}

function bounce_database
{
    [[ "$debug" == 1 ]] && set -x
    typeset func="bounce_database"
    typeset sleeptime=${1:-5}
    write_prompt $func "Running $func"

    if [[ -n "$CLUSTER" ]]; then
        bounce_cluster $sleeptime
    else
        bounce_local $sleeptime
    fi

    wait_up
}
