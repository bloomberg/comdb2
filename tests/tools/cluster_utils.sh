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

function bounce_node
{
    [[ "$debug" == 1 ]] && set -x
    typeset func="bounce_node"
    typeset node=${1}
    typeset sleeptime=${2:-5}
    write_prompt $func "Running $func"
    $CDB2SQL_EXE $CDB2_OPTIONS --tabs $DBNAME --host $node "exec procedure sys.cmd.send(\"exit\")"
    sleep $sleeptime
    if [ $node == $(hostname) ] ; then
        (
            kill -9 $(cat ${TMPDIR}/${DBNAME}.${node}.pid)
            sleep $sleeptime
            ${DEBUG_PREFIX} ${COMDB2_EXE} ${PARAMS} --lrl $DBDIR/${DBNAME}.lrl -pidfile ${TMPDIR}/${DBNAME}.${node}.pid 2>&1 | gawk '{ print strftime("%H:%M:%S>"), $0; fflush(); }' >$TESTDIR/logs/${DBNAME}.${node}.db 2>&1
        ) &
    else
        PARAMS="$DBNAME --no-global-lrl"
        CMD="sleep $sleeptime ; source ${TESTDIR}/replicant_vars ; ${COMDB2_EXE} ${PARAMS} --lrl $DBDIR/${DBNAME}.lrl -pidfile ${TMPDIR}/${DBNAME}.pid"
        kill -9 $(cat ${TMPDIR}/${DBNAME}.${node}.pid)
        ssh -o StrictHostKeyChecking=no -tt $node ${DEBUG_PREFIX} ${CMD} 2>&1 </dev/null > >(gawk '{ print strftime("%H:%M:%S>"), $0; fflush(); }' >> $TESTDIR/logs/${DBNAME}.${node}.db) &
        echo $! > ${TMPDIR}/${DBNAME}.${node}.pid
    fi
}

function bounce_cluster
{
    [[ "$debug" == 1 ]] && set -x
    typeset func="bounce_cluster"
    typeset sleeptime=${1:-5}
    write_prompt $func "Running $func"
    for node in $CLUSTER ; do
        $CDB2SQL_EXE $CDB2_OPTIONS --tabs $DBNAME --host $n "exec procedure sys.cmd.send(\"exit\")" &
    done
    wait
    sleep $sleeptime

    REP_ENV_VARS="${DBDIR}/replicant_env_vars"
    for node in $CLUSTER ; do
        PARAMS="$DBNAME --no-global-lrl"
        CMD="sleep $sleeptime ; source ${REP_ENV_VARS} ; ${COMDB2_EXE} ${PARAMS} --lrl $DBDIR/${DBNAME}.lrl --pidfile ${TMPDIR}/${DBNAME}.pid"
        if [ $node == $(hostname) ] ; then
            (
                kill -9 $(cat ${TMPDIR}/${DBNAME}.${node}.pid)
                mv --backup=numbered $LOGDIR/${DBNAME}.db $LOGDIR/${DBNAME}.db.1
                sleep $sleeptime
                ${DEBUG_PREFIX} ${COMDB2_EXE} ${PARAMS} --lrl $DBDIR/${DBNAME}.lrl --pidfile ${TMPDIR}/${DBNAME}.${node}.pid 2>&1 | gawk '{ print strftime("%H:%M:%S>"), $0; fflush(); }' >$TESTDIR/logs/${DBNAME}.${node}.db 2>&1
            ) &
        else
            kill -9 $(cat ${TMPDIR}/${DBNAME}.${node}.pid)
            mv --backup=numbered $LOGDIR/${DBNAME}.${node}.db $LOGDIR/${DBNAME}.${node}.db.1
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
        ${DEBUG_PREFIX} ${COMDB2_EXE} $PARAMS --lrl $DBDIR/${DBNAME}.lrl --pidfile ${TMPDIR}/${DBNAME}.pid 2>&1 | gawk '{ print strftime("%H:%M:%S>"), $0; fflush(); }' >>$TESTDIR/logs/${DBNAME}.db &
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
}

function kill_by_pidfile() {
    pidfile=$1
    if [[ -f $pidfile ]]; then
        local pid=$(cat $pidfile)
        ps -p $pid -o args | grep -q "comdb2 ${DBNAME}"
        if [[ $? -eq 0 ]]; then
            echo "kill -9 $pid"
            kill -9 $pid
        fi
        rm -f $pidfile
    else
        failexit "kill_by_pidfile: pidfile $pidfile does not exist"
    fi
}


function kill_restart_node
{
    node=$1
    if [ -z "$node" ] ; then # if not set
        failexit "kill_restart_node: needs node to be passed in as parameter"
    fi
    delay=$2
    if [ -z "$delay" ] ; then # if not set
        delay=0
    fi
    dowait=$3
    if [ -z "$dowait" ] ; then
        dowait=1
    fi

    pushd $DBDIR
    # cdb2sql ${CDB2_OPTIONS} --tabs --host $node $DBNAME  'exec procedure sys.cmd.send("flush")'
    export LOGDIR=$TESTDIR/logs

    if [ -n "$CLUSTER" ] ; then
        kill_by_pidfile ${TMPDIR}/${DBNAME}.${node}.pid
        mv --backup=numbered $LOGDIR/${DBNAME}.${node}.db $LOGDIR/${DBNAME}.${node}.db.1
        sleep $delay
        if [ $node == `hostname` ] ; then
            PARAMS="--no-global-lrl --lrl $DBDIR/${DBNAME}.lrl --pidfile ${TMPDIR}/${DBNAME}.${node}.pid"
            $COMDB2_EXE ${DBNAME} ${PARAMS} &> $LOGDIR/${DBNAME}.${node}.db &
        else
            PARAMS="--no-global-lrl --lrl $DBDIR/${DBNAME}.lrl --pidfile ${TMPDIR}/${DBNAME}.${node}.pid"
            CMD="cd ${DBDIR}; source ${REP_ENV_VARS} ; $COMDB2_EXE ${DBNAME} ${PARAMS} 2>&1 | tee $TESTDIR/${DBNAME}.db"
            ssh -n -o StrictHostKeyChecking=no -tt $node ${CMD} &> $LOGDIR/${DBNAME}.${node}.db &
            echo $! > ${TMPDIR}/${DBNAME}.${node}.pid
        fi
    else
        kill_by_pidfile ${TMPDIR}/${DBNAME}.pid
        mv --backup=numbered $LOGDIR/${DBNAME}.db $LOGDIR/${DBNAME}.db.1
        sleep $delay
        echo "$DBNAME: starting single node"
        PARAMS="--no-global-lrl --lrl $DBDIR/${DBNAME}.lrl --pidfile ${TMPDIR}/${DBNAME}.pid"
        echo "$COMDB2_EXE ${DBNAME} ${PARAMS} &> $LOGDIR/${DBNAME}.db"
        $COMDB2_EXE ${DBNAME} ${PARAMS} &> $LOGDIR/${DBNAME}.db &
    fi

    popd

    if [[ "$dowait" == "1" ]]; then
        waitmach $node
    fi
}

function kill_restart_secondary_node
{
    node=$1
    if [ -z "$node" ] ; then # if not set
        failexit "kill_restart_node: needs node to be passed in as parameter"
    fi
    delay=$2
    if [ -z "$delay" ] ; then # if not set
        delay=0
    fi

    pushd $SECONDARY_DBDIR
    # cdb2sql ${CDB2_OPTIONS} --tabs --host $node $SECONDARY_DBNAME  'exec procedure sys.cmd.send("flush")'
    export LOGDIR=$TESTDIR/logs

    if [ -n "$CLUSTER" ] ; then
        kill_by_pidfile ${TMPDIR}/${SECONDARY_DBNAME}.${node}.pid
        mv --backup=numbered $LOGDIR/${SECONDARY_DBNAME}.${node}.db $LOGDIR/${SECONDARY_DBNAME}.${node}.db.1
        sleep $delay
        if [ $node == `hostname` ] ; then
            PARAMS="--no-global-lrl --lrl $SECONDARY_DBDIR/${SECONDARY_DBNAME}.lrl --pidfile ${TMPDIR}/${SECONDARY_DBNAME}.${node}.pid"
            $COMDB2_EXE ${SECONDARY_DBNAME} ${PARAMS} &> $LOGDIR/${SECONDARY_DBNAME}.${node}.db &
        else
            PARAMS="--no-global-lrl --lrl $SECONDARY_DBDIR/${SECONDARY_DBNAME}.lrl --pidfile ${TMPDIR}/${SECONDARY_DBNAME}.${node}.pid"
            CMD="cd ${SECONDARY_DBDIR}; source ${REP_ENV_VARS} ; $COMDB2_EXE ${SECONDARY_DBNAME} ${PARAMS} 2>&1 | tee $TESTDIR/${SECONDARY_DBNAME}.db"
            ssh -n -o StrictHostKeyChecking=no -tt $node ${CMD} &> $LOGDIR/${SECONDARY_DBNAME}.${node}.db &
            echo $! > ${TMPDIR}/${SECONDARY_DBNAME}.${node}.pid
        fi
    else
        kill_by_pidfile ${TMPDIR}/${SECONDARY_DBNAME}.pid
        mv --backup=numbered $LOGDIR/${SECONDARY_DBNAME}.db $LOGDIR/${SECONDARY_DBNAME}.db.1
        sleep $delay
        echo "$SECONDARY_DBNAME: starting single node"
        PARAMS="--no-global-lrl --lrl $SECONDARY_DBDIR/${SECONDARY_DBNAME}.lrl --pidfile ${TMPDIR}/${SECONDARY_DBNAME}.pid"
        echo "$COMDB2_EXE ${SECONDARY_DBNAME} ${PARAMS} &> $LOGDIR/${SECONDARY_DBNAME}.db"
        $COMDB2_EXE ${SECONDARY_DBNAME} ${PARAMS} &> $LOGDIR/${SECONDARY_DBNAME}.db &
    fi

    popd

    waitmach $node $SECONDARY_DBNAME
}

function kill_restart_tertiary_node
{
    node=$1
    if [ -z "$node" ] ; then # if not set
        failexit "kill_restart_node: needs node to be passed in as parameter"
    fi
    delay=$2
    if [ -z "$delay" ] ; then # if not set
        delay=0
    fi

    pushd $TERTIARY_DBDIR
    # cdb2sql ${CDB2_OPTIONS} --tabs --host $node $TERTIARY_DBNAME  'exec procedure sys.cmd.send("flush")'
    export LOGDIR=$TESTDIR/logs

    if [ -n "$CLUSTER" ] ; then
        kill_by_pidfile ${TMPDIR}/${TERTIARY_DBNAME}.${node}.pid
        mv --backup=numbered $LOGDIR/${TERTIARY_DBNAME}.${node}.db $LOGDIR/${TERTIARY_DBNAME}.${node}.db.1
        sleep $delay
        if [ $node == `hostname` ] ; then
            PARAMS="--no-global-lrl --lrl $TERTIARY_DBDIR/${TERTIARY_DBNAME}.lrl --pidfile ${TMPDIR}/${TERTIARY_DBNAME}.${node}.pid"
            $COMDB2_EXE ${TERTIARY_DBNAME} ${PARAMS} &> $LOGDIR/${TERTIARY_DBNAME}.${node}.db &
        else
            PARAMS="--no-global-lrl --lrl $TERTIARY_DBDIR/${TERTIARY_DBNAME}.lrl --pidfile ${TMPDIR}/${TERTIARY_DBNAME}.${node}.pid"
            CMD="cd ${TERTIARY_DBDIR}; source ${REP_ENV_VARS} ; $COMDB2_EXE ${TERTIARY_DBNAME} ${PARAMS} 2>&1 | tee $TESTDIR/${TERTIARY_DBNAME}.db"
            ssh -n -o StrictHostKeyChecking=no -tt $node ${CMD} &> $LOGDIR/${TERTIARY_DBNAME}.${node}.db &
            echo $! > ${TMPDIR}/${TERTIARY_DBNAME}.${node}.pid
        fi
    else
        kill_by_pidfile ${TMPDIR}/${TERTIARY_DBNAME}.pid
        mv --backup=numbered $LOGDIR/${TERTIARY_DBNAME}.db $LOGDIR/${TERTIARY_DBNAME}.db.1
        sleep $delay
        echo "$TERTIARY_DBNAME: starting single node"
        PARAMS="--no-global-lrl --lrl $TERTIARY_DBDIR/${TERTIARY_DBNAME}.lrl --pidfile ${TMPDIR}/${TERTIARY_DBNAME}.pid"
        echo "$COMDB2_EXE ${TERTIARY_DBNAME} ${PARAMS} &> $LOGDIR/${TERTIARY_DBNAME}.db"
        $COMDB2_EXE ${TERTIARY_DBNAME} ${PARAMS} &> $LOGDIR/${TERTIARY_DBNAME}.db &
    fi

    popd

    waitmach $node $TERTIARY_DBNAME
}

function wait_for_cluster
{
    if [ -z "$CLUSTER" ]; then
        waitmach default
    else
        for node in ${CLUSTER} ; do
            waitmach ${node}
        done
    fi
}
