#!/usr/bin/env bash

#set -x

vars="HOSTNAME TESTSROOTDIR TESTDIR COMDB2_EXE CDB2SQL_EXE COMDB2AR_EXE PMUX_EXE"
for required in $vars; do
    q=${!required}
    if [[ -z "$q" ]]; then
        echo "$required not set" >&2
        exit 1
    fi
#    echo "$required=$q"
done

pmux_port=${pmux_port:-5105}
source $TESTSROOTDIR/setup.common

pmux_cmd="${PMUX_EXE} -l"
if [ -n "${PMUXPORT}" ] ; then
    pmux_port=${PMUXPORT}
    pmux_socket=/tmp/pmux.socket.${PMUXPORT}
    pmux_port_range="-r $((pmux_port+1)):$((pmux_port+200))"
    pmux_cmd="${PMUX_EXE} -l -p ${PMUXPORT} -b ${pmux_socket} ${pmux_port_range}"
fi



stop_pmux="pgrep pmux > /dev/null && (exec 3<>/dev/tcp/localhost/${pmux_port} && echo exit >&3 ) || echo PMUX DOWN"

copy_files_to_cluster() 
{
    echo copying executables to each node except localhost
    for node in $CLUSTER; do
        if [ $node == $HOSTNAME ] ; then
            continue
        fi
        ssh -o StrictHostKeyChecking=no $node "mkdir -p $d1 $d2 $d3 $PMUX_DIR $TESTDIR/logs/ $TESTDIR/var/log/cdb2 $TESTDIR/tmp/cdb2" < /dev/null
        scp -o StrictHostKeyChecking=no $COMDB2AR_EXE $node:$COMDB2AR_EXE
        scp -o StrictHostKeyChecking=no $COMDB2_EXE $node:$COMDB2_EXE
        scp -o StrictHostKeyChecking=no $CDB2SQL_EXE $node:$CDB2SQL_EXE
        if [ -n "$RESTARTPMUX" ] ; then
            echo stop pmux on $node first before copying and starting it
            ssh -o StrictHostKeyChecking=no $node "$stop_pmux" < /dev/null
        fi
        set +e
        scp -o StrictHostKeyChecking=no $PMUX_EXE $node:$PMUX_EXE
        echo start pmux on $node if not running 
        ssh -o StrictHostKeyChecking=no $node "COMDB2_PMUX_FILE='$PMUX_DIR/pmux.sqlite' $pmux_cmd" < /dev/null
        set -e
    done
}


if [ -n "$RESTARTPMUX" ] ; then
    echo stop pmux on localhost
    eval $stop_pmux
fi
echo start pmux on local host if not running
COMDB2_PMUX_FILE="$TESTSROOTDIR/pmux.sqlite" $pmux_cmd 2>&1

# if CLUSTER is length is nonzero copy to cluster
if [[ -n "$CLUSTER" ]] ; then 
    set -e
    copy_files_to_cluster
fi
