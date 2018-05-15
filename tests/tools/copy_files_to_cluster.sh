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

SSH_OPT="-o StrictHostKeyChecking=no "
#use connection sharing via master node
SSH_MSTR="-o ControlPath=$TESTDIR/%r%h%p"

close_master_ssh_session() {
    ssh $SSH_OPT $SSH_MSTR -O exit $node #close master ssh session
}

copy_files_to_node() {
    local node=$1
    echo "copying to node $node"
    # TRAP to close_master_ssh_session if the script is killed in mid copy
    trap "close_master_ssh_session \"closing\"" INT EXIT
      
    ssh $SSH_OPT $SSH_MSTR -MNf $node   #start master ssh session for node
    ssh $SSH_OPT $SSH_MSTR $node "mkdir -p $d1 $d2 $d3 $PMUX_DIR $TESTDIR/logs/ $TESTDIR/var/log/cdb2 $TESTDIR/tmp/cdb2" < /dev/null
    scp $SSH_OPT $SSH_MSTR $COMDB2AR_EXE $node:$COMDB2AR_EXE
    scp $SSH_OPT $SSH_MSTR $COMDB2_EXE $node:$COMDB2_EXE
    scp $SSH_OPT $SSH_MSTR $CDB2SQL_EXE $node:$CDB2SQL_EXE
    if [ -n "$RESTARTPMUX" ] ; then
        echo stop pmux on $node first before copying and starting it
        ssh $SSH_OPT $SSH_MSTR $node "$stop_pmux" < /dev/null
    fi
    set +e
    scp $SSH_OPT $SSH_MSTR $PMUX_EXE $node:$PMUX_EXE
    echo start pmux on $node if not running 
    ssh $SSH_OPT $SSH_MSTR $node "COMDB2_PMUX_FILE='$PMUX_DIR/pmux.sqlite' $pmux_cmd" < /dev/null
    ssh $SSH_OPT $SSH_MSTR -O exit $node #close master ssh session
    trap - INT EXIT  #Clear TRAP
    set -e
}

copy_files_to_cluster() 
{
    echo copying executables to each node except localhost
    local node
    local i=0
    declare -a pids
    for node in ${CLUSTER/$HOSTNAME/}; do
        if [ $node == $HOSTNAME ] ; then
            echo "Error: hostname is in the CLUSTER list -HOSTNAME"
            exit 1
        fi
        copy_files_to_node $node &
        pids[$i]=$!
        let i=i+1
    done
    i=0
    for node in ${CLUSTER/$HOSTNAME/}; do
        wait ${pids[$i]}
        let i=i+1
    done
    echo "done copying to all cluster nodes"
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
    set +e
fi
