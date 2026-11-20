#!/usr/bin/env bash
# Run once by tests/Makefile, before any test, so that tests which set
# COMDB2_UBSAN=1 can run concurrently without each copying (and overwriting a
# running) binary. Touches UBSAN_CLUSTER_STAMP on success; testcase.mk fails
# UBSan tests without it.

comdb2=${COMDB2_UBSAN_BUILDDIR}/db/comdb2

if [[ -z "$CLUSTER" ]]; then
    echo "no CLUSTER, nothing to copy"
    exit 0
fi
if [[ ! -f "$comdb2" ]]; then
    echo "$comdb2 not found, nothing to copy"
    exit 0
fi

SSH_OPT="-o StrictHostKeyChecking=no"
SSH_MSTR="-o ControlPath=$TESTDIR/%r%h%p.ubsan"

copy_to_node() {
    local node=$1
    ssh $SSH_OPT $SSH_MSTR -MNf $node || return 1
    trap "ssh $SSH_OPT $SSH_MSTR -O exit $node" RETURN
    ssh $SSH_OPT $SSH_MSTR $node "mkdir -p $(dirname $comdb2)" < /dev/null &&
    scp $SSH_OPT $SSH_MSTR "$comdb2" $node:$comdb2
}

declare -A pids
for node in $CLUSTER; do
    [[ $node == $HOSTNAME ]] && continue
    echo "copying UBSan comdb2 to $node"
    copy_to_node $node &
    pids[$node]=$!
done

rc=0
for node in "${!pids[@]}"; do
    if ! wait ${pids[$node]}; then
        echo "copy to $node failed"
        rc=1
    fi
done

if [[ $rc -eq 0 ]]; then
    touch "$UBSAN_CLUSTER_STAMP"
    echo "done copying UBSan comdb2 to all cluster nodes"
fi
exit $rc
