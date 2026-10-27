#!/usr/bin/env bash
# Usage: stall_hook.sh <server-host> <client-pid>

host=$1
clientpid=$2
ssh_opts="-o StrictHostKeyChecking=no -o BatchMode=yes"

sockets='
echo "--- sockets on $(hostname) ---"
if command -v ss >/dev/null; then
    ss -tinmo
else
    echo "sl local rem st tx_queue:rx_queue (hex)"
    awk "NR>1 {print \$1, \$2, \$3, \$4, \$5}" /proc/net/tcp /proc/net/tcp6
fi
'

stacks='
PATH=$PATH:/opt/bb/bin
echo "--- stacks of $1 (pid $2) on $(hostname) ---"
if command -v pstack >/dev/null; then
    pstack $2
elif command -v gdb >/dev/null; then
    gdb -q -batch -p $2 -ex "thread apply all bt" 2>&1
else
    for t in /proc/$2/task/*; do echo "$t $(cat $t/comm) $(cat $t/wchan 2>/dev/null) $(cut -d" " -f3 $t/stat)"; done
fi
'

on_host() {
    if [[ -n "$CLUSTER" ]]; then
        ssh $ssh_opts $host bash -s -- "$@"
    else
        bash -s -- "$@"
    fi
}

echo "===== STALL HOOK $(date +%T.%N) server=$host client-pid=$clientpid ====="

bash -c "$sockets"
bash -c "$stacks" -- nowritetimeout $clientpid

dbpid=$(on_host <<< "pgrep -f '^[^ ]*comdb2 ${DBNAME}( |\$)' | head -1")

on_host <<< "$sockets"
if [[ -n "$dbpid" ]]; then
    on_host comdb2 $dbpid <<< "$stacks"
else
    echo "couldn't find pid of ${DBNAME} on $host"
fi

echo "===== STALL HOOK DONE $(date +%T.%N) ====="
