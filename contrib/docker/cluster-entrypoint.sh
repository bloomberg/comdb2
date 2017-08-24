#!/bin/bash

# Start pmux
pmux -l

# Core Pattern matches host machine
# If you really need cores, change your
# host machines' /proc/sys/kernel/core_pattern
# to something like /dumps/$HOSTNAME/core.%h.%e.%p
ulimit -c unlimited

# Start DB
comdb2 --lrl /db/testdb.lrl testdb &

# Keep Container Alive for debugging
while :; do
    sleep 1
done
