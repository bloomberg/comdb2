#!/bin/bash

# Start pmux
pmux -l

# Route core dumps into dumps volume
echo '/dumps/core.%e.%p' | tee /proc/sys/kernel/core_pattern
ulimit -c unlimited

# Start DB
comdb2 --lrl /db/testdb.lrl testdb &

# Keep Container Alive for debugging
while :; do
    sleep 1
done
