#!/bin/bash

# Exit immediately if a command fails.
set -e

while [[ $1 = -* ]]; do
    # TODO: handle options, if any, here
    shift
done

# Start pmux
pmux -l
if [ $? -ne 0 ]; then
    echo 'pmux failed to start, exiting..'
    exit 1
fi

host=$(hostname -i)
echo "hostname $host" > /opt/bb/etc/cdb2/config/comdb2.d/hostname.lrl

# Initialize the database(s)
for dbname in $*; do
    if [ ! -d /opt/bb/var/cdb2/$dbname ]; then
        comdb2 --create $dbname >/dev/null &
    else
        echo 'Skipping initialization of database $dbname, it already exists!'
    fi
done
wait

for dbname in $*; do
    comdb2 $dbname &
done
wait

# exec "$@"
