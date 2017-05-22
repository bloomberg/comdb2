#!/bin/bash
set -e

while [[ $1 = -* ]]; do
    # TODO: handle options, if any, here
    shift
done

pmux -l

host=$(/sbin/ip route|awk '/default/ { print $3 }')
echo "hostname $host" > /opt/bb/etc/cdb2/config/comdb2.d/hostname.lrl

sleep 5

port=19000
for dbname in $*; do
    comdb2 --create $dbname >/dev/null &
    port=$(($port+1))
done
wait

port=19000
for dbname in $*; do
#   echo "port localhost $port" >> /opt/bb/var/cdb2/$dbname/$dbname.lrl
    comdb2 $dbname &
done
wait
