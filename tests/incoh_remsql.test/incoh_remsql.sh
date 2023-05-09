#!/usr/bin/env bash

clients=$1
iterations=$2

#clients=1
#iterations=2

echo "Launching ${clients} clients"
for ((clnt=1; clnt<=${clients}; clnt++));  do
    echo "Running client ${clnt} for ${iterations} iterations"
    ./incoh_remsql_clnt.sh ${clnt} ${iterations} &
done

echo "Waiting for clients"
wait
echo "Done waiting"

echo "Checking for failure"
for ((clnt=1; clnt<=${clients}; clnt++)) ; do
    echo " - check failure.${clnt}"
    if [[ -f "failure.${clnt}" ]] ; then
        echo "Client ${clnt} failed!"
        exit 1
    fi
done
echo "Clients success"
