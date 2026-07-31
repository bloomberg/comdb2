#!/bin/bash

# Entrypoint for the 'dev' (client) container.
#
# With no arguments it stays up so you can `docker compose exec` into it and run
# cdb2sql against the cluster. With arguments it runs them as a one-off command,
# so `docker compose run --rm dev cdb2sql testdb default "select 1"` works too.

if [ "$#" -gt 0 ]; then
    exec "$@"
fi

while :; do
    sleep 1
done
