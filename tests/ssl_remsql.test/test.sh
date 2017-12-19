#!/usr/bin/env bash

# args
# <dbname> <dbdir> <testdir> <autodbname> <autodbnum> <cluster> <task>
echo "main db vars"
vars="A B C"
for required in $vars; do
    q=${!required}
    echo "$required=$q" 
    if [[ -z "$q" ]]; then
        echo "$required not set" >&2
        exit 1
    fi
done

