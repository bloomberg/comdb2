#!/usr/bin/env bash
bash -n "$0" | exit 1

dbname=${DBNAME}
tier=default
drain=${TESTSBUILDDIR}/cdb2api_drain


${drain} ${dbname} ${tier}
if [[ $? -ne 0 ]]; then
    echo >&2 'drain - fail'
    exit 1
fi

echo 'drain - pass'
exit 0
