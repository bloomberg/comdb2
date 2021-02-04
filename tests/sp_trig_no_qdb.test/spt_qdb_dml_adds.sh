#!/usr/bin/env bash

for ((k=0;k<$1;++k)); do
    did_insert=$(cdb2sql $SP_OPTIONS "insert into foraudit3 values($2)")

    if [[ "$did_insert" != "(rows inserted=1)" ]]; then
        echo "failed INSERT for round $2 at iteration $k"
    fi
done
