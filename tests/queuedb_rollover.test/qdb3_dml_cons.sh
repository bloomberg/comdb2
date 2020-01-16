#!/usr/bin/env bash

for ((k=0;k<$1;++k)); do
    cdb2sql $SP_OPTIONS "exec procedure const3($2)" 2>&1 >/dev/null
done
