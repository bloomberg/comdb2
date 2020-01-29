#!/usr/bin/env bash

for ((k=0;k<$1;++k)); do
    cdb2sql $SP_OPTIONS "insert into foraudit3 values($2)" &>/dev/null
done
