#!/usr/bin/env bash

for ((k=0;k<$1;++k)); do
    cdb2sql $SP_OPTIONS "drop lua consumer const3" > /dev/null
    cdb2sql $SP_OPTIONS "create lua consumer const3 on (table foraudit3 for insert)" > /dev/null
done
