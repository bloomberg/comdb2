#!/usr/bin/env bash

for ((i=1;i<$1;++i)); do
    cdb2sql $SP_OPTIONS "insert into foraudit2 values(${i})" > /dev/null
done
