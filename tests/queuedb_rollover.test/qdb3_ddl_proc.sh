#!/usr/bin/env bash

for ((k=0;k<$1;++k)); do
    cdb2sql $SP_OPTIONS "drop procedure const3 version 'const_test'" 2>&1 >/dev/null
    cdb2sql $SP_OPTIONS "create procedure const3 version 'const_test' {$(cat const_consumer.lua)}" 2>&1 >/dev/null
done
