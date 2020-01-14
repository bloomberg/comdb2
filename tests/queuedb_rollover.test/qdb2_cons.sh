#!/usr/bin/env bash

for ((i=1;i<$1;++i)); do
    cdb2sql $SP_OPTIONS "exec procedure nop1()" > /dev/null
done
