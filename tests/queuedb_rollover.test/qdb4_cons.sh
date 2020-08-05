#!/usr/bin/env bash

for ((k=0;k<$1*2;++k)); do
    cdb2sql $SP_OPTIONS "exec procedure nop2()" >/dev/null
done
