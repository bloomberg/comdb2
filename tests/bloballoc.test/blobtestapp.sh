#!/usr/bin/env bash

# arguments 
args=$1
dbnm=$2
wrkd=$3
inp=$4
rmt=$5
log=$6

# local variables
blbt=./blob

cdb2sql ${CDB2_OPTIONS} $dbnm default "drop table tbl" &> /dev/null
cdb2sql ${CDB2_OPTIONS} $dbnm default "create table tbl  { `cat blob.csc2 ` }"
exec $blbt 4 $dbnm default ${CDB2_CONFIG}
