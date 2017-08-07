#!/bin/bash
# This runs analyze and makes sure that getting count(*)
# in stat1 and stat4 succeeds (can rely on a value?
# and after backout there should be half as many?)

args=$1
dbname=$2
set -e

cdb2sql ${CDB2_OPTIONS} $dbname default "select count(*) from sqlite_stat1" > stat1_count.res
cdb2sql ${CDB2_OPTIONS} $dbname default "select count(*) from sqlite_stat4" > stat4_count.res
cdb2sql ${CDB2_OPTIONS} $dbname default "select 1" > /dev/null
if [ $? != 0 ] ; then
    echo FAILED
    exit 0
fi

echo SUCCESS

