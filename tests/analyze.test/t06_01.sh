#!/bin/bash
# This script runs analyze and sends analyze abort to cancel it 
# The analyse abort command needs to be sent to the same node 
# which is performing analyze.

args=$1
dbname=$2

cdb2sql ${CDB2_OPTIONS} $dbname default "select count(*) from t1" > t06_01.req.res.1
cdb2sql ${CDB2_OPTIONS} $dbname default "truncate sqlite_stat1" >> t06_01.req.res.1
cdb2sql ${CDB2_OPTIONS} $dbname default "truncate sqlite_stat4" >> t06_01.req.res.1

cdb2sql ${CDB2_OPTIONS} $dbname default 'exec procedure sys.cmd.analyze("t1")' > t06_01.req.res.2 &
sleep 0.02
cdb2sql ${CDB2_OPTIONS} $dbname default 'exec procedure sys.cmd.send("analyze abort")' > t06_01.req.res.3

wait

if ! diff t06_01.req.res.3 t06_01.req.out.3 ; then
    echo "FAILED abort output"
    exit 0
fi

if ! diff t06_01.req.res.2 t06_01.req.out.2 > /dev/null && 
   ! diff t06_01.req.res.2 t06_01.req.out.2_alt > /dev/null ; then
    echo "FAILED analyze output"
    exit 0
fi

c1=`cdb2sql --tabs ${CDB2_OPTIONS} $dbname default "select count(*) from sqlite_stat1"`
c2=`cdb2sql --tabs ${CDB2_OPTIONS} $dbname default "select count(*) from sqlite_stat4"`

if [[ $c1 != 0 ]] && [[ $c2 != 0 ]] ; then
    echo "$c1 != 0 or $c2 != 0"
    echo FAILED
    exit 0
fi

cdb2sql ${CDB2_OPTIONS} $dbname default "select 1" > /dev/null
if [ $? != 0 ] ; then
    echo FAILED
    exit 0
fi

echo SUCCESS
