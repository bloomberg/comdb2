#!/usr/bin/env bash
# parent-transaction failure test for the cdb2tcm testsuite

set -e

args=$1
dbnm=$2
log=$3


# local variables
isrmt=
mch=
ptn=${TESTSBUILDDIR}/ptrantest

master=`cdb2sql -tabs ${CDB2_OPTIONS} $dbnm default 'exec procedure sys.cmd.send("bdb cluster")' | grep MASTER | cut -f1 -d":" | tr -d '[:space:]'`

# Enable rowlocks
#${SRCHOME}/comdb2sc $dbnm rowlocks enable_master_only >/dev/null 2>&1
cdb2sql ${CDB2_OPTIONS} $dbnm --host $master 'put rowlocks enable' &> out.txt
cdb2sql ${CDB2_OPTIONS} $dbnm default "exec procedure sys.cmd.send('debug tcmtest ptranfail on')" &> out.txt
cdb2sql ${CDB2_OPTIONS} $dbnm --host $master "exec procedure sys.cmd.send('debug tcmtest ptranfail on')" &> out.txt

# simply run the test
$ptn $dbnm -s
