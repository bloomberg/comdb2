#!/usr/bin/env bash

# parent-transaction failure test for the cdb2tcm testsuite

# arguments

args=$1
dbnm=$2
wrkd=$3
inp=$4
rmt=$5
log=$6

# archcode function
function myarch
{
    u=$(uname)
    a="<unknown>"
    [[ "$u" == "SunOS" ]]   && a="sundev1"
    [[ "$u" == "AIX" ]]     && a="ibm"
    [[ "$u" == "HP-UX" ]]   && a="hp"
    [[ "$u" == "Linux" ]]   && a="linux"

    echo $a
    exit 0
}

# local variables
arch=$(myarch)
isrmt=
mch=
ptn=${TESTSBUILDDIR}/ptrantest

# cdb2test makes sure the master is listed first, followed by our reader
rmt=${rmt%%:*}

# use cdb2sql to send it
cdb2sql ${CDB2_OPTIONS} $dbnm default "exec procedure sys.cmd.send('debug tcmtest ptranfail on')" &> ptranfail_set.out

# simply run the test
$ptn $dbnm
