#!/usr/bin/env bash

# Cancel sql and transactions test
################################################################################

set -x 

# args
# <dbname> <autodbname> <dbdir> <testdir>
a_rdbname=$4
a_rcdb2config=$2
a_dbname=$1
a_cdb2config=$2
a_dbdir=$3
a_testdir=$4

S_CDB2_OPTIONS="--cdb2cfg ${a_cdb2config}"

# Make sure we talk to the same host
mach=`cdb2sql ${S_CDB2_OPTIONS} --tabs $a_dbname default "SELECT comdb2_host()"`

S_SQL="cdb2sql ${S_CDB2_OPTIONS} --host $mach $a_dbname"
S_TSQL="cdb2sql --tabs ${S_CDB2_OPTIONS} --host $mach $a_dbname"

function header
{
    set -x
    echo "TEST $1"
    echo "$2"
}

header 1 "running a long query, retrieve it and cancel it"
echo "Running tests on node ${mach}"
echo "inserting 60 records"
$S_SQL "insert into t select * from generate_series(1,60)"
if [[ $? != 0 ]] ; then
    echo "Failed to insert"
    exit 1
fi

echo "running async select sleep"
$S_SQL "select *, sleep(1) from t order by 1" &
if [[ $? != 0 ]] ; then
    echo "Failed to async run sleep"
    exit 1
fi

echo "collecting the sleep uuid"
uuid=`$S_TSQL "select uuid from comdb2_connections where sql like '%sleep%' and sql not like '%connections%'"`
if [[ $? != 0 ]] ; then
    echo "Failed to select uuid"
    exit 1
fi

echo "Found uuid '${uuid}'"
if [[ -z ${uuid} ]] ; then 
    echo "Failed to retrieve the uuid for sleeping query"
    exit 1
fi

echo "running cancel ${uuid} trap"
$S_SQL "exec procedure sys.cmd.cancel('cnonce', '${uuid}')"
if [[ $? != 0 ]] ; then
    echo "Failed to run cancel message"
    exit 1
fi

sleep 2

echo "check if the ${uuid} is gone"
found_uuid=`$S_TSQL "select uuid from comdb2_connections where sql like '%sleep%' and sql not like '%connections%'"`
if [[ $? != 0 ]] ; then
    echo "Failed to run cancel message"
    exit 1
fi

if [[ ! -z ${found_uuid} ]] ; then
    echo "Failed to cancel the sleep select"
    exit 1
fi

echo "Testcase passed."
