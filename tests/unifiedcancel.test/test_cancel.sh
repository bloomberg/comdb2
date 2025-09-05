#!/usr/bin/env bash

# Cancel sql and transactions test
################################################################################

set -x 

# args
# <dbname> <autodbname> <dbdir> <testdir>
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

function waitforsql
{
    local cnt=0
    local mx=$1
    local itr=0

    while true; do 
        cnt=`$S_TSQL "select count(*) from comdb2_connections where sql like '%sleep%' and sql not like '%connections%'"`
        echo "Found $d queries iteration $itr"
        if [[ "$cnt" -eq "$mx" ]] ; then
            echo "Done waiting after $itr iterations"
            break;
        fi
        let itr=itr+1
        sleep 1
    done
}

echo "Running tests on node ${mach}"
echo "inserting 60 records"
$S_SQL "insert into t select * from generate_series(1,60)"
if [[ $? != 0 ]] ; then
    echo "Failed to insert"
    exit 1
fi

header 1 "running a long query, retrieve it and cancel it"
echo "running async select sleep"
$S_SQL "select *, sleep(1) from t order by 1" &
if [[ $? != 0 ]] ; then
    echo "Failed to async run sleep"
    exit 1
fi

waitforsql 1

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

echo "running cancel('cnonce', '${uuid}') trap"
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


header 2 "running three long queries, and cancel all running"

for cnt in {1..3} ; do
    echo "running async select sleep $cnt"
    $S_SQL "select *, sleep(1) from t order by 1" &
    if [[ $? != 0 ]] ; then
        echo "Failed to async run sleep $cnt"
        exit 1
    fi
done

waitforsql 3

echo "running cancel('running') trap"
$S_SQL "exec procedure sys.cmd.cancel('running')"
if [[ $? != 0 ]] ; then
    echo "Failed to run cancel message"
    exit 1
fi

sleep 6

echo "check if the sleepy queries are gone"
found_uuid=`$S_TSQL "select uuid from comdb2_connections where sql like '%sleep%' and sql not like '%connections%'"`
if [[ $? != 0 ]] ; then
    echo "Failed to run cancel message"
    exit 1
fi

if [[ ! -z ${found_uuid} ]] ; then
    echo "Failed to cancel the sleep select, found $found_uuid"
    exit 1
fi

ooo=run.log

header 3 "checking syntax arguments"

$S_TSQL "exec procedure sys.cmd.cancel()" > $ooo 2>&1
if [[ $? == 0 ]] ; then
    echo "Failed no type"
    exit 1
fi
$S_TSQL "exec procedure sys.cmd.cancel(123)" >> $ooo 2>&1
if [[ $? == 0 ]] ; then
    echo "Failed wrong non-string type"
    exit 1
fi
$S_TSQL "exec procedure sys.cmd.cancel('blah')" >> $ooo 2>&1
if [[ $? == 0 ]] ; then
    echo "Failed bogus string type"
    exit 1
fi
$S_TSQL "exec procedure sys.cmd.cancel('cnonce')" >> $ooo 2>&1
if [[ $? == 0 ]] ; then
    echo "Failed no uuid cnonce"
    exit 1
fi
$S_TSQL "exec procedure sys.cmd.cancel('cnonce', 123)" >> $ooo 2>&1
if [[ $? != 0 ]] ; then
    echo "Failed wrong type uuid cnonce"
    exit 1
fi
$S_TSQL "exec procedure sys.cmd.cancel('cnonce', '1-23-456')" >> $ooo 2>&1
if [[ $? != 0 ]] ; then
    echo "Failed invalid uuid cnonce"
    exit 1
fi

#convert the table to actual dbname
sed "s/dorintdb/${a_dbname}/g" output.log > output.log.actual

# validate results
testcase_output=$(cat ${ooo})
expected_output=$(cat output.log.actual)
if [[ "$testcase_output" != "$expected_output" ]]; then

   # print message
   echo "  ^^^^^^^^^^^^"
   echo "The above testcase (${testcase}) has failed!!!"
   echo " "
   echo "Use 'diff <expected-output> <my-output>' to see why:"
   echo "> diff ${PWD}/{${ooo}, output.log.actual}"
   echo " "
   diff ${ooo} output.log.actual
   echo " "

   # quit
   exit 1
fi

echo "Testcase passed."
