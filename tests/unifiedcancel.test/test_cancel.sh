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

echo "running cancel('uuid', '${uuid}') trap"
$S_SQL "exec procedure sys.cmd.cancel('uuid', '${uuid}')"
if [[ $? != 0 ]] ; then
    echo "Failed to run cancel uuid sp"
    exit 1
fi

sleep 2

echo "check if the ${uuid} is gone"
found_uuid=`$S_TSQL "select uuid from comdb2_connections where sql like '%sleep%' and sql not like '%connections%'"`
if [[ $? != 0 ]] ; then
    echo "Failed to select from comdb2_connections"
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
    echo "Failed to run cancel running sp"
    exit 1
fi

sleep 6

echo "check if the sleepy queries are gone"
found_uuid=`$S_TSQL "select uuid from comdb2_connections where sql like '%sleep%' and sql not like '%connections%'"`
if [[ $? != 0 ]] ; then
    echo "Failed to select from comdb2_connections"
    exit 1
fi

if [[ ! -z ${found_uuid} ]] ; then
    echo "Failed to cancel the sleep select, found $found_uuid"
    exit 1
fi

header 3 "test cancelling queued sql"
echo "Setting maxt to 1"
$S_SQL "put tunable sqlenginepool.maxt 1"
if [[ $? != 0 ]] ; then
    echo "Failed to reduce sql threads"
    exit 1
fi

echo "Starting 3 sleepy threads, running for 5 seconds each"
for cnt in {1..3} ; do
    echo "running async select sleep $cnt"
    $S_SQL "select *, sleep(1) from t order by 1 limit 5" &
    if [[ $? != 0 ]] ; then
        echo "Failed to async run sleep $cnt"
        exit 1
    fi
done

echo "Cancelling queueing threads"
$S_SQL "exec procedure sys.cmd.cancel('queued')"
if [[ $? != 0 ]] ; then
    echo "Failed to run cancel queued sp"
    exit 1
fi

echo "Waiting 6 seconds to allow running thread to go away"
sleep 6

#NOTE: this is good check, but only works if we ssh locally on that node...
#echo "Check on canceled queued sql"
#found_cnt=`$S_TSQL "select count(*) from comdb2_connections where sql like '%sleep%' and sql not like '%connections%' and name = 'queued_canceled'"`
#if [[ ! "$found_cnt" -eq "2" ]] ; then
#    echo "Failed to canceled the two queued requests"
#    exit 1
#fi

echo "Check if the threads are gone"
found_cnt=`$S_TSQL "select count(*) from comdb2_connections where sql like '%sleep%' and sql not like '%connections%'"`
if [[ ! "$found_cnt" -eq "0" ]] ; then
    echo "Failed to cancel queued requests"
fi

header 4 "test cancelling all sql"
$S_SQL "put tunable sqlenginepool.maxt 4"
if [[ $? != 0 ]] ; then
    echo "Failed to set sql threads to 4"
    exit 1
fi

echo "Starting 4 sleepy threads, running for 15/25/35/45 seconds"
$S_SQL "select 1, *, sleep(1) from t order by 1 limit 10" &
if [[ $? != 0 ]] ; then
    echo "Failed to async run sleep 1"
    exit 1
fi
for cnt in {2..4} ; do
    echo "running async select sleep $cnt"
    $S_SQL "select ${cnt}, *, sleep(1) from t order by 1 limit 20" &
    if [[ $? != 0 ]] ; then
        echo "Failed to async run sleep $cnt"
        exit 1
    fi
done

echo "Waiting a bit to allow previous 4 queries to run"
sleep 3

echo "Cancelling all requests, this will get queued, and run after 7 seconds"
$S_SQL "exec procedure sys.cmd.cancel('all')" &
if [[ $? != 0 ]] ; then
    echo "Failed to run cancel all sp"
    exit 1
fi

echo "Waiting a bit to allow cancel to queueu"
sleep 3

echo "Queueing 6 more requests"
for cnt in {5..10} ; do
    echo "running async select sleep $cnt"
    $S_SQL "select ${cnt}, *, sleep(1) from t order by 1 limit 20 " &
    if [[ $? != 0 ]] ; then
        echo "Failed to async run sleep $cnt"
        exit 1
    fi
done

echo "Waiting for one worker to become available"
sleep 15

echo "Checking if queries are gone"
found_cnt=`timeout -k 1 3 $S_TSQL "select count(*) from comdb2_connections where sql like '%sleep%' and sql not like '%connections%'"`
if [[ ! "$?" -eq "0" ]] ; then
    echo "Failed to cancel all requests, timeout"
    exit 1
fi
if [[ ! "$found_cnt" -eq "0" ]] ; then
    echo "Failed to cancel all requests"
    qqq=`$S_TSQL "select status, sql from comdb2_connections where sql like '%sleep%' and sql not like '%connections%'"`
    echo "'$qqq'"
    exit 1
fi

ooo=run.log

header 5 "test cancelling fingerprint sql"
$S_SQL "put tunable sqlenginepool.maxt 10"
echo "Running 3 slow sql"
for cnt in {1..3} ; do
    echo "running async select sleep $cnt"
    $S_SQL "select *, sleep(1) from t order by 1" &
    if [[ $? != 0 ]] ; then
        echo "Failed to async run sleep $cnt"
        exit 1
    fi
done
echo "different query"
$S_SQL "select 123, *, sleep(1) from t order by 1" &

echo "Waiting for cdb2sql to kick in"
sleep 5

echo "Picking up one query"
uuid=`$S_TSQL "select fingerprint from comdb2_connections where sql like '%sleep%' and sql not like '%connections%' and sql not like '%123$' limit 1"`
if [[ $? != 0 ]] ; then
    echo "Failed to select uuid"
    exit 1
fi

echo "Found fingerprint '${uuid}'"
if [[ -z ${uuid} ]] ; then 
    echo "Failed to retrieve the uuid for sleeping query"
    exit 1
fi

echo "running cancel('fingerprint', '${uuid}') trap"
$S_SQL "exec procedure sys.cmd.cancel('fingerprint', '${uuid}')"
if [[ $? != 0 ]] ; then
    echo "Failed to run cancel fingerprint sp"
    exit 1
fi

sleep 2

echo "check if the ${uuid} is gone"
found_uuid=`$S_TSQL "select uuid from comdb2_connections where sql like '%sleep%' and sql not like '%connections%' and uuid='${uuid}'"`
if [[ $? != 0 ]] ; then
    echo "Failed to select from comdb2_connections"
    exit 1
fi

if [[ ! -z ${found_uuid} ]] ; then
    echo "Failed to cancel the sleep select ${uuid}"
    exit 1
fi

echo "Checking if other queries are still here"
found_cnt=`timeout -k 1 3 $S_TSQL "select count(*) from comdb2_connections where sql like '%sleep%' and sql not like '%connections%'"`
if [[ ! "$?" -eq "0" ]] ; then
    echo "Failed to cancel all requests, timeout"
    exit 1
fi
if [[ ! "$found_cnt" -eq "1" ]] ; then
    echo "Failed to cancel only the specified fingerprint ${uuid} requests"
    exit 1
fi

echo "Cancelling all sql"
$S_SQL "put tunable sqlenginepool.maxt 4"
echo "Running 10 slow sql"
for cnt in {1..10} ; do
    echo "running async select sleep $cnt"
    $S_SQL "select *, sleep(1) from t order by 1" &
    if [[ $? != 0 ]] ; then
        echo "Failed to async run sleep $cnt"
        exit 1
    fi
done

echo "Waiting for cdb2sql to kick in"
sleep 5

$S_SQL "exec procedure sys.cmd.cancel('all')"
if [[ $? != 0 ]] ; then
    echo "Failed to run cancel all the other requests"
    exit 1
fi

header 6 "checking syntax arguments"

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
$S_TSQL "exec procedure sys.cmd.cancel('uuid')" >> $ooo 2>&1
if [[ $? == 0 ]] ; then
    echo "Failed no uuid cnonce"
    exit 1
fi
$S_TSQL "exec procedure sys.cmd.cancel('uuid', 123)" >> $ooo 2>&1
if [[ $? != 0 ]] ; then
    echo "Failed wrong type uuid cnonce"
    exit 1
fi
$S_TSQL "exec procedure sys.cmd.cancel('uuid', '1-23-456')" >> $ooo 2>&1
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
