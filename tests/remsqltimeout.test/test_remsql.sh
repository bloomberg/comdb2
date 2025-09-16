#!/usr/bin/env bash

# Remote cursor moves testcase for comdb2
################################################################################


# args
# <dbname> <autodbname> <dbdir> <testdir>
a_remdbname=$1
a_remcdb2config=$2
a_dbname=$3
a_cdb2config=$4
a_dbdir=$5
a_testdir=$6

output=run.out

function kill_query
{
    echo "Kill the slow select"
    uuid=`${STM} "select uuid from comdb2_connections where sql like '%order by 1%' and sql not like '%connections%'"`
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
    ${SM} "exec procedure sys.cmd.cancel('cnonce', '${uuid}')"
    if [[ $? != 0 ]] ; then
        echo "Failed to run cancel cnonce sp"
        exit 1
    fi
}

rm ${output}

mach=`cdb2sql --cdb2cfg ${a_cdb2config} --tabs $a_dbname default "SELECT comdb2_host()"`
master=`cdb2sql --cdb2cfg ${a_cdb2config} --tabs $a_dbname default 'select host from comdb2_cluster where is_master="Y"'`

echo "Found use node ${mach} master ${master}"

if [[ -z "${mach}" ]] ; then
    echo "Failed to get a node"
    exit 1
fi
if [[ -z "${master}" ]] ; then
    echo "Failed to get master"
    exit 1
fi

ST="cdb2sql --cdb2cfg ${a_cdb2config} --tabs --host ${mach} $a_dbname"
S="cdb2sql --cdb2cfg ${a_cdb2config} --host ${mach} $a_dbname"
SM="cdb2sql --cdb2cfg ${a_cdb2config} --host ${master} $a_dbname"
STM="cdb2sql --cdb2cfg ${a_cdb2config} --tabs --host ${master} $a_dbname"
DT="cdb2sql --cdb2cfg ${a_remcdb2config} --tabs --host ${mach} $a_remdbname"
D="cdb2sql --cdb2cfg ${a_remcdb2config} --host ${mach} $a_remdbname"
DM="cdb2sql --cdb2cfg ${a_remcdb2config} --host ${master} $a_remdbname"

echo "Deleting old data"
${D} "delete from t"
if [[ $? != 0 ]] ; then
    echo "Failed to delete old data in ${a_remdbname}.t"
    exit 1
fi

echo "Insert data in remote ${a_remdbname}"
${D} "insert into t select * from generate_series(1,10)"
if [[ $? != 0 ]] ; then
    echo "Failed to insert data in ${a_remdbname}.t"
    exit 1
fi

echo "prep-ing the fdb code to test the problem at steady time"
${S} "select * from LOCAL_${a_remdbname}.t order by 1" >> $output 2>&1
if [[ $? != 0 ]] ; then
    echo "Failed to select from LOCAL_${a_remdbname}.t"
    exit 1
fi

echo "Slowing down reads on destination ${a_remdbname}"
${D} "put tunable sql_row_delay_msecs 30000"
if [[ $? != 0 ]] ; then
    echo "Failed to set tunable in ${mach}"
    exit 1
fi

for node in ${CLUSTER}; do
    cdb2sql --cdb2cfg ${a_remcdb2config} --host ${node} ${a_remdbname} "put tunable sql_row_delay_msecs 30000"
    if [[ $? != 0 ]] ; then
        echo "Failed to set tunable in ${node}"
        exit 1
    fi
done

echo "getting bdb lock with a long time taking query"
${SM} "select * from LOCAL_${a_remdbname}.t order by 1" &
if [[ $? != 0 ]] ; then
    echo "Failed to run async select on  ${mach} to ${a_remdbname}"
    exit 1
fi

echo "waiting for query to run"
sleep 5

echo "downgrading the master"
echo $(timeout 10 ${SM} "exec procedure sys.cmd.send('downgrade')")
timeout 10 ${SM} "exec procedure sys.cmd.send('downgrade')"
if [[ $? != 0 ]]; then
    echo "failed to run downgrade"
    kill_query
    exit 1
fi

kill_query

#convert the table to actual dbname
sed "s/dorintdb/${a_remdbname}/g" output.log > output.log.actual

# validate results
testcase_output=$(cat $output)
expected_output=$(cat output.log.actual)
if [[ "$testcase_output" != "$expected_output" ]]; then

   # print message
   echo "  ^^^^^^^^^^^^"
   echo "The above testcase (${testcase}) has failed!!!"
   echo " "
   echo "Use 'diff <expected-output> <my-output>' to see why:"
   echo "> diff ${PWD}/{output.log.actual,$output}"
   echo " "
   diff output.log.actual $output
   echo " "

   # quit
   exit 1
fi

echo "Testcase passed."
