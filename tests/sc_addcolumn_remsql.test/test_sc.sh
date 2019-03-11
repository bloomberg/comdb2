#!/usr/bin/env bash

# Remote cursor moves testcase for comdb2
################################################################################

set -x 

function fdbinfo
{
    echo cdb2sql --tabs --host $mach $a_dbname "exec procedure sys.cmd.send('fdb info db')"
    cdb2sql --tabs --host $mach $a_dbname "exec procedure sys.cmd.send('fdb info db')" >> $output 2>&1
    if (( $? != 0 )) ; then
        echo "Failed to retrieved fdb info ${a_dbname}"
        exit 1
    fi
}

function tblver
{
    echo cdb2sql --cdb2cfg ${a_remcdb2config} ${a_remdbname} default "select table_version('${tbl}')"
    cdb2sql --cdb2cfg ${a_remcdb2config} ${a_remdbname} default "select table_version('${tbl}')" >> $output
    if (( $? != 0 )) ; then
        echo "Failed to retrieved version for ${a_remdbname}"
        exit 1
    fi
}

function alter
{
    csc=$1
    echo cdb2sql --cdb2cfg ${a_remcdb2config} $a_remdbname default "alter table ${tbl} { `cat ${csc}` }"
    cdb2sql --cdb2cfg ${a_remcdb2config} $a_remdbname default "alter table ${tbl} { `cat ${csc}` }"
    if (( $? != 0 )) ; then
        echo "Failed to alter schema to ${csc}"
        exit 1
    fi

    fdbinfo 
    tblver
}

function sel
{
    col=$1
    echo cdb2sql --host $mach ${a_dbname} "select ${col} from LOCAL_${a_remdbname}.${tbl} order by ${col1}"
    cdb2sql --host $mach ${a_dbname} "select ${col} from LOCAL_${a_remdbname}.${tbl} order by ${col1}" >> $output 2>&1
    if (( $? != 0 )) ; then
        echo "Failed to select rows from ${a_dbname}"
        exit 1
    fi
}

# args
# <dbname> <autodbname> <dbdir> <testdir>
a_remdbname=$1
a_remcdb2config=$2
a_dbname=$3
a_dbdir=$4
a_testdir=$5

output=run.out
col1="a"
col2="b"
csc1="t1.csc2"
csc2="t2.csc2"
tbl="t"

sed "s/ t / LOCAL_${a_remdbname}.t /g" output.log.src > output.log
test1=0
test2=1000

if (( $# > 5 )); then
    test1=$6
    if (( $# > 6 )); then
        test2=$7
    else
        test2=${test1}
    fi
fi

# Make sure we talk to the same host
echo cdb2sql --tabs --cdb2cfg ${a_remcdb2config} ${a_remdbname} default "SELECT comdb2_host()"
mach=`cdb2sql --tabs --cdb2cfg ${a_remcdb2config} ${a_remdbname} default "SELECT comdb2_host()"`
if (( $? != 0 )); then
    echo "Failed to get a machine"
    exit 1
fi

test=1
if (( $test >= $test1 && $test<= $test2 )) ; then

echo cdb2sql -s --cdb2cfg ${a_remcdb2config} ${a_remdbname} "create table ${tbl} {`cat ${csc1}`}"
cdb2sql -s --cdb2cfg ${a_remcdb2config} ${a_remdbname} default "create table ${tbl} {`cat ${csc1}`}" > $output 2>&1
if (( $? != 0 )); then
    echo "Fail to create table ${tbl} for ${a_remdbname}"
    exit 1
fi

fi
let test=test+1
if (( $test >= $test1 && $test<= $test2 )) ; then

# populate table on remote
cdb2sql -s --cdb2cfg ${a_remcdb2config} $a_remdbname default - < insdata.req >> $output 2>&1

fdbinfo
tblver

fi
let test=test+1
if (( $test >= $test1 && $test<= $test2 )) ; then

# retrieve data through remote sql
sel ${col1}

fdbinfo
tblver

fi
let test=test+1
if (( $test >= $test1 && $test<= $test2 )) ; then

# alter schema remote, add a column
alter ${csc2}

fi
let test=test+1
if (( $test >= $test1 && $test<= $test2 )) ; then

# try to use the new column, source should refresh and recover
sel ${col2}

fdbinfo 
tblver

fi
let test=test+1
if (( $test >= $test1 && $test<= $test2 )) ; then

# make the column dissappear
alter ${csc1}

fi
let test=test+1
if (( $test >= $test1 && $test<= $test2 )) ; then

# error now
cdb2sql --host $mach ${a_dbname} "select ${col2} from LOCAL_${a_remdbname}.${tbl} order by ${col1}" >> $output 2>&1

fdbinfo 
tblver

fi
let test=test+1
if (( $test >= $test1 && $test<= $test2 )) ; then

# put back the column
alter ${csc2}

fi
let test=test+1
if (( $test >= $test1 && $test<= $test2 )) ; then

# working again
sel ${col2}

fdbinfo 
tblver

fi
let test=test+1
if (( $test >= $test1 && $test<= $test2 )) ; then

# remove the column again to try recover using a good column
alter ${csc1}

fi
let test=test+1
if (( $test >= $test1 && $test<= $test2 )) ; then

# should work
sel ${col1}

fdbinfo
tblver

fi
let test=test+1
if (( $test >= $test1 && $test<= $test2 )) ; then

# validate results 
testcase_output=$(cat $output)
expected_output=$(cat output.log)
if [[ "$testcase_output" != "$expected_output" ]]; then

   # print message 
   echo "  ^^^^^^^^^^^^"
   echo "The above testcase (${testcase}) has failed!!!" 
   echo " "
   echo "Use 'diff <expected-output> <my-output>' to see why:"
   echo "> diff ${PWD}/{output.log,$output}"
   echo " "
   diff output.log $output
   echo " "

   # quit
   exit 1
fi

echo "Testcase passed."

fi
