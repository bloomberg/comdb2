#!/usr/bin/env bash

set -x
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

REM_CDB2_OPTIONS="--cdb2cfg ${a_remcdb2config}"
SRC_CDB2_OPTIONS="--cdb2cfg ${a_cdb2config}"

output=run.out


#TEST1 test conflicts between V1 and V2 (see README)

# populate table on remote
cdb2sql -s $REM_CDB2_OPTIONS $a_remdbname default - < remdata.req > $output 2>&1

# get the version V1 from remote
cdb2sql $REM_CDB2_OPTIONS $a_remdbname default 'select table_version("t")' >> $output

# Make sure we talk to the same host
mach=`cdb2sql --tabs ${SRC_CDB2_OPTIONS} $a_dbname default "SELECT comdb2_host()"`

# retrieve data through remote sql
cdb2sql ${SRC_CDB2_OPTIONS} --host $mach $a_dbname "select * from LOCAL_${a_remdbname}.t order by id" >> $output 2>&1

# get the version V2
#comdb2sc $a_dbname send fdb info db >> $output 2>&1
echo cdb2sql ${SRC_CDB2_OPTIONS} --tabs --host $mach $a_dbname "exec procedure sys.cmd.send(\"fdb info db\")"
cdb2sql ${SRC_CDB2_OPTIONS} --tabs --host $mach $a_dbname "exec procedure sys.cmd.send(\"fdb info db\")"  2>&1 | cut -f 5- -d ' ' >> $output

# schema change the remote V1->V1'
cdb2sql $REM_CDB2_OPTIONS $a_remdbname default "alter table t { `cat t.csc2 ` }"

# sleep since we don't wait for schema change right now!
sleep 5

# get the new version V1'
cdb2sql $REM_CDB2_OPTIONS $a_remdbname default 'select table_version("t")' >> $output

# retrieve data
cdb2sql ${SRC_CDB2_OPTIONS} -cost --host $mach $a_dbname default "select * from LOCAL_${a_remdbname}.t order by id" >> $output 2>&1

# get the new version V2'
#comdb2sc $a_dbname send fdb info db >> $output 2>&1
echo cdb2sql ${SRC_CDB2_OPTIONS} --tabs --host $mach $a_dbname "exec procedure sys.cmd.send(\"fdb info db\")"
cdb2sql ${SRC_CDB2_OPTIONS} --tabs --host $mach $a_dbname "exec procedure sys.cmd.send(\"fdb info db\")"  2>&1 | cut -f 5- -d ' ' >> $output


# validate results 
if [[ $a_dbname == "srcdbscremsqlfdbpushgenerated"* ]]; then
    active_output=output.log.fdbpush
else
    active_output=output.log
fi

sed "s/DBNAME/${a_remdbname}/g" $active_output > output.log.actual
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
