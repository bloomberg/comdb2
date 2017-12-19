#!/usr/bin/env bash

# Remote cursor moves testcase for comdb2
################################################################################


# args
# <dbname> <autodbname> <dbdir> <testdir>
db1=$1
cdb2cfg1=$2
db2=$3
cdb2cfg1=$4

a_remcdb2config=$2
a_dbname=$3
a_dbdir=$4
a_testdir=$5

output=run.out


#TEST1 test conflicts between V1 and V2 (see README)

# populate table on remote
cdb2sql -s --cdb2cfg $cdb2cfg1 $db1 default - < remdata.req > $output 2>&1

cdb2sql -s --cdb2cfg $cdb2cfg2 $db2 default "select * from LOCAL_${db1}.t order by id" >> $output 2>&1

# get the version V2
#comdb2sc $a_dbname send fdb info db >> $output 2>&1
echo cdb2sql --tabs --cdb2cfg $cdb2cfg2 $db2 default "exec procedure sys.cmd.send(\"fdb info db\")" 
cdb2sql --tabs --cdb2cfg $cdb2cfg2 $db2 default "exec procedure sys.cmd.send(\"fdb info db\")" >> $output 2>&1

# validate results 
testcase_output=$(cat $output)
expected_output=$(cat output.log.actual)
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
