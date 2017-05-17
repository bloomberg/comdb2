#!/bin/bash

# Remote cursor moves testcase for comdb2
################################################################################


# args
# <dbname> <autodbname> <dbdir> <testdir>
a_remdbname=$1
a_remcdb2config=$2
a_dbname=$3
a_dbdir=$4
a_testdir=$5

output=run.out


#TEST1 test conflicts between V1 and V2 (see README)

# populate table on remote
cdb2sql -s --cdb2cfg ${a_remcdb2config} $a_remdbname default - < remdata.req > $output 2>&1

# get the version V1 from remote
cdb2sql --cdb2cfg ${a_remcdb2config} $a_remdbname default 'select table_version("t")' >> $output

a_cdb2config=${CDB2_OPTIONS}
# retrieve data through remote sql
cdb2sql $a_cdb2config $a_dbname default "select * from LOCAL_${a_remdbname}.t order by id" >> $output 2>&1

# get the version V2
#comdb2sc $a_dbname send fdb info db >> $output 2>&1
echo cdb2sql --tabs $a_cdb2config $a_dbname default "exec procedure sys.cmd.send(\"fdb info db\")" 
cdb2sql --tabs $a_cdb2config $a_dbname default "exec procedure sys.cmd.send(\"fdb info db\")" >> $output 2>&1

# schema change the remote V1->V1'
cdb2sql --cdb2cfg ${a_remcdb2config} $a_remdbname default "alter table t { `cat t.csc2 ` }"

# sleep since we don't wait for schema change right now!
sleep 5

# get the new version V1'
cdb2sql --cdb2cfg ${a_remcdb2config} $a_remdbname default 'select table_version("t")' >> $output

# retrieve data
cdb2sql ${CDB2_OPTIONS} $a_dbname default "select * from LOCAL_${a_remdbname}.t order by id" >> $output 2>&1

# get the new version V2'
#comdb2sc $a_dbname send fdb info db >> $output 2>&1
echo cdb2sql --tabs $a_cdb2config $a_dbname default "exec procedure sys.cmd.send(\"fdb info db\")" 
cdb2sql --tabs $a_cdb2config $a_dbname default "exec procedure sys.cmd.send(\"fdb info db\")" >> $output 2>&1

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

#TEST2 test conflicts between V3 and V2 (see README)
#gonna simulate this by dropping the table manually


# get the version V1 

# get the version V2

# schema change the remote V1->V1'

# get the new version V1'

# get the version V2

# drop the version V2 cache

# retrieve the data

# get the new version V2'

# validate results 


echo "Testcase passed."
