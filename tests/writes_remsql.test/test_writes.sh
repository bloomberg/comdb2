#!/usr/bin/env bash

# Remote cursor moves testcase for comdb2
################################################################################

opt=

# args
# <dbname> <autodbname> <dbdir> <testdir>
a_remdbname=$1
a_remcdb2config=$2
a_dbname=$3
a_dbdir=$4
a_testdir=$5

if [[ ! -z $6 ]]; then
   opt=$6
fi

checkresult()
{
   correct_output_file=$1
   run_output_file=$2

   #convert the table to actual dbname
   sed "s/dorintdb/${a_remdbname}/g" ${correct_output_file} > ${correct_output_file}.actual

   # validate results 
   testcase_output=$(cat ${run_output_file})
   expected_output=$(cat ${correct_output_file}.actual)
   if [[ "$testcase_output" != "$expected_output" ]]; then

      # generate filename
      tc_out=$TMPDIR/cdb2test.tc_output.$$.out

      # copy output to semi-permanent file
      cp $run_output_file $tc_out

      # print message 
      echo "  ^^^^^^^^^^^^"
      echo "The above testcase (${testcase}) has failed!!!" 
      echo " "
      echo "Use 'diff <expected-output> <my-output>' to see why:"
      echo "> diff ${correct_output_file}.actual $tc_out"
      echo " "
            diff ${correct_output_file}.actual $tc_out
      echo " "

      exit 1
   fi
}

run_test()
{
   set -x 
   input=$1
   exp_output=$2
   output=$3

   rm $output

   work_input=${input}.actual

   # fix the target
   sed "s/ t / LOCAL_${a_remdbname}.t /g" $input > $work_input

   # populate table on remote
   cdb2sql --host $mach $a_dbname - < $work_input >> $output 2>&1
   

   # retrieve data through remote sql
   cdb2sql --host $mach $a_dbname "select * from LOCAL_${a_remdbname}.t order by id" >> $output 2>&1

   a_cdb2config=${CDB2_OPTIONS}
   # get the version V2
   #comdb2sc $a_dbname send fdb info db >> $output 2>&1
   echo cdb2sql --tabs --host $mach $a_dbname "exec procedure sys.cmd.send(\"fdb info db\")"
   cdb2sql --tabs --host $mach $a_dbname "exec procedure sys.cmd.send(\"fdb info db\")" >> $output 2>&1

   work_exp_output=${exp_output}.actual
   sed "s/ t / LOCAL_${a_remdbname}.t /g" ${exp_output}  > ${work_exp_output}
   # drop the 
   checkresult  $work_exp_output $output 
}

# Make sure we talk to the same host
mach=`cdb2sql --tabs ${CDB2_OPTIONS} $a_dbname default "SELECT comdb2_host()"`

if [[ -z $opt || "$opt" == "1" ]]; then

   #TEST1 check inserts 

   output=./run.out

   run_test inserts.req output.1.log $output
fi



if [[ -z $opt || "$opt" == "2" ]]; then

   #TEST2 check updates

   output=./run.2.out

   run_test updates.req output.2.log $output
fi


if [[ -z $opt || "$opt" == "3" ]]; then

   #TEST3 check deletes

   output=./run.3.out

   run_test deletes.req output.3.log $output
fi


if [[ -z $opt || "$opt" == "4" ]]; then

   #TEST2 check begin/commit/rollback updates

   output=./run.4.out

   run_test batch_remsql.req output.4.log $output
fi

if [[ -z $opt || "$opt" == "5" ]]; then

   #TEST2 check local writes with remote reads

   output=./run.5.out

   run_test write_with_selects.req output.5.log $output
fi

if [[ -z $opt || "$opt" == "6" ]]; then

   #TEST2 check remote writes with local reads

   #last test needs some prep 
   cdb2sql --host $mach $a_dbname "truncate t2"
   if (( $? != 0 )) ; then
      echo "Failure to truncate remote db "
      exit 1
   fi
   cdb2sql --host $mach $a_dbname "insert into t2 select * from LOCAL_${a_remdbname}.t"
   if (( $? != 0 )) ; then
      echo "Failure to populate remote db "
      exit 1
   fi
   cdb2sql --cdb2cfg ${a_remcdb2config} $a_remdbname default "truncate t"
   if (( $? != 0 )) ; then
      echo "Failure to truncate local db "
      exit 1
   fi

   output=./run.6.out

   run_test select_with_writes.req output.6.log $output
fi


echo "Testcase passed."
