#!/usr/bin/env bash

# Remote cursor moves testcase for comdb2
################################################################################

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
   tblname=$4
   column=$5

   rm $output

   work_input=${input}.actual

   # fix the target
   sed "s/ t / LOCAL_${a_remdbname}.${tblname} /g" $input > $work_input

   # populate table on remote
   ${CDB2SQL_EXE} --host $mach $a_dbname - < $work_input | grep -v "${tblname}" >> $output


   # retrieve data through remote sql
   ${CDB2SQL_EXE} --host $mach $a_dbname "select * from LOCAL_${a_remdbname}.${tblname} order by ${column}" >> $output

   a_cdb2config=${CDB2_OPTIONS}
   # get the version V2
   #comdb2sc $a_dbname send fdb info db >> $output 2>&1
   echo ${CDB2SQL_EXE} --tabs --host $mach $a_dbname "exec procedure sys.cmd.send(\"fdb info db\")"
   ${CDB2SQL_EXE} --tabs --host $mach $a_dbname "exec procedure sys.cmd.send(\"fdb info db\")" >> $output 2>&1

   work_exp_output=${exp_output}.actual
   sed "s/ t / LOCAL_${a_remdbname}.${tblname} /g" ${exp_output}  > ${work_exp_output}

   # drop the 
   checkresult  $work_exp_output $output 
}

# Make sure we talk to the same host
mach=`${CDB2SQL_EXE} --tabs ${CDB2_OPTIONS} $a_dbname default "SELECT comdb2_host()"`

if [[ -z $opt || "$opt" == "1" ]]; then
   output=./run.out
   rm $output
   run_test inserts.req output.1.log $output t1 id
fi

if [[ -z $opt || "$opt" == "2" ]]; then
   output=./run.2.out
   rm $output
   run_test inserts2.req output.2.log $output t2 a 
fi

if [[ -z $opt || "$opt" == "3" ]]; then
    ${CDB2SQL_EXE} ${CDB2_OPTIONS} ${a_remdbname} default "drop table tt"
fi

if [[ -z $opt || "$opt" == "4" ]]; then

   #TEST2 check updates

   output=./run.4.out

   rm $output
   run_test updates.req output.4.log $output t1 id
fi


if [[ -z $opt || "$opt" == "5" ]]; then

   #TEST3 check deletes

   output=./run.5.out

   rm $output
   run_test deletes.req output.5.log $output t1 id
fi

echo "Testcase passed."

exit $result
