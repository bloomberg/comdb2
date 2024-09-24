#!/usr/bin/env bash

# Remote cursor moves testcase for comdb2
################################################################################

a_remdbname=$1
a_remcdb2config=$2
a_dbname=$3
a_cdb2config=$4
a_dbdir=$5
a_testdir=$6

if [[ ! -z $7 ]]; then
   opt=$7
fi

set -x

REM_CDB2_OPTIONS="--cdb2cfg ${a_remcdb2config}"
SRC_CDB2_OPTIONS="--cdb2cfg ${a_cdb2config}"

checkresult()
{
   correct_output_file=$1
   run_output_file=$2

   # validate results 
   testcase_output=$(cat ${run_output_file})
   expected_output=$(cat ${correct_output_file})
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
      echo "> diff ${PWD}/${correct_output_file} $tc_out"
      echo " "
            diff ${PWD}/${correct_output_file} $tc_out
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

   #rm $output

   work_input=${input}.actual

   # fix the target
   sed "s/ t / LOCAL_${a_remdbname}.${tblname} /g" $input > $work_input

   # populate table on remote
   ${CDB2SQL_EXE} ${SRC_CDB2_OPTIONS} --host $mach $a_dbname - < $work_input | grep -v "${tblname}" >> $output


   # retrieve data through remote sql
   ${CDB2SQL_EXE} ${SRC_CDB2_OPTIONS} --host $mach $a_dbname "select * from LOCAL_${a_remdbname}.${tblname} order by ${column}" >> $output

   # get the version V2
   #comdb2sc $a_dbname send fdb info db >> $output 2>&1
   echo ${CDB2SQL_EXE} ${SRC_CDB2_OPTIONS} --tabs --host $mach $a_dbname "exec procedure sys.cmd.send(\"fdb info db\")"
   ${CDB2SQL_EXE} ${SRC_CDB2_OPTIONS} --tabs --host $mach $a_dbname "exec procedure sys.cmd.send(\"fdb info db\")" 2>&1 | cut -f 5- -d ' ' >> $output

   work_exp_output=${exp_output}.actual
   sed "s/ t / LOCAL_${a_remdbname}.${tblname} /g" ${exp_output}  > ${work_exp_output}

   # drop the 
   checkresult  $work_exp_output $output 
}

# Make sure we talk to the same host
echo "${CDB2SQL_EXE} --tabs ${SRC_CDB2_OPTIONS} $a_dbname default 'SELECT comdb2_host()'"
mach=`${CDB2SQL_EXE} --tabs ${SRC_CDB2_OPTIONS} $a_dbname default "SELECT comdb2_host()"`
if [[ -z $mach ]]; then
    echo "Failure to get a machine name"
    exit 1
fi

if [[ -z $opt || "$opt" == "1" ]]; then
   output=./run.out
   rm $output 2>/dev/null

   run_test inserts.req output.1.log $output t1 id
fi

if [[ -z $opt || "$opt" == "2" ]]; then
   output=./run.2.out
   rm $output 2>/dev/null

   run_test inserts2.req output.2.log $output t2 a 
fi

if [[ -z $opt || "$opt" == "3" ]]; then
    ${CDB2SQL_EXE} ${REM_CDB2_OPTIONS} ${a_remdbname} default "drop table tt"
fi

if [[ -z $opt || "$opt" == "4" ]]; then

   #TEST2 check updates

   output=./run.4.out
   rm $output 2>/dev/null

   run_test updates.req output.4.log $output t1 id
fi


if [[ -z $opt || "$opt" == "5" ]]; then

   #TEST3 check deletes

   output=./run.5.out
   rm $output 2> /dev/null

   run_test deletes.req output.5.log $output t1 id
fi

if [[ -z $opt || "$opt" == "6" ]]; then

   #TEST4 check negative rowids

   output=./run.6.out
   rm $output 2>/dev/null

   echo "${CDB2SQL_EXE} ${REM_CDB2_OPTIONS} --host $mach ${a_remdbname} - < inserts3.req "
   ${CDB2SQL_EXE} ${REM_CDB2_OPTIONS} --host $mach ${a_remdbname} - < inserts3.req | grep -v "t2" >> $output
   
   echo "1. Round of updates" >> $output
   run_test updates2.req output.6.log $output t2 a
   rm $output 2>/dev/null
   echo "2. Round of updates" >> $output
   run_test updates2.req output.6.2.log $output t2 a
   rm $output 2>/dev/null
   echo "3. Round of updates" >> $output
   run_test updates2.req output.6.3.log $output t2 a
   rm $output 2>/dev/null
   echo "4. Round of updates" >> $output
   run_test updates2.req output.6.4.log $output t2 a
   rm $output 2>/dev/null
   echo "5. Round of updates" >> $output
   run_test updates2.req output.6.5.log $output t2 a
   rm $output 2>/dev/null
   echo "6. Round of updates" >> $output
   run_test updates2.req output.6.6.log $output t2 a
   rm $output 2>/dev/null
   echo "7. Round of updates" >> $output
   run_test updates2.req output.6.7.log $output t2 a
   rm $output 2>/dev/null
   echo "8. Round of updates" >> $output
   run_test updates2.req output.6.8.log $output t2 a
   rm $output 2>/dev/null
   echo "9. Round of updates" >> $output
   run_test updates2.req output.6.9.log $output t2 a
fi

if [[ -z $opt || "$opt" == "7" ]]; then

   #TEST3 check deletes

   output=./run.7.out
   rm $output 2> /dev/null

   run_test rollbackupdates.req output.7.log $output t2 a
fi

if [[ -z $opt || "$opt" == "8" ]]; then

    output=./run.8.out
    run_test pausedtxn.req output.8.log $output t2 a
fi



echo "Testcase passed."

exit $result
