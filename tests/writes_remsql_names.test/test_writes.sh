#!/usr/bin/ksh

# Remote cursor moves testcase for comdb2
################################################################################

opt=

# args
# <dbname> <autodbname> <dbdir> <testdir>
a_remdbname=$1
a_dbname=$2
a_dbdir=$3
a_testdir=$4

# result
result=0

if [[ ! -z $5 ]]; then
   opt=$5
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
      tc_out=/bb/data/tmp/cdb2test.tc_output.$$.out

      # copy output to semi-permanent file
      cp $run_output_file $tc_out

      # print message 
      print "  ^^^^^^^^^^^^"
      print "The above testcase (${testcase}) @$HOSTNAME has failed!!!" 
      print " "
      print "Use 'diff <expected-output> <my-output>' to see why:"
      print "> diff ${correct_output_file}.actual $tc_out"
      print " "
            diff -u ${correct_output_file}.actual $tc_out
      print " "

      result=1
   fi
}

run_test()
{
   set -x 
   input=$1
   exp_output=$2
   output=$3
   table=$4
   col=$5

   work_input=${input}.actual

   # fix the target
   sed "s/ ${table} / LOCAL_${a_remdbname}.${table} /g" $input > $work_input

   # populate table on remote
   comdb2sql $a_dbname - < $work_input >> $output 2>&1

   # retrieve data through remote sql
   comdb2sql $a_dbname "select * from LOCAL_${a_remdbname}.${table} order by ${col}" >> $output 2>&1

   # get the version V2
   comdb2sc.tsk $a_dbname send fdb info db >> $output 2>&1

   # drop the 
   checkresult  $exp_output $output 
}


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
    comdb2sql ${a_remdbname} "drop table tt"
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

print "Testcase passed."

return $result

