#!/usr/bin/env bash

# Remote cursor moves testcase for comdb2
################################################################################


# args
# <dbname> <autodbname> <dbdir> <testdir>
a_remdbname=$1
a_remcdb2config=$2
#srcdb:
a_dbname=$3
a_cdb2config=$4


# find input files
files=$( find . -type f -name \*.req | sort )

# counter 
nfiles=0

# last batch
last_batch=

# post-process
pproc=cat

# testcase output
testcase_output=

# expected output
expected_output=


# fastinit
function fastinit
{
    # echo debug trace
    [[ "$debug" == "1" ]] && set -x

    # args
    typeset db=$1
    typeset iter=$2
    typeset config=$3
    typeset tbl

    # flagged?
    if [[ ! -f $iter.fastinit ]]; then
        return 0
    fi

    # fastinit
    for tbl in $(cat $iter.fastinit) ; do
        echo "cdb2sql $config $db default \"truncate $tbl\""
        cdb2sql $config $db default "truncate $tbl"
    done

    return 0
}

# iterate through input files
for testcase in $files ; do

    # increment counter
    let nfiles=nfiles+1

    # cleanup testcase
    testcase=${testcase##*/}
    
    # see if the prefix has changed
    new_batch=${testcase%%_*}

    # set output
    output=$testcase.res
    
    # fastinit if requested
    if [[ $new_batch != $last_batch ]] ; then

        fastinit $a_remdbname $new_batch "--cdb2cfg $a_remcdb2config"
        last_batch=$new_batch

    fi

    is_insert=`grep insert $testcase`

    if [[ -z $is_insert ]] ; then

      # run command
      cmd="cdb2sql -s --cdb2cfg ${a_cdb2config} $a_dbname default - < $testcase > $output 2>&1"
      echo $cmd
      eval $cmd

    else

      # run command
      cmd="cdb2sql -s --cdb2cfg ${a_remcdb2config} $a_remdbname default - < $testcase > $output 2>&1"
      echo $cmd
      eval $cmd

    fi


    # post-process
    if [[ -f $new_batch.post ]]; then

        # zap file
        > $output.postprocess

        # collect post-processing tool
        pproc=$(cat $new_batch.post)

        # post-process output
        eval $pproc $output >> $output.postprocess

        # copy post-processed output to original
        mv $output.postprocess $output
    fi

    # get testcase output
    testcase_output=$(cat $output)

    # get expected output
    expected_output=$(cat $testcase.out)

    # verify 
    if [[ "$testcase_output" != "$expected_output" ]]; then

        echo "  ^^^^^^^^^^^^"
        echo "The above testcase (${testcase}) has failed!!!"
        echo " "
        echo "Use 'diff <expected-output> <my-output>' to see why:"
        echo "> diff ${PWD}/{$testcase.out,$output}"
        echo " "
        diff $testcase.out $output
        echo " "
        exit 1

    fi

done

echo "Testcase passed."
