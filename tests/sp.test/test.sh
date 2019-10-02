#!/usr/bin/env bash
bash -n "$0" | exit 1

################################################################################

# debug=1
TMPDIR=${TMPDIR:-/tmp}

# args
a_dbn=$1

# find input files
files=$( ls *.req | sort )

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
    # print debug trace
    [[ "$debug" == "1" ]] && set -x

    # args
    typeset db=$1
    typeset iter=$2
    typeset tbl

    # flagged?
    if [[ ! -f $iter.fastinit ]]; then
        return 0
    fi

    # fastinit
    for tbl in $(cat $iter.fastinit) ; do
        echo "cdb2sql ${CDB2_OPTIONS} $db default \"truncate $tbl\""
        cdb2sql ${CDB2_OPTIONS} $db default "truncate $tbl"
    done

    return 0
}

# archcode function
function myarch
{
    # print debug trace
    [[ "$debug" == "1" ]] && set -x

    u=$(uname)
    a="<unknown>"
    [[ "$u" == "SunOS" ]]   && a="sundev1"
    [[ "$u" == "AIX" ]]     && a="ibm"
    [[ "$u" == "HP-UX" ]]   && a="hp"
    [[ "$u" == "Linux" ]]   && a="linux"

    echo $a
    return 0
}

function fix_genid {
    sed 's/genid =.* rc/genid =dum rc/' < $1
}

# Iterate through input files
for testcase in $files ; do

    # increment counter
    let nfiles=nfiles+1

    # cleanup testcase
    testcase=${testcase##*/}
    
    # see if the prefix has changed
    new_batch=${testcase%%_*}

    # set output
    output=$testcase.res

    # full path 
    [[ "$output" == "${output#\/}" ]] && output=${PWD}/$output
    
    # fastinit if requested
    if [[ $new_batch != $last_batch ]] ; then

        fastinit $a_dbn $new_batch
        last_batch=$new_batch

    fi

    # Check for run-stepper
    if [[ -f $new_batch.tool ]] ; then

        tool=$( cat $new_batch.tool )
        args=$( cat $new_batch.args )
        echo "> $tool $a_dbn $args $testcase $output"
        $tool $a_dbn $args $testcase $output

    else

        # Be verbose
        cmd="cdb2sql -s ${CDB2_OPTIONS} $a_dbn default - < $testcase > $output 2>&1"
        echo $cmd

        # run command
        eval $cmd

    fi

    # post-process
    if [[ -f $new_batch.post ]]; then

        # zap file
        > $output.postprocess

        # collect post-processing tool
        pproc=$(cat $new_batch.post)

        # post-process output
        $pproc $output >> $output.postprocess

        # copy post-processed output to original
        mv $output.postprocess $output
    fi

    # get testcase output
    testcase_output=$(cat $output)

    # get expected output
    expected_output=$(cat $testcase.out)

    if [[ -f $testcase.out.1 ]]; then
        expected_output_alt=$(cat $testcase.out.1)
    else
        expected_output_alt=""
    fi

    # verify 
    if [[ "$testcase_output" != "$expected_output" && "$testcase_output" != "$expected_output_alt" ]]; then

        echo "  ^^^^^^^^^^^^"
        echo "The above testcase (${testcase}) has failed!!!"
        echo "diff ${PWD}/$testcase.out $output"
        echo " "
        diff $testcase.out $output
        echo " "
        exit 1

    fi

done

egrep -i "ctrl engine has wrong state" $TESTDIR/logs/*db
if [[ $? == 0 ]]; then
    echo "error: corrupted transaction state detected"
    exit 1
fi

echo "Testcase passed."

exit 0
