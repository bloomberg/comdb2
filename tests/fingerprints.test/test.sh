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

# figure out which host should contain the fingerprints
export SP_HOST=$(cdb2sql --tabs -s ${CDB2_OPTIONS} $a_dbn default "SELECT comdb2_host()")

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
    [[ "$output" == "${output#\/}" ]] && output=$(pwd)/$output

    # Check for run-stepper
    if [[ -f $new_batch.tool ]] ; then

        tool=$( cat $new_batch.tool )
        args=$( cat $new_batch.args )
        echo "> $tool $a_dbn $args $testcase > $output"
        $tool $a_dbn $args $testcase > $output

    else

        # Be verbose
        cmd="cdb2sql --host $SP_HOST -s ${CDB2_OPTIONS} $a_dbn default - < $testcase > $output 2>&1"
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

    # verify
    if [[ "$testcase_output" != "$expected_output" ]]; then
        echo "  ^^^^^^^^^^^^"
        echo "The above testcase (${testcase}) has failed!!!"
        echo "diff ${PWD}/$testcase.out $output"
        echo " "
        diff $testcase.out $output
        echo " "
        exit 1
    fi
done

echo "Testcase passed."

exit 0
