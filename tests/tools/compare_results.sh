#!/usr/bin/env bash

# Execute sql scripts and compare the results against expected output

bash -n "$0" | exit 1

script_mode=""
dbname=
sql_extn="sql"
exp_extn="expected"

usage() {
    echo "Usage $0 [-s] [-d dbname] [-r sql] [-e expected]"
    echo "Options:"
    echo "  -d db name"
    echo "  -s run cdb2sql in script mode"
    echo "  -r extension for input file(s) (default: $sql_extn)"
    echo "  -e extension for expected output file(s) (default: $exp_extn)"
    exit 1
}

prepare_abort() {
    prep_log=$1
    testname=$2
    echo "failed to prepare table(s) for $testname test"
    echo "prepare logs:"
    cat $prep_log
    echo "exiting .."
    exit 1
}

while getopts "sd:r:e:" options; do
    case "${options}" in
        d) dbname=${OPTARG} ;;
        s) script_mode="-s" ;;
        r) sql_extn=${OPTARG} ;;
        e) exp_extn=${OPTARG} ;;
        *) usage ;;
    esac
done

if [[ -z $dbname ]] ; then
    echo dbname missing
    exit 1
fi

sqlfiles=`ls *.$sql_extn`
[ $? -eq 0 ] || exit 1

for sqlfile in $sqlfiles; do
    echo "$sqlfile"
    testname=`echo $sqlfile | cut -d "." -f 1`
    prep_log=$testname.prepare.err

    touch $prep_log

    for schema in `ls $testname.*.csc2 2> /dev/null` ; do
        table=`echo $schema | cut -d "." -f2`
        cdb2sql ${CDB2_OPTIONS} $dbname default "drop table if exists $table" > $prep_log 2>&1
        [ $? -eq 0 ] || prepare_abort $prep_log $testname

        cdb2sql ${CDB2_OPTIONS} $dbname default "create table $table { `cat $schema` }" > $prep_log 2>&1
        [ $? -eq 0 ] || prepare_abort $prep_log $testname
    done

    rm $prep_log

    cmd="cdb2sql ${CDB2_OPTIONS} $script_mode -f $sqlfile $dbname default "
    echo $cmd "> $testname.output"
    $cmd 2>&1 | perl -pe "s/.n_writeops_done=([0-9]+)/rows inserted='\1'/;
                          s/BLOCK2_SEQV2\(824\)/BLOCK_SEQ(800)/;
                          s/OP #2 BLOCK_SEQ/OP #3 BLOCK_SEQ/;
                          s/rrn ([0-9]+) genid 0x([a-zA-Z0-9]+)/rrn xx genid xx/;"\
                          > $testname.output

    cmd="diff $testname.$exp_extn $testname.output"
    $cmd > /dev/null

    if [[  $? -eq 0 ]]; then
        echo "passed $testname"
    else
        echo "failed $testname"
        echo "see diffs here: $HOSTNAME"
        echo "> diff -u ${PWD}/{$testname.$exp_extn,$testname.output}"
        echo "first 10 lines of the diff:"
        $cmd | head -10
        echo
        exit 1
    fi
done
echo
exit 0
