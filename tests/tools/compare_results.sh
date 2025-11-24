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

# Source shared systables injection functions
source ${TESTSROOTDIR}/test-utils/inject_systables.sh

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
    testname=${sqlfile%.*}
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

    if [ -f ${testname}.fastinit ] ; then
        for t in `cat ${testname}.fastinit`; do
            cmd="cdb2sql ${CDB2_OPTIONS} $dbname default 'truncate $t'"
            echo $cmd "> $testname.output"
            eval $cmd 2>&1 >> $testname.fastinit_out
        done
    fi

    cmd="cdb2sql ${CDB2_OPTIONS} $script_mode -f $sqlfile $dbname default "
    echo $cmd "> $testname.output"
    eval $cmd 2>&1 | sed 's/rrn 2 genid 0x[[:alnum:]]\+/rrn xx genid xx/' > $testname.output
    
    inject_systables_in_expected_files "$testname.$exp_extn" 
    
    diff "$testname.$exp_extn"  $testname.output > /dev/null
    if [[  $? -eq 0 ]]; then
        echo "passed $testname"
    else
        echo "failed $testname"
        echo "see diffs here: $HOSTNAME"
        echo "> diff -u ${PWD}/{$testname.$exp_extn,$testname.output}"
        echo "first 10 lines of the diff:"
        diff $testname.$exp_extn $testname.output | head -10
        echo
        exit 1
    fi

    restore_expected_files
done
echo
exit 0
