#!/usr/bin/env bash

# This script is called by the test suite's Makefile to create generated tests.
# This is what it does:
#
# For each <fileprefix> with the .testopts extension in a test directory, 
# it generates a new test in the directory passed in $2 with
#
# - the options in the <fileprefix>.testopts file appended to lrl.options
# - the options in the <fileprefix>.testopts${rank} file appended to lrl_${rank}.options
#
# If <fileprefix>.testopts${rank} doesn't exist for some rank, 
# then lrl_${rank}.options is untouched.

DEST=$PWD
DB_RANKS=(1 2 3)

# generate a single test by running $0 basic.test/snapshot.testopts
generate_test()
{
    local gtst=$1
    if [ "x$gtst" == "x" ] ; then
        return
    fi

    TST=`dirname $gtst | sed 's/.test$//g'`
    OPTFL=`basename $gtst | sed 's/.testopts$//g'`
    NDIR=${TST}_${OPTFL}_generated.test
    mkdir -p $DEST/$NDIR/
    cp ${TST}.test/* $DEST/$NDIR/

    cat $gtst >> $DEST/$NDIR/lrl.options

    for rank in "${DB_RANKS[@]}"; do
        test -f "${gtst}${rank}" && \
        cat "${gtst}${rank}" >> $DEST/$NDIR/lrl_${rank}.options
    done
}


if [ "x$1" != "x" ] ; then
    one_test=$1
    shift
fi

if [ "x$1" != "x" ] ; then
    DEST=$1
fi

if [ "x$one_test" != "x" ] ; then
    generate_test $one_test
    exit
fi

for i in `ls *.test/*.testopts | grep -v '_generated.test'`; do 
    generate_test $i
done
