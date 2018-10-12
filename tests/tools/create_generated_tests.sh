#!/usr/bin/env bash

# will be called by tests makefile to create all the generated tests:
# for every .testopts file in a test directory, will generate a new test
# with those features (in the .testopts file) appended to the lrl.options
# of the test in question into destination directory passed in $2.

DEST=$PWD


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
