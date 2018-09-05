#!/usr/bin/env bash
# will be called by tests makefile to create all the generated tests:
# for every .testopts file in a test directory, will generate a new test
# with those features (in the .testopts file) appended to the lrl.options
# of the test in question.

for i in `ls *.test/*.testopts | grep -v '_generated.test'`; do 
    TST=`dirname $i | sed 's/.test$//g'`
    OPTFL=`basename $i | sed 's/.testopts$//g'`
    NDIR=${TST}_${OPTFL}_generated.test
    mkdir -p $NDIR/
    cp ${TST}.test/* $NDIR/
    cat $i >> $NDIR/lrl.options
done
