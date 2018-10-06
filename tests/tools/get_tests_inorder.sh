#!/usr/bin/env bash

#get list of tests in order from longest to shortest test to run
DEFAULT_TIMEOUT=5

tests=`ls -d *.test | grep -v '_generated.test'`
for i in $tests ; do 
    a=`cat $i/Makefile | grep TEST_TIMEOUT | egrep -v "ifeq" | cut -f2 -d'=' | sed 's/m//'` ; 
    [ -z "$a" ] && a=$DEFAULT_TIMEOUT; 
    echo $a $i; 
    for j in `ls $i/*.testopts 2> /dev/null` ; do 
        gen=`echo $j | sed 's#\.testopts#_generated#g; s#/#_#g; s#\.test##g; s#$#.test#'`
        echo $a $gen;
    done
done | sort -nr | cut -d' ' -f2
