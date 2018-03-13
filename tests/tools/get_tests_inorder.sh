#!/usr/bin/env bash

#get list of tests in order from longest to shortest test to run
DEFAULT_TIMEOUT=5

for i in ` ls -d *.test` ; do 
    a=`cat $i/Makefile | grep TEST_TIMEOUT | egrep -v "ifeq" | cut -f2 -d'=' | sed 's/m//'` ; 
    [ -z "$a" ] && a=$DEFAULT_TIMEOUT; 
    echo $a $i; 
done | sort -nr | cut -d' ' -f2
