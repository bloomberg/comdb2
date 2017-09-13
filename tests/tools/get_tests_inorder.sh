#!/bin/bash

#get list of tests in order from longest to shortest test to run
DEFAULT_TIMEOUT=5

for i in `ls -d *.test` ; do
    if ! grep -s "^$i" disabled.tests > /dev/null; then
        a=`cat $i/Makefile | grep TEST_TIMEOUT | cut -f2 -d'=' | sed 's/m//'` ;
        [ -z $a ] && a=$DEFAULT_TIMEOUT;
        echo $a $i;
    fi
done | sort -nr | cut -d' ' -f2

