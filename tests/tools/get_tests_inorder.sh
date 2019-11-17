#!/usr/bin/env bash

#get list of tests in order from longest to shortest test to run
DEFAULT_TIMEOUT=5

#get the tests with custom times
custom_times=`grep "TEST_TIMEOUT=" *.test/Makefile | sed 's#export TEST_TIMEOUT=##; s#/Makefile:# #; s#m$##'`

#get the tests with default times
default_times=`grep -c "TEST_TIMEOUT=" *.test/Makefile  | grep ":0" | sed "s#/Makefile:0# $DEFAULT_TIMEOUT#"`
IFS=$'\n'; 

#get the generated tests
generated=$(for j in */*.testopts ; do 
    basedir=${j%%/*}
    time=`echo -e "${custom_times} \n${default_times}" | grep $basedir | awk '{print $2}'`
    echo $j $time
done | sed 's#.test/#_#g; s#\.testopts#_generated.test#g;' )

echo -e "${custom_times} \n${default_times} \n${generated}" | sort -k2 -t' ' -nr | cut -f1 -d' '
