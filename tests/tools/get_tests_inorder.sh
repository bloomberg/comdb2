#!/usr/bin/env bash

#get list of tests in order from longest to shortest test to run
DEFAULT_TIMEOUT=5

#get the tests with custom times
custom_times=`grep "TEST_TIMEOUT=" *.test/Makefile | sed 's#export TEST_TIMEOUT=##; s#/Makefile:# #; s#m$##'`

#get the tests with default times
default_times=`grep -c "TEST_TIMEOUT=" *.test/Makefile  | grep ":0" | sed "s#/Makefile:0# $DEFAULT_TIMEOUT#"`
IFS=$'\n'; 

#get the generated tests
generated=$(for j in `ls */*.testopts` ; do 
    gen=`echo $j | sed 's#\.testopts#_generated#g; s#/#_#g; s#\.test##g; s#$#.test#'`
    name=`dirname $j`
    time=`echo -e "${custom_times[*]} \n${default_times[*]}" | grep $name | awk '{print $2}'`
    echo $gen $time
done )

echo -e "${custom_times[*]} \n${default_times[*]} \n${generated[*]}" | awk '{print $2,$1}'| sort -nr | awk '{print $2}'
