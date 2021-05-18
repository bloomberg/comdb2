#!/usr/bin/env bash
# Get list of tests in order from longest to shortest duration

SKIPLONGRUNNING=0
SHOWTIMEOUT=
LONGESTFIRST=1

while true ; do
  case "$1" in
  --skip-long-running) SKIPLONGRUNNING=1; shift;;
  --show-timeout) SHOWTIMEOUT=1; shift;;
  --shortest-first) LONGESTFIRST=0; shift;;
  * ) break;;
  esac
done

DEFAULT_TIMEOUT=5  # default timeout for the tests is 5 seconds

#get the tests with custom times
custom_times=`grep "TEST_TIMEOUT=" *.test/Makefile | sed 's#export TEST_TIMEOUT=##; s#/Makefile:# #; s#m$##'`

#get the tests with default times
default_times=`grep -c "TEST_TIMEOUT=" *.test/Makefile  | grep ":0" | sed "s#/Makefile:0# $DEFAULT_TIMEOUT#"`
IFS=$'\n'; 

#get the generated tests
generated=$(for j in */*.testopts ; do 
    basedir=${j%%/*}
    time=`echo -e "${custom_times} \n${default_times}" | grep "^$basedir" | awk '{print $2}'`
    echo $j $time
done | sed 's#.test/#_#g; s#\.testopts#_generated.test#g;' )

F=${SHOWTIMEOUT:+,\$2}
if [ "$LONGESTFIRST" -eq 1 ] ; then
    SORTORDER="r"
fi

echo -e "${custom_times} \n${default_times} \n${generated}" | sort -k2 -t' ' -n${SORTORDER} | awk '{ if(!'$SKIPLONGRUNNING' || $2 < 120) { print $1'$F' } }'
