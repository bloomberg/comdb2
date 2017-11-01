#!/usr/bin/env bash
# use bash file locking ability to atomically get and increment
# test counter used by Makefile to print counter of current running test

LOCKFL=${TESTDIR}/counter_lock_file.txt
CNTRFL=${TESTDIR}/test_counter.txt

set -o noclobber
{ > ${LOCKFL}; } &> /dev/null
rc=$?
while [ $rc -ne 0 ] ; do
    #echo 'did not get lock, trying again'
    sleep 0.01
    { > ${LOCKFL} ; } &> /dev/null
    rc=$?
done

counter=`cat $CNTRFL 2> /dev/null`
newcntr=$((counter+1))
set +o noclobber

echo $newcntr > ${CNTRFL}
rm $LOCKFL

echo $newcntr
