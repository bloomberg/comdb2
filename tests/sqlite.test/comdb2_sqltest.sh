#!/usr/bin/env bash

# sqlite test-script for comdb2 databases

# arguments

args=$1
dbnm=$2
wrkd=$3
inp=$4
rmt=$5
log=$6


# sql-tester
exe=${TESTSBUILDDIR}/comdb2_sqltest

# direct output
out=${TMPDIR}/cdb2_sqltest.$$.log

# run the test
$exe -verify -engine Comdb2 -db $dbnm $inp > $out 2>&1

# verify and print results
egrep " 0 errors out of" $out >/dev/null 2>&1

if [[ $? == 0 ]]; then

    echo "SUCCESS"

else

    echo "FAILED TEST"
    cat $out

fi

