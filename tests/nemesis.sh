#!/bin/bash

t=$1
tst=${2:-jepsenregisternemesis}
d=$tst$t 
c=/home/mhannum/comdb2/tests/test_$t/$d/comdb2db.cfg 
linearizable/ctest/breakloop -d $d -c $c -G partition 2>&1 | gawk '{ print strftime("%H:%M:%S>"), $0; fflush(); }'

