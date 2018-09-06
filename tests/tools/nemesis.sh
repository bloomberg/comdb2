#!/usr/bin/env bash

set -x
t=$1
tst=${2:-jepsenregisternemesis}
d=$tst$t 
whoami=$(whoami)
c=/home/$whoami/comdb2/tests/test_$t/$d/comdb2db.cfg 
linearizable/ctest/breakloop -d $d -c $c -C -G partition -m $d.node1 -m $d.node2 -m $d.node3 -m $d.node4 -m $d.node5 2>&1 | gawk '{ print strftime("%H:%M:%S>"), $0; fflush(); }'

