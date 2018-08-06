#!/usr/bin/env bash
. /home/kliang43/comdb2/tests/phys_reptest.test/setenv.sh 
cd /home/kliang43/comdb2/tests/phys_reptest.test/
./runit physreptest50758 $1 
echo; echo; echo;
/home/kliang43/comdb2/tests/unsetup
