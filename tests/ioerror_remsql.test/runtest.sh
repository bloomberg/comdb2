#!/usr/bin/env bash
. /bb/bigstorn/systems/dhogea/comdb2/OPEN/c112/tests/ioerror_remsql.test/setenv.sh
cd /bb/bigstorn/systems/dhogea/comdb2/OPEN/c112/tests/ioerror_remsql.test/
./runit ioerrorremsql36629 
echo; echo; echo;
/bb/bigstorn/systems/dhogea/comdb2/OPEN/c112/tests/unsetup
