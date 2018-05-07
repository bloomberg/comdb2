#!/usr/bin/env bash
export HOSTNAME="pnj-comdb2btda1"
export TESTID="39725"
export SRCHOME="/bb/bigstorn/systems/dhogea/comdb2/OPEN/c5"
export TESTCASE="timepart_constraints"
export DBNAME="timepartconstraints39725"
export DBDIR="/bb/bigstorn/systems/dhogea/comdb2/OPEN/c5/tests/test_39725/timepartconstraints39725"
export TESTSROOTDIR="/bb/bigstorn/systems/dhogea/comdb2/OPEN/c5/tests"
export TESTDIR="/bb/bigstorn/systems/dhogea/comdb2/OPEN/c5/tests/test_39725"
export TMPDIR="/bb/bigstorn/systems/dhogea/comdb2/OPEN/c5/tests/test_39725/tmp"
export CDB2_OPTIONS="--cdb2cfg /bb/bigstorn/systems/dhogea/comdb2/OPEN/c5/tests/test_39725/timepartconstraints39725/comdb2db.cfg"
export CDB2_CONFIG="/bb/bigstorn/systems/dhogea/comdb2/OPEN/c5/tests/test_39725/timepartconstraints39725/comdb2db.cfg"
export COMDB2_EXE="/bb/bigstorn/systems/dhogea/comdb2/OPEN/c5/build/db/comdb2"
export CDB2SQL_EXE="/bb/bigstorn/systems/dhogea/comdb2/OPEN/c5/build/tools/cdb2sql/cdb2sql"
export COMDB2AR_EXE="/bb/bigstorn/systems/dhogea/comdb2/OPEN/c5/build/tools/comdb2ar/comdb2ar"
export pmux_port="5105"
export PATH=/bb/bigstorn/systems/dhogea/comdb2/OPEN/c5/build/tools/comdb2ar:/bb/bigstorn/systems/dhogea/comdb2/OPEN/c5/build/db:/bb/bigstorn/systems/dhogea/comdb2/OPEN/c5/build/tools/cdb2sql:/bb/bigstorn/systems/dhogea/comdb2/OPEN/c5/build/tools/pmux:${PATH}
cd /bb/bigstorn/systems/dhogea/comdb2/OPEN/c5/tests/timepart_constraints.test/
./runit timepartconstraints39725 
echo; echo; echo;
/bb/bigstorn/systems/dhogea/comdb2/OPEN/c5/tests/unsetup
