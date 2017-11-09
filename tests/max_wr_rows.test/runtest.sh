#!/usr/bin/env bash
export TESTID="41762"
export SRCHOME="/home/mhannum/comdb2"
export TESTCASE="max_wr_rows"
export DBNAME="maxwrrows41762"
export DBDIR="/home/mhannum/comdb2/tests/test_41762/maxwrrows41762"
export TESTSROOTDIR="/home/mhannum/comdb2/tests"
export TESTDIR="/home/mhannum/comdb2/tests/test_41762"
export TMPDIR="/home/mhannum/comdb2/tests/test_41762/tmp"
export CDB2_OPTIONS="--cdb2cfg /home/mhannum/comdb2/tests/test_41762/maxwrrows41762/comdb2db.cfg"
export CDB2_CONFIG="/home/mhannum/comdb2/tests/test_41762/maxwrrows41762/comdb2db.cfg"
export COMDB2_EXE="/home/mhannum/comdb2/comdb2"
export COMDB2MD5SUM="989c6e82368d49dfa9c6ae6341b846a8"
export CDB2SQL_EXE="/home/mhannum/comdb2/cdb2sql"
export PATH=${SRCHOME}/:${PATH}
export SKIPSSL="1"
cd /home/mhannum/comdb2/tests/max_wr_rows.test/
./runit maxwrrows41762 
echo; echo; echo;
# /home/mhannum/comdb2/tests/unsetup
