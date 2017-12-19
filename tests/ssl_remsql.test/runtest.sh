#!/usr/bin/env bash
export TESTID="3007"
export SRCHOME="/bb/bigstorn/systems/dhogea/comdb2/OPEN/c5"
export TESTCASE="ssl_remsql"
export DBNAME="sslremsql3007"
export DBDIR="/bb/bigstorn/systems/dhogea/comdb2/OPEN/c5/tests/test_3007/sslremsql3007"
export TESTSROOTDIR="/bb/bigstorn/systems/dhogea/comdb2/OPEN/c5/tests"
export TESTDIR="/bb/bigstorn/systems/dhogea/comdb2/OPEN/c5/tests/test_3007"
export TMPDIR="/bb/bigstorn/systems/dhogea/comdb2/OPEN/c5/tests/test_3007/tmp"
export CDB2_OPTIONS="--cdb2cfg /bb/bigstorn/systems/dhogea/comdb2/OPEN/c5/tests/test_3007/sslremsql3007/comdb2db.cfg"
export CDB2_CONFIG="/bb/bigstorn/systems/dhogea/comdb2/OPEN/c5/tests/test_3007/sslremsql3007/comdb2db.cfg"
export COMDB2_EXE="/bb/bigstorn/systems/dhogea/comdb2/OPEN/c5/comdb2"
export COMDB2MD5SUM="60172e45e454aa75b7dd9701b74ffb71"
export CDB2SQL_EXE="/bb/bigstorn/systems/dhogea/comdb2/OPEN/c5/cdb2sql"
export PATH=${SRCHOME}/:${PATH}
export SKIPSSL="1"
cd /bb/bigstorn/systems/dhogea/comdb2/OPEN/c5/tests/ssl_remsql.test/
./runit sslremsql3007 
echo; echo; echo;
/bb/bigstorn/systems/dhogea/comdb2/OPEN/c5/tests/unsetup
