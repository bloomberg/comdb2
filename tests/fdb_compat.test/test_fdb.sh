#!/usr/bin/env bash

# Remote cursor moves testcase for comdb2
################################################################################

set -x 

# args
# <dbname> <autodbname> <dbdir> <testdir>
a_rdbname=$1
a_rcdb2config=$2
a_dbname=$3
a_cdb2config=$4
a_dbdir=$5
a_testdir=$6

output=run.out

rm $output

R_DB2_OPTIONS="--cdb2cfg ${a_rcdb2config}"
S_CDB2_OPTIONS="--cdb2cfg ${a_cdb2config}"

# Make sure we talk to the same host
mach=`cdb2sql ${S_CDB2_OPTIONS} --tabs $a_dbname default "SELECT comdb2_host()"`

S_SQLT="cdb2sql ${S_CDB2_OPTIONS} --tabs --host $mach $a_dbname"
S_SQL="cdb2sql ${S_CDB2_OPTIONS} --host $mach $a_dbname"
R_SQLT="cdb2sql ${R_CDB2_OPTIONS} --tabs --host $mach $a_rdbname"
R_SQL="cdb2sql ${R_CDB2_OPTIONS} --host $mach $a_rdbname"

function check
{
    set -x
    $S_SQL "exec procedure sys.cmd.send('fdb info db')" 2>&1 | cut -f 5- -d ' ' >> $output
    $S_SQL "exec procedure sys.cmd.send('fdb init')"
    $R_SQL "exec procedure sys.cmd.send('fdb info db')" 2>&1 | cut -f 5- -d ' ' >> $output
    $R_SQL "exec procedure sys.cmd.send('fdb init')"
}
function header
{
    set -x
    echo "TEST $1"
    echo "$2"
    echo "TEST $1" >> $output
    echo "$2" >> $output
}

#grab current version
ver=`$R_SQLT "select value from comdb2_tunables where name='fdb_default_version'"`
echo "Current fdb version $ver" >> $output 2>&1

# populate table on remote
$S_SQL "insert into t select * from generate_series(1, 3)" >> $output 2>&1
$R_SQL "insert into t select * from generate_series(11, 13)" >> $output 2>&1

#gonna test remsql, disable push
$S_SQL "put tunable foreign_db_push_remote 0"
$R_SQL "put tunable foreign_db_push_remote 0"

header 1 "remsql run against same version"
$S_SQL "select * from LOCAL_${a_rdbname}.t order by id" >> $output 2>&1
check

echo $R_SQL "put tunable fdb_default_version 6"

echo $S_SQL "select * from LOCAL_${a_rdbname}.t order by id"
echo $R_SQL "select * from LOCAL_${a_dbname}.t order by id"

echo $R_SQL "put tunable fdb_default_version $ver"

header 2 "remsql run against a pre cdb2api version"
$R_SQL "put tunable fdb_default_version 6" >> $output 2>&1

$S_SQL "select * from LOCAL_${a_rdbname}.t order by id" >> $output 2>&1
$R_SQL "select * from LOCAL_${a_dbname}.t order by id" >> $output 2>&1
check

$R_SQL "put tunable fdb_default_version $ver" >> $output 2>&1

header 3 "test against a too new version"
let newver=ver+1
$R_SQL "put tunable fdb_default_version $newver" >> $output 2>&1

$S_SQL "select * from LOCAL_${a_rdbname}.t order by id" >> $output 2>&1
$R_SQL "select * from LOCAL_${a_dbname}.t order by id" >> $output 2>&1
check

$R_SQL "put tunable fdb_default_version $ver" >> $output 2>&1

header 4 "test against cdb2api remsql but not remtran"
$R_SQL "put tunable fdb_default_version 7" >> $output 2>&1

$S_SQL "select * from LOCAL_${a_rdbname}.t order by id" >> $output 2>&1
$R_SQL "select * from LOCAL_${a_dbname}.t order by id" >> $output 2>&1
check

$R_SQL "put tunable fdb_default_version $ver" >> $output 2>&1


header 5 "test insert, delete, update current version"

#gonna test remtran, it needs push code
$S_SQL "put tunable foreign_db_push_remote 1"
$R_SQL "put tunable foreign_db_push_remote 1"

#echo cdb2sql $a_dbname localhost "insert into LOCAL_${a_rdbname}.t(id) select * from generate_series(101,110)"
#echo cdb2sql $a_rdbname localhost "insert into LOCAL_${a_dbname}.t(id) select * from generate_series(101,110)"

$S_SQL "insert into LOCAL_${a_rdbname}.t(id) select * from generate_series(101,110)" >> $output 2>&1
$R_SQL "insert into LOCAL_${a_dbname}.t(id) select * from generate_series(101,110)" >> $output 2>&1
check

$S_SQL "update LOCAL_${a_rdbname}.t set id=id+1 where id>=101" >> $output 2>&1
$R_SQL "update LOCAL_${a_dbname}.t set id=id+1 where id>=101" >> $output 2>&1
check

$S_SQL "delete from LOCAL_${a_rdbname}.t where id>=101" >> $output 2>&1
$R_SQL "delete from LOCAL_${a_dbname}.t where id>=101" >> $output 2>&1
check

header 6 "remtran run against a pre cdb2api version"
$R_SQL "put tunable fdb_default_version 6" >> $output 2>&1

$S_SQL "insert into LOCAL_${a_rdbname}.t(id) select * from generate_series(101,110)" >> $output 2>&1
$R_SQL "insert into LOCAL_${a_dbname}.t(id) select * from generate_series(101,110)" >> $output 2>&1
check

$S_SQL "update LOCAL_${a_rdbname}.t set id=id+1 where id>=101" >> $output 2>&1
$R_SQL "update LOCAL_${a_dbname}.t set id=id+1 where id>=101" >> $output 2>&1
check

$S_SQL "delete from LOCAL_${a_rdbname}.t where id>=101" >> $output 2>&1
$R_SQL "delete from LOCAL_${a_dbname}.t where id>=101" >> $output 2>&1
check

$R_SQL "put tunable fdb_default_version $ver" >> $output 2>&1

header 7 "remtran test against a too new version"
$R_SQL "put tunable fdb_default_version $newver" >> $output 2>&1

$S_SQL "insert into LOCAL_${a_rdbname}.t(id) select * from generate_series(101,110)" >> $output 2>&1
$R_SQL "insert into LOCAL_${a_dbname}.t(id) select * from generate_series(101,110)" >> $output 2>&1
check

$S_SQL "update LOCAL_${a_rdbname}.t set id=id+1 where id>=101" >> $output 2>&1
$R_SQL "update LOCAL_${a_dbname}.t set id=id+1 where id>=101" >> $output 2>&1
check

$S_SQL "delete from LOCAL_${a_rdbname}.t where id>=101" >> $output 2>&1
$R_SQL "delete from LOCAL_${a_dbname}.t where id>=101" >> $output 2>&1
check

$R_SQL "put tunable fdb_default_version $ver" >> $output 2>&1

header 8 "remtran test for client transactions"
echo $S_SQL <<EOF
begin
insert into LOCAL_${a_rdbname}.t values (100)
commit
EOF
if [[ $? != 0 ]] ; then
    echo "Failed to run insert in a client txn"
    exit 1
fi
check

$S_SQL <<EOF
begin
update LOCAL_${a_rdbname}.t set id=id+1 where id=100
commit
EOF
if [[ $? != 0 ]] ; then
    echo "Failed to run update in a client txn"
    exit 1
fi
check

$S_SQL <<EOF
begin
delete from LOCAL_${a_rdbname}.t where id=101
commit
EOF
if [[ $? != 0 ]] ; then
    echo "Failed to run delete in a client txn"
    exit 1
fi
check

#convert the table to actual dbname
sed "s/dorintdb/${a_rdbname}/g" output.log > output.log.actual

# validate results 
testcase_output=$(cat $output)
expected_output=$(cat output.log.actual)
if [[ "$testcase_output" != "$expected_output" ]]; then

   # print message 
   echo "  ^^^^^^^^^^^^"
   echo "The above testcase (${testcase}) has failed!!!" 
   echo " "
   echo "Use 'diff <expected-output> <my-output>' to see why:"
   echo "> diff ${PWD}/{output.log.actual,$output}"
   echo " "
   diff output.log.actual $output
   echo " "

   # quit
   exit 1
fi

echo "Testcase passed."
