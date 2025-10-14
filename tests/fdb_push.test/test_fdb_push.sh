#!/usr/bin/env bash

# Remote cursor moves testcase for comdb2
################################################################################


# args
# <dbname> <autodbname> <dbdir> <testdir>
a_remdbname=$1
a_remcdb2config=$2
a_dbname=$3
a_cdb2config=$4
a_remdbname2=$5
a_remcdb2config2=$6
a_dbdir=$7
a_testdir=$8

output=run.out

REM_CDB2_OPTIONS="--cdb2cfg ${a_remcdb2config}"
REM_CDB2_OPTIONS2="--cdb2cfg ${a_remcdb2config2}"
SRC_CDB2_OPTIONS="--cdb2cfg ${a_cdb2config}"

# Make sure we talk to the same host
mach=`cdb2sql ${SRC_CDB2_OPTIONS} --tabs $a_dbname default "SELECT comdb2_host()"`

#TEST1 after a good query, try to access a missing table, followed by some good tables

# populate table on remote
cdb2sql -s ${REM_CDB2_OPTIONS} $a_remdbname default - < remdata.req > $output 2>&1

# retrieve data through remote sql
cdb2sql ${SRC_CDB2_OPTIONS} --host $mach $a_dbname "select * from LOCAL_${a_remdbname}.t order by id" >> $output 2>&1

# get the version V2
cdb2sql ${SRC_CDB2_OPTIONS} --tabs --host $mach $a_dbname "exec procedure sys.cmd.send(\"fdb info db\")" 2>&1 | cut -f 5- -d ' ' >> $output

# make sure clnt->fdb_push is cleared when running local stmt after foreign stmt (insert stmt will fail if clnt->fdb_push is not cleared)
echo "Test running fdb stmt followed by local stmt" >> $output
cdb2sql -s ${SRC_CDB2_OPTIONS} --host $mach $a_dbname - >> $output 2>&1 << EOF
select * from LOCAL_${a_remdbname}.t order by id
insert into t values (10, "hi")
EOF

echo "Test parameters" >> $output
cdb2sql -s ${REM_CDB2_OPTIONS} $a_remdbname default - >> $output 2>&1 << EOF
insert into t2(i) values (10), (20)
insert into t2(r) values (1.0), (1.2)
insert into t2(s) values ('hi'), ('ho')
insert into t2(b) values (x'deadbeaf')
insert into t2(d) values ('20230913T'), ('20230914T')
insert into t2(d2) values ('2023-09-13T00:00:00.000001'), ('2023-09-14T00:00:00.000001')
EOF

cdb2sql -s ${SRC_CDB2_OPTIONS} $a_dbname default - >> $output 2>&1 << EOF
@bind CDB2_INTEGER i 20
select * from LOCAL_${a_remdbname}.t2 where i=@i
EOF

cdb2sql -s ${SRC_CDB2_OPTIONS} $a_dbname default - >> $output 2>&1 << EOF
@bind CDB2_REAL r 1.2
select * from LOCAL_${a_remdbname}.t2 where r=@r
EOF

cdb2sql -s ${SRC_CDB2_OPTIONS} $a_dbname default - >> $output 2>&1 << EOF
@bind CDB2_CSTRING s hi
select * from LOCAL_${a_remdbname}.t2 where s=@s
EOF

cdb2sql -s ${SRC_CDB2_OPTIONS} $a_dbname  default - >> $output 2>&1 << EOF
@bind CDB2_BLOB b x'deadbeaf'
select * from LOCAL_${a_remdbname}.t2 where b=@b
EOF

cdb2sql -s ${SRC_CDB2_OPTIONS} $a_dbname default - >> $output 2>&1 << EOF
@bind CDB2_DATETIME d 2023-09-13T00:00:00
select * from LOCAL_${a_remdbname}.t2 where d=@d
EOF

cdb2sql -s ${SRC_CDB2_OPTIONS} $a_dbname default - >> $output 2>&1 << EOF
@bind CDB2_DATETIMEUS d2 2023-09-13T00:00:00.000001
select * from LOCAL_${a_remdbname}.t2 where d2=@d2
EOF

echo "Test set statement" >> $output
cdb2sql -s ${SRC_CDB2_OPTIONS} $a_dbname default - >> $output 2>&1 << EOF
set timezone Asia/Tokyo
select * from LOCAL_${a_remdbname}.t2 where d="2023-09-13T13:00:00"
set maxquerytime 10
select * from LOCAL_${a_remdbname}.t2 where d="2023-09-13T13:00:00"
set timezone America/Jamaica
select * from LOCAL_${a_remdbname}.t2 where d="2023-09-13T23:00:00"
select * from LOCAL_${a_remdbname}.t2 where d="2023-09-13T23:00:00"
EOF

# drop sqlite_stat1 table from remote and make sure can still retrieve data
# needs to be first query sent to remote db, so use a new db
echo "Test running with no sqlite_stat1" >> $output
cdb2sql -s ${REM_CDB2_OPTIONS2} $a_remdbname2 default - < remdata.req >> $output 2>&1
cdb2sql ${REM_CDB2_OPTIONS2} $a_remdbname2 default "drop table if exists sqlite_stat1" >> $output 2>&1
cdb2sql ${SRC_CDB2_OPTIONS} --host $mach $a_dbname "select * from LOCAL_${a_remdbname2}.t order by id" >> $output 2>&1

# problem when running fdb stmt followed by local stmt in client transaction
echo "Make sure disabled for client transactions" >> $output
cdb2sql -s ${SRC_CDB2_OPTIONS} $a_dbname default - >> $output 2>&1 << EOF
begin
select * from LOCAL_${a_remdbname}.t order by id
select 1
commit
EOF

# test set options
echo "Test set force_fdb_push redirect" >> $output
cdb2sql -s ${SRC_CDB2_OPTIONS} $a_dbname default - >> $output 2>&1 << EOF
set force_fdb_push redirect
select * from LOCAL_${a_remdbname}.t order by id
EOF

echo "Test set force_fdb_push remote" >> $output
cdb2sql -s ${SRC_CDB2_OPTIONS} $a_dbname default - >> $output 2>&1 << EOF
set force_fdb_push remote
select * from LOCAL_${a_remdbname}.t order by id
EOF

echo "Test set force_fdb_push off" >> $output
cdb2sql -s ${SRC_CDB2_OPTIONS} $a_dbname default - >> $output 2>&1 << EOF
set force_fdb_push off
select * from LOCAL_${a_remdbname}.t order by id
EOF

echo "Test effects with mix of local stmt and fdb stmt in transaction" >> $output
cdb2sql -s -showeffects ${SRC_CDB2_OPTIONS} $a_dbname default - >> $output 2>&1 << EOF
set verifyretry off
begin
insert into t values (5, 'Hello5')
insert into LOCAL_${a_remdbname}.t values (5, 'Hello5')
insert into t values (6, 'Hello6'), (7, 'Hello7')
insert into LOCAL_${a_remdbname}.t values (6, 'Hello6'), (7, 'Hello7')
commit
EOF

echo "Test effects with mix of local stmt and fdb stmt in transaction with verifyretry" >> $output
cdb2sql -s -showeffects ${SRC_CDB2_OPTIONS} $a_dbname default - >> $output 2>&1 << EOF
begin
insert into t values (8, 'Hello8')
insert into LOCAL_${a_remdbname}.t values (8, 'Hello8')
insert into t values (9, 'Hello9'), (100, 'Hello100')
insert into LOCAL_${a_remdbname}.t values (9, 'Hello9'), (100, 'Hello100')
commit
EOF

echo "effects for multiple selects part of the same transaction with verify retry" >> $output
cdb2sql -s -showeffects ${SRC_CDB2_OPTIONS} $a_dbname default -  >> $output 2>&1 << EOF
begin
select * from t order by 1
select * from t order by 1
insert into t values (1000, 'Hah')
select * from t order by 1
select * from t order by 1
update t set b1='Heh' where id = 9
select * from t order by 1
commit
select * from t order by 1
EOF

echo "effects for multiple selects part of the same transaction without verify retry" >> $output
cdb2sql -s -showeffects ${SRC_CDB2_OPTIONS} $a_dbname default -  >> $output 2>&1 << EOF
set verifyretry off
begin
select * from t order by 1
select * from t order by 1
insert into t values (1001, 'Hah')
select * from t order by 1
select * from t order by 1
update t set b1='Heh' where id = 1000
select * from t order by 1
commit
select * from t order by 1
EOF

echo "multiple chunk transactions on same socket, with selects" >> $output
cdb2sql -s -showeffects ${SRC_CDB2_OPTIONS} $a_dbname default -  >> $output 2>&1 << EOF
begin
select count(*) from t
select count(*) from LOCAL_${a_remdbname}.t
select count(*) from LOCAL_${a_remdbname2}.t
select * from t order by 1
select * from LOCAL_${a_remdbname}.t order by 1
select * from LOCAL_${a_remdbname2}.t order by 1
delete from t where id >= 6
delete from LOCAL_${a_remdbname}.t where id > 1
commit
begin
insert into t values (1, "H1"), (2,"H2")
insert into LOCAL_${a_remdbname}.t values (11, "H11"), (12, "H12")
select * from t order by 1
select * from LOCAL_${a_remdbname}.t order by 1
commit
select * from t order by 1
select * from LOCAL_${a_remdbname}.t order by 1
select * from LOCAL_${a_remdbname2}.t order by 1
EOF

echo "multiple chunk transactions on same socket, with selects, no verifyretry" >> $output
#echo cdb2sql -f q.txt -s -showeffects$a_dbname localhost - 
cdb2sql -s -showeffects ${SRC_CDB2_OPTIONS} $a_dbname default -  >> $output 2>&1 << EOF
set verifyretry off
begin
select count(*) from t
select count(*) from LOCAL_${a_remdbname}.t
select count(*) from LOCAL_${a_remdbname2}.t
select * from t order by 1
select * from LOCAL_${a_remdbname}.t order by 1
select * from LOCAL_${a_remdbname2}.t order by 1
update t set id = id + 100
update LOCAL_${a_remdbname}.t set id = id + 100
commit
begin
insert into t values (1, "H1D"), (2,"H2D")
insert into LOCAL_${a_remdbname}.t values (11, "H11D"), (12, "H12D")
select * from t order by 1
select * from LOCAL_${a_remdbname}.t order by 1
commit
select * from t order by 1
select * from LOCAL_${a_remdbname}.t order by 1
select * from LOCAL_${a_remdbname2}.t order by 1
EOF

echo "only remote writes, no verifyretry" >> $output
#echo cdb2sql -f q.txt -s -showeffects$a_dbname localhost - 
cdb2sql -s -showeffects ${SRC_CDB2_OPTIONS} $a_dbname default -  >> $output 2>&1 << EOF
set verifyretry off
begin
update LOCAL_${a_remdbname}.t set id = id + 100
delete from LOCAL_${a_remdbname2}.t
commit
EOF

#convert the table to actual dbname
sed "s/dorintdb/${a_remdbname}/g" output.log > output.log.actual

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
