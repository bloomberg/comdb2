db=$2

cdb2sql ${CDB2_OPTIONS} $db "create table t8a (a integer)"
cdb2sql ${CDB2_OPTIONS} $db "create table t8b (a integer)"

cdb2sql ${CDB2_OPTIONS} $db default - <<EOF
insert into t8a values(1)
insert into t8a values(2)
insert into t8b values(1)
insert into t8b values(2)
begin
insert into t8a values(3)
insert into t8b values(3)
update t8a set a=4 where a=2
update t8b set a=4 where a=2
delete from t8a where a=1
delete from t8b where a=1
commit
select * from comdb2_table_hits where tablename in ('t8a', 't8b') order by tablename
EOF
