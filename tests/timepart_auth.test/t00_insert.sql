-- create 2 users
put password 'root' for 'root';
put password 'foo' for 'foo';

-- grant OP to 'root
grant op to 'root';

set user 'root'
set password 'root'
-- i am an OP user
put authentication on
-- authentication enabled

select * from comdb2_users;

create table t1(i int)$$

-- grant read on t1 (the base table) to 'foo'.
-- note: since this is done before the creation of time partition,
-- this access permission must be inherited by the time partition
-- that gets created on this table as well as all its shards.
grant read on 't1' to 'foo';

-- create time paritions
create time partition on t1 as t1_tp period 'yearly' retention 2 start '2018-01-01';

select sleep(5);

-- grant write on t1_tp (the time partition) to 'foo'.
grant write on 't1_tp' to 'foo';

#select 'time partitions' as rows;
#select * from comdb2_timepartitions;
select 'time partition shards' as rows;
select * from comdb2_timepartshards;
select 'time partition shard permissions' as rows;
select * from comdb2_timepartpermissions;
select 'table permissions' as rows;
select * from comdb2_tablepermissions where username = 'foo' and tablename like '$%';

insert into t1_tp values(1);

-- login as non-OP user
set user foo
set password foo
-- the following commands must succeed
insert into t1_tp values(2);
select * from t1_tp order by 1;

-- try revoking write permission
set user 'root'
set password 'root'
revoke write on 't1_tp' to 'foo';
select 'table permissions' as rows;
select * from comdb2_tablepermissions where username = 'foo' and tablename like '$%';

-- login as non-OP user
set user foo
set password foo
-- the following insert must fail now
insert into t1_tp values(3);
select * from t1_tp order by 1;


-- cleanup
set user 'root'
set password 'root'
put authentication off;

put password off for 'root'
put password off for 'foo'
select * from comdb2_users;

#drop view t1_tp;
#select * from comdb2_tables;
#select * from comdb2_timepartitions;
#select * from comdb2_timepartshards;
