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

create table t2(i int)$$

grant read on 't2' to 'foo';
grant ddl on 't2' to 'foo';

-- create time paritions
create time partition on t2 as t2_tp period 'yearly' retention 2 start '2018-01-01';

select sleep(5);

#select 'time partitions' as rows;
#select * from comdb2_timepartitions;
select 'time partition shards' as rows;
select * from comdb2_timepartshards;
select 'time partition shard permissions' as rows;
select * from comdb2_timepartpermissions;
select 'table permissions' as rows;
select * from comdb2_tablepermissions where username = 'foo' and tablename like '$%';

insert into t2_tp values(1);

-- login as non-OP user
set user foo
set password foo
-- the following command must succeed
alter table t2_tp { schema { int i int j null=yes } }$$
insert into t2_tp values(2, 2);
select * from t2_tp order by 1;

-- try revoking write permission
set user 'root'
set password 'root'
revoke ddl on 't2_tp' to 'foo';
select 'table permissions' as rows;
select * from comdb2_tablepermissions where username = 'foo' and tablename like '$%';

-- login as non-OP user
set user foo
set password foo
-- the following alter must fail now
alter table t2_tp { schema { int i int j null=yes } }$$
select * from t2_tp order by 1;

-- cleanup
set user 'root'
set password 'root'
put authentication off;

put password off for 'root'
put password off for 'foo'
select * from comdb2_users;
