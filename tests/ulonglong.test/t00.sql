# Turn off forbid_ulonglong
put tunable forbid_ulonglong = 'off';

create table t1 { schema { u_longlong u } } $$
insert into t1 values(1);

# Turn it back on
put tunable forbid_ulonglong = 'on';

insert into t1 values(2);
select * from t1 order by 1;

# CREATE/ALTER TABLE commands must fail
create table t2 { schema { u_longlong u } } $$
alter table t1 { schema { u_longlong u int i null=yes } } $$

put tunable forbid_ulonglong = 'off';
alter table t1 { schema { u_longlong u int i null=yes } } $$
select * from t1 order by 1;

# REBUILD/ANALYZE/TRUNCATE should work
put tunable forbid_ulonglong = 'on';
rebuild t1
analyze t1
truncate t1;
select * from t1 order by 1;

# Ensure that time partition rollouts are not affected by 'forbid_ulonglong'
create time partition on t1 as t1_tp period 'yearly' retention 2 start '2020-01-01';
select sleep(5);
select count(*) from comdb2_timepartshards;
insert into t1_tp values(3,3), (4,5);
select * from t1_tp order by 1;

put tunable forbid_ulonglong = 'off';
create table tlong {tag ondisk {u_longlong u}} $$

# a valid unsigned long long without bind should still fail
insert into tlong values(18446744073709551615)

#cdb2sql only accepts signed long
@bind CDB2_INTEGER a -1

#this should succeed
insert into tlong values (@a);
