drop table if exists time_part_t;
drop table if exists t2;

create table time_part_t(i int)$$
create table t2(i int)$$

insert into time_part_t values(1),(2),(3);
insert into t2 values(1),(2),(3);

# create a time partition
create time partition on time_part_t as time_part_v period 'daily' retention 2 start '{{STARTTIME}}';

select sleep(5);

# create a view
create view t2_v as select * from t2;

select * from time_part_t;
select * from t2;
select * from t2_v;

# this should only list 'user' views
select * from comdb2_views;

# this should list everything!
select * from sqlite_master where name not like 'sqlite%' order by name;

# cleanup
drop table t2;
drop view t2_v;
# Can't schema change time partition
#drop table time_part_t;
