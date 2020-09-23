drop table if exists t1;

create table t1(i int)$$
insert into t1 values(1),(2),(3);

# create a time partition
create time partition on t1 as t1_tp period 'daily' retention 2 start '{{STARTTIME}}';

select sleep(5);

select * from t1_tp;

# Schema change the time partition
alter table t1_tp add column j int null$$
select name, csc2 from sqlite_master where name not like 'sqlite%' and type = 'table' order by name;

# We should now see the newly added column
select * from t1_tp;

# Try creating indices
create unique index idx1 on t1_tp(i);
create index idx2 on t1_tp(i);
select name, csc2 from sqlite_master where name not like 'sqlite%' and type = 'table' order by name;

# Schema change the time partition, again
alter table t1_tp drop column i$$
select name, csc2 from sqlite_master where name not like 'sqlite%' and type = 'table' order by name;

# We should NOT see the dropped column anymore
select * from t1_tp;

# cleanup
drop time partition t1_tp;
