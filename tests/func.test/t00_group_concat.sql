select '== test for group_concat_memory_limit/groupconcatmemlimit ==' as t;

create table t1(i int, v varchar(10000))$$
insert into t1 select value, printf('%.10000c', '*') from generate_series(1, 30000);

# pass (no limit)
set groupconcatmemlimit 0
select length(group_concat(v)) as pass from t1;

set groupconcatmemlimit 10001
select length(group_concat(v)) as pass from t1 where i in (1);
select length(group_concat(v)) as fail from t1 where i in (1,2);

set groupconcatmemlimit 20002
select length(group_concat(v)) as pass from t1 where i in (1,2);

drop table t1;

# Test smaller lengths
create table t1(i int)$$
insert into t1 values(1),(2),(3),(4),(5);

set groupconcatmemlimit 0
select group_concat(i) as pass from t1;

set groupconcatmemlimit 1
select group_concat(i) as fail from t1;

# pass (sqlite allocates in multiples of 8)
set groupconcatmemlimit 9
select group_concat(i) as pass from t1;

# cleanup
drop table t1;

