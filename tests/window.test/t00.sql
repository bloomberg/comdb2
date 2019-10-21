select '1. test for a assertion failure' as test;

create table t1(i int)$$
insert into t1 values(1), (2), (2), (3), (3), (3);
select count(i) over (order by i) from t1;
select i, count(i) over (order by i), rank() over (order by i) from t1;
drop table t1;
