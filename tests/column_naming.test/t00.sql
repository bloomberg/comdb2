create table t1(i int)$$
insert into t1 values(1);
select * from t1;
select i from t1;
select I from t1;
select i from t1 union select 1;
select I from t1 union select 1;
drop table t1;
