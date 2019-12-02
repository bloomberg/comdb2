select '==== test 1 ====' as test;
create table t1(i int)$$
insert into t1 values(1);
select * from t1;
select i from t1;
select I from t1;
select i from t1 union select 1;
# Should get "I" with legacy_column_name enabled
select I from t1 union select 1;
drop table t1;

select '==== test 2 ====' as test;
create table t1(i int)$$
insert into t1 values(1);
# Should get "t1.i" with legacy_column_name enabled
select t1.i from t1 join (select i from t1);
drop table t1;

select '==== test 3 ====' as test;
create table t1(i int)$$
insert into t1 values(1);
select a.i,b.i from t1 a, t1 b;
select a.i,b.i from t1 a, t1 b group by a.i;
drop table t1;
