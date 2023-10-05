create table t(a int)$$
insert into t select * from generate_series(1, 10);
select * from t; -- test query that doesn't need to use queue
