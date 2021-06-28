create table t1(i int, j int)$$
create index ix1 on t1(j);

insert into t1 select value, value % 2 from generate_series(1, 1000);
analyze t1

select i from t1 where j = 1;

# cleanup
drop table t1;
