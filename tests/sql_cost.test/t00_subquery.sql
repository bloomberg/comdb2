# DRQS-162160366
create table t1(i int unique, j int)$$
create table t2(x int unique, y int)$$

insert into t1 select value, 1 from generate_series(1, 10);
insert into t2 select value, 2 from generate_series(1, 10);

select 'Subquery flattening optimization enabled' as comment;
put tunable enable_sq_flattening_optimization 1 
select i, j, a.x, a.y from t1 left join (select x,y from t2 where x=11) a on t1.i = a.x order by i asc;

select 'Subquery flattening optimization disabled' as comment;
put tunable enable_sq_flattening_optimization 0
select i, j, a.x, a.y from t1 left join (select x,y from t2 where x=11) a on t1.i = a.x order by i asc;

# cleanup
drop table t2;
drop table t1;
