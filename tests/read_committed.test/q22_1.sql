drop table if exists t
create table t(i int, j int)$$
create index t_ij on t(i, j)
insert into t(i, j) values(1,1),(2,2)

set transaction read committed
begin
delete from t where i = 1
select max(j) from t where i = 1
commit
select max(j) from t where i = 1
