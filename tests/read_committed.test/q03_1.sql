delete from t1 where 1
insert into t1 (a,b) values (2,1)
set transaction read committed
begin
insert into t1 (a,b) values (1,2)
insert into t1 (a,b) values (0,4)
insert into t1 (a,b) values (5,6)
select a,b from t1 where b = 4
rollback
