set transaction read committed
begin
insert into t4 (id, attr, a) values (1, 1, 10.00)
insert into t4 (id, attr, a) values (1, 2, 10.00)
update t4 set a=0.0 where id=1 and attr=2
update t4 set attr=10 where id = 1 and a>0.0
commit
select * from t4 order by id, attr
