set transaction read committed
insert into t5 values (1, 1)
begin
update t5 set b = 2
update t5 set b = 3
commit
select * from t5
exec procedure sys.cmd.verify('t5')
begin
update t5 set b = 4
update t5 set b = 5
commit
select * from t5
exec procedure sys.cmd.verify('t5')
begin
update t5 set b = 6
update t5 set b = 7
commit
select * from t5
exec procedure sys.cmd.verify('t5')
begin
update t5 set b = 2
update t5 set b = 3
commit
select * from t5
exec procedure sys.cmd.verify('t5')
truncate t5
select * from t5
