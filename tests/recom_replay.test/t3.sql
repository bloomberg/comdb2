set transaction read committed
begin
update t3 set y = 0 where x = 0
commit
