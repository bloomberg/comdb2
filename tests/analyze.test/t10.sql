set transaction read committed
begin
selectv * from t10 where id > 20 order by id
commit
