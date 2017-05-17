set transaction read committed
begin
selectv group_id, opt_type, opt_value from t2 where group_id = 1117
update t2 set group_id = 1117, opt_type=113, opt_value=1 where group_id=1117 and opt_type=113
commit
