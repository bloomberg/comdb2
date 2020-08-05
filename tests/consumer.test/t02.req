drop table if exists t
create table t(d0 datetime, d1 datetimeus)$$
create procedure queue_tz version 'sptest' {
local function main()
    local c = db:consumer()
    local e = c:get()
    c:emit(e.new.d0, e.new.d1)
    c:consume()
end
}$$
create lua consumer queue_tz on (table t for insert)
insert into t values(1, 1)
insert into t values(2, 2)
SET TIMEZONE UTC
exec procedure queue_tz()
SET TIMEZONE America/New_York
exec procedure queue_tz()
