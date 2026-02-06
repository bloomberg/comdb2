-- testing string to blob conversion in sp
drop table if exists t
create table t (b blob)$$
create procedure p version 't' {
    local function main(str)
        local t = db:table("t")
        local b = db:cast(str, 'blob')
        return t:insert({b=b})
    end
}$$
exec procedure p("text")
exec procedure p("")
select * from t
