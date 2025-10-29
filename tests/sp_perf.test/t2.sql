drop table if exists t
create table t {
    schema {
        int id
        cstring a[16] dbstore="" null=yes
    }
}$$
insert into t (id, a) values (1, 'shortstr')
create procedure update_t version 'test' {
    local function main(val, id)
        local sql = "update t set a=? where id=?"
        local res, rc = db:prepare(sql)
        if (rc~=0) then
            return -200, ("got error " .. rc .. db:error())
        end
        res:bind(1, val)
        res:bind(2,id)
        rc = res:exec()
        if (rc~=0) then
            print("error when executing sql", rc, db:error())
            return -1, ("got error on exec" .. rc .. db:error())
        end
        print("updated rows:", res:rows_changed())
        return 0
    end
}$$
@bind CDB2_INTEGER id 1
@bind CDB2_CSTRING val newstr
exec procedure update_t(@val, @id)
select * from t 
