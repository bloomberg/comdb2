local function main(tbl, max)
        local stmt = "insert into " .. tbl .. " values(@a)"
        local dbtab = db:prepare(stmt)
        local i = 1
        local tran = 0
        local rc = 0

        db:begin()
        local strt=100000
        for i = strt+1, strt+max do
                dbtab:bind("a", i)
                dbtab:exec()

                tran = tran + 1
                if tran > 100000 then
                    rc = db:commit()
                    if (rc ~= 0) then
                        return -225, db:error()..tostring(rc)
                    end
                    tran = 0
                    db:begin()
                end
        end

        rc = db:commit()
        if rc ~= 0 then
            return -225, db:error()..tostring(rc)
        end

        dbtab = db:exec("select count(*) from t2")
        dbtab:emit()
end

