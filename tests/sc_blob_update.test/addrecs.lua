local function main(max)
        db:setmaxinstructions(20000000)
        local dbtab = db:prepare("insert into t2(a,b,c,d,e,f) values(@a, 'test1', x'1234', @d, @e, @f)")
        local dbtab2 = db:prepare("insert into t1(a) values(@a)")
        local i = 1
        local tran = 0
        local rc = 0

        db:begin()
        local strt=100000
        for i = strt+1, strt+max do
                local j = max - i
                local k = i
                local l = i
                dbtab2:bind("a", i)
                dbtab2:exec()

                dbtab:bind("a", i)
                dbtab:bind("d", i)
                dbtab:bind("e", i*2)
                dbtab:bind("f", i)
                dbtab:exec()

                tran = tran + 1
                if tran > 100000 then
                    rc = db:commit()
                    if rc ~= 0 then
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

