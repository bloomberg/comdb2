local function main()
    db:begin()
    local select_stmt = "selectv a from commitsp"
    local stmt, rc = db:prepare(select_stmt)
    if(rc ~= 0) then
        return -201, "Error preparing selectv statement"
    end

    rc = stmt:exec()
    if(rc ~= 0) then
        return -202, "Error executing selectv statement"
    end

    local a = stmt:fetch()

    if(a == nil) then
        return -203, "No record found"
    end

    stmt:fetch()

    local update_stmt = "update commitsp set a = " .. a.a + 1 .. " where a = " .. a.a
    stmt, rc = db:prepare(update_stmt)
    if(rc ~= 0) then
        return -204, "Error preparing update statement"
    end

    rc = stmt:exec()
    if(rc ~= 0) then
        return -205, "Error executing update statement"
    end

    rc = db:commit()
    return 0
end
