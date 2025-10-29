local function define_emit_columns()
    local num = db:exec("select count(*) as num from comdb2_columns where tablename='t1'"):fetch().num
    local total = 2 + (num) -- type, tablename, new column values, old column values
    db:num_columns(total)
    db:column_name('tbl', 1)  db:column_type('cstring', 1)
    db:column_name('type', 2) db:column_type('cstring', 2)
    local stmt = db:exec("select columnname as name, type as type from comdb2_columns where tablename='t1'")
    local r = stmt:fetch()
    local i = 2
    while r do
        i = i + 1
        db:column_name(r.name, i)                db:column_type(r.type, i)
        r = stmt:fetch()
    end
end

local function main()
    define_emit_columns()
    local consumer = db:consumer()
    while true do
        local change = consumer:get() -- blocking call
        local row = {}
        if change.new ~= nil then
            -- row = change.new
            for k, v in pairs(change.new) do
                row[k] = v
            end
        end
        if change.old ~= nil then
            for k, v in pairs(change.old) do
                row[k] = v
            end
        end
        row.tbl = change.name
        row.type = change.type
        consumer:emit(row) -- blocking call
        consumer:consume()
    end
end
