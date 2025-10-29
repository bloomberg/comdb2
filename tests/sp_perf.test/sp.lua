local function insert_into_cstring_table(row)
    local cstr_row = {}
    for k, v in pairs(row) do
        v = db:cast(v, 'cstring')
        if v ~= nil then
            cstr_row[k] = v
        else
            cstr_row[k] = nil
        end
    end
    local t1_cstr = db:table("t1_cstr")
    return t1_cstr:insert(cstr_row)
end


local function test_cstring_conversions(cstr_fetched)
    db:num_columns(3)
    db:column_name('conversion', 1)  db:column_type('cstring', 1)
    db:column_name('input', 2)       db:column_type('cstring', 2)
    db:column_name('output', 3)      db:column_type('cstring', 3)
    
    local int_val = db:cast(cstr_fetched.alltypes_short, 'int')
    db:emit({ conversion = "cstring->int", input = tostring(cstr_fetched.alltypes_short), output = tostring(int_val) })
    local real_val = db:cast(cstr_fetched.alltypes_float, 'real')
    db:emit({ conversion = "cstring->real", input = tostring(cstr_fetched.alltypes_float), output = tostring(real_val) })
    local blob_val = db:cast(cstr_fetched.alltypes_blob, 'blob')
    db:emit({ conversion = "cstring->blob", input = tostring(cstr_fetched.alltypes_blob), output = tostring(blob_val) })
    local dt_val = db:cast(cstr_fetched.alltypes_datetime, 'datetime')
    db:emit({ conversion = "cstring->datetime", input = tostring(cstr_fetched.alltypes_datetime), output = tostring(dt_val) })
    local dec_val = db:cast(cstr_fetched.alltypes_decimal32, 'decimal')
    db:emit({ conversion = "cstring->decimal", input = tostring(cstr_fetched.alltypes_decimal32), output = tostring(dec_val) })
    local ym_val = db:cast(cstr_fetched.alltypes_intervalym, 'intervalym')
    db:emit({ conversion = "cstring->intervalym", input = tostring(cstr_fetched.alltypes_intervalym), output = tostring(ym_val) })
    local ds_val = db:cast(cstr_fetched.alltypes_intervalds, 'intervalds')
    db:emit({ conversion = "cstring->intervalds", input = tostring(cstr_fetched.alltypes_intervalds), output = tostring(ds_val) })
end

local function strip_quotes(s)
    if s == nil then return "" end
    return string.gsub(s, "^'(.*)'$", "%1")
end

local function insert_from_cstring_row(cstr_fetched, new_short_val)
    local insert_sql = string.format([[
        INSERT INTO t1 (
            alltypes_short, alltypes_u_short, alltypes_int, alltypes_u_int, alltypes_longlong,
            alltypes_float, alltypes_double, alltypes_byte, alltypes_cstring, alltypes_pstring,
            alltypes_blob, alltypes_datetime, alltypes_datetimeus, alltypes_vutf8,
            alltypes_intervalym, alltypes_intervalds, alltypes_intervaldsus,
            alltypes_decimal32, alltypes_decimal64, alltypes_decimal128
        ) VALUES (
            %d, %s, %s, %s, %s,
            %s, %s, x'%s', '%s', '%s',
            x'%s', '%s', '%s', '%s',
            '%s', '%s', '%s',
            %s, %s, %s
        )
    ]],
        new_short_val,
        cstr_fetched.alltypes_u_short,
        cstr_fetched.alltypes_int,
        cstr_fetched.alltypes_u_int,
        cstr_fetched.alltypes_longlong,
        cstr_fetched.alltypes_float,
        cstr_fetched.alltypes_double,
        cstr_fetched.alltypes_byte,
        strip_quotes(cstr_fetched.alltypes_cstring),
        strip_quotes(cstr_fetched.alltypes_pstring),
        cstr_fetched.alltypes_blob,
        cstr_fetched.alltypes_datetime,
        cstr_fetched.alltypes_datetimeus,
        strip_quotes(cstr_fetched.alltypes_vutf8),
        cstr_fetched.alltypes_intervalym,
        cstr_fetched.alltypes_intervalds,
        cstr_fetched.alltypes_intervaldsus,
        cstr_fetched.alltypes_decimal32,
        cstr_fetched.alltypes_decimal64,
        cstr_fetched.alltypes_decimal128
    )
    
    local stmt = db:exec(insert_sql)
    if stmt == nil then
        return -1, db:error()
    end
    return 0
end


local function main()
    local rc = db:begin()
    local stmt = db:exec("SELECT * FROM t1 LIMIT 1")
    local row = stmt:fetch()
    if row == nil then
        db:rollback()
        return -1, "No rows found in t1"
    end
    rc = insert_into_cstring_table(row)
    if rc ~= 0 then
        db:rollback()
        return rc, "Failed to insert into t1_cstr"
    end
    rc = db:commit()    
    if rc ~= 0 then
        return rc, "Failed to commit insert to t1_cstr: " .. db:error()
    end

    
    rc = db:begin()
    if rc ~= 0 then
        return rc, "Failed to begin second transaction"
    end
    local stmt2 = db:exec("SELECT * FROM t1_cstr")
    local cstr_fetched = stmt2:fetch()
    if cstr_fetched == nil then
        return -1, "Failed to read back from t1_cstr"
    end
    test_cstring_conversions(cstr_fetched)
    local short_val = db:cast(cstr_fetched["alltypes_short"], 'int')
    short_val = short_val + 1
    local rc, err = insert_from_cstring_row(cstr_fetched, short_val)
    if rc ~= 0 then
        print("Reinsert failed: ", err)
        db:rollback()
        return rc, "Failed to reinsert into t1: " .. (err or "")
    end
    rc = db:commit()
    if rc ~= 0 then
        return rc, "Failed to commit final transaction: " .. db:error()
    end
    
    return 0, "Success"
end
