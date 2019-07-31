#!/usr/bin/env bash

cdb2sql $SP_OPTIONS - <<'EOF'
create procedure tmp_tbl_and_thread version '1' {
local function create_tmp_tbl(t)
    return db:table(t, {{"i", "integer"}})
end
local function work(t, i)
    db:exec("insert into "..t:name().." select * from generate_series limit "..i.." * 100 offset 1")
    local tmp = create_tmp_tbl("tmp")
    tmp:copyfrom(t)
    local min = db:exec("select '"..t:name().."' as thd, min(i) as i from "..t:name())
    local max = db:exec("select '"..t:name().."' as thd, max(i) as i from "..tmp:name())
    min:emit()
    max:emit()
    return 0
end
local function create_tmp_tbl_and_thread(i)
    if i < 10 then
        i = '00'..tostring(i)
    elseif i < 100 then
        i = '0'..tostring(i)
    else
        i = tostring(i)
    end
    local t = create_tmp_tbl("tmp"..i)
    return db:create_thread(work, t, i)
end
local function main()
    db:setmaxinstructions(1000000)
    local total = 500
    for i = 1, total do
        create_tmp_tbl_and_thread(i)
    end
    local t = create_tmp_tbl("main")
    work(t, "1000")
end}$$
EOF

cdb2sql $SP_OPTIONS "exec procedure tmp_tbl_and_thread()" | sort
