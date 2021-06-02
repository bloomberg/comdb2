#!/usr/bin/env bash

[[ -n "$3" ]] && exec >$3 2>&1
cdb2sql $SP_OPTIONS - <<'EOF'
create procedure tmptbls version 'sptest' {
local function func(tbls)
    for i, tbl in ipairs(tbls) do
        tbl:insert({i=i})
    end
end
local function main()
    local thds = {}
    local tbl = db:table("tbl", {{"i", "int"}})
    for i = 1, 20 do
        local tbls = {}
        for j = 1, i do
            table.insert(tbls, tbl)
        end
        local thd = db:create_thread(func, tbls)
        table.insert(thds, thd)
    end
    for _, thd in ipairs(thds) do
        thd:join()
    end
    db:exec("select i, count(*) from tbl group by i"):emit()
end
}$$
put default procedure tmptbls 'sptest'
exec procedure tmptbls()
exec procedure tmptbls()
exec procedure tmptbls()
exec procedure tmptbls()
exec procedure tmptbls()
EOF
