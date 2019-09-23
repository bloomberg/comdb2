#!/usr/bin/env bash

[[ -n "$3" ]] && exec >$3 2>&1
cdb2sql $SP_OPTIONS - <<'EOF'
create procedure try_to_use_tran_statements version '1' {
local function main()
    local tran_stmts = {
        "begin",
        "commit",
        "rollback",
        "savepoint one",
        "release one",
        "savepoint one",
        "release savepoint one"
    }
    local stmt0, stmt1
    local rc0, rc1
    for _, tran_stmt in ipairs(tran_stmts) do
        stmt0, rc0 = db:exec(tran_stmt)
        stmt1, rc1 = db:prepare(tran_stmt)
        db:emit(rc0, rc1, tran_stmt)
    end
end}$$
EOF

cdb2sql $SP_OPTIONS "exec procedure try_to_use_tran_statements()"
