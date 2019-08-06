#!/usr/bin/env bash

cdb2sql $SP_OPTIONS - <<'EOF'
CREATE PROCEDURE test_fp VERSION '1' {
local function exec_sql_and_emit(sql)
  local rc
  local n, q
  q, rc = db:exec(sql)
  if (rc == 0) then
    n = q:fetch()
    while n do
      db:emit(n)
      n = q:fetch()
    end
  else
    db:emit(db:sqlerror())
  end
end
local function main()
  exec_sql_and_emit("SELECT 1 AS xyz")
  exec_sql_and_emit("INSERT INTO fp1(x) VALUES(0)")
  exec_sql_and_emit("SELECT x + x AS y FROM fp1 ORDER BY x")
  exec_sql_and_emit("SELECT SUM(x) AS z FROM fp1")
end}$$
EOF

cdb2sql $SP_OPTIONS "EXEC PROCEDURE test_fp()"
