#!/usr/bin/env bash

cdb2sql --host $SP_HOST $SP_OPTIONS - <<'EOF'
CREATE PROCEDURE test_fp VERSION '1' {
local function exec_fetch_and_emit(sql)
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
local function exec_and_nothing(sql)
  local rc
  local q
  q, rc = db:exec(sql)
  if (rc ~= 0) then
    db:emit(db:sqlerror())
  end
end
local function main()
  exec_fetch_and_emit("SELECT 1 AS xyz") -- 1 row
  exec_and_nothing("INSERT INTO fp1(x) VALUES(0)") -- 1 row
  exec_fetch_and_emit("SELECT x AS w FROM fp1 ORDER BY x") -- 2 rows
  exec_fetch_and_emit("SELECT x + x AS y FROM fp1 ORDER BY x") -- 2 rows
  exec_fetch_and_emit("SELECT SUM(x) AS z FROM fp1") -- 1 row
end}$$
EOF

cdb2sql --host $SP_HOST $SP_OPTIONS "EXEC PROCEDURE test_fp()"
