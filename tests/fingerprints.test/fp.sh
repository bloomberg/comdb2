#!/usr/bin/env bash

cdb2sql $SP_OPTIONS - <<'EOF'
CREATE PROCEDURE test_fp VERSION 1 {
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
local function prepare_sql_and_emit(sql)
  local rc
  local n, q, r
  q, rc = db:prepare(sql)
  if (rc == 0) then
    r, rc = db:exec(q)
    if (rc == 0) then
      n = r:fetch()
      while n do
        db:emit(n)
        n = r:fetch()
      end
    else
      db:emit(db:sqlerror())
    end
  else
    db:emit(db:sqlerror())
  end
end
local function main()
  exec_sql_and_emit("SELECT 1")
  exec_sql_and_emit("INSERT INTO fp1(x) VALUES(0)")
  exec_sql_and_emit("SELECT x FROM fp1 ORDER BY x")
  exec_sql_and_emit("DELETE FROM fp1 WHERE x = 0")
  exec_sql_and_emit("SELECT * FROM fp1 ORDER BY x")
  prepare_sql_and_emit("SELECT * FROM fp1 ORDER BY x")
end}$$
EOF

cdb2sql $SP_OPTIONS "EXEC PROCEDURE test_fp()"
