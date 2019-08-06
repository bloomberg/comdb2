#!/usr/bin/env bash

cdb2sql $SP_OPTIONS - <<'EOF'
create procedure test_fp version '1' {
local function main()
  db:exec("SELECT 1"):emit()
  db:exec("INSERT INTO fp1(x) VALUES(0)"):emit()
  db:exec("SELECT x FROM fp1"):emit()
  db:exec("DELETE FROM fp1 WHERE x = 0"):emit()

  local rc
  local n, q, r

  q, rc = db:exec("SELECT * FROM fp1 ORDER BY x")
  if (rc == 0) then
    n = q:fetch()
    db:emit(n)
  else
    db:emit(db:sqlerror())
  end   

  r, rc = db:prepare("SELECT * FROM fp1 ORDER BY x")
  if (rc == 0) then
    r:emit()
  else
    db:emit(db:sqlerror())
  end   
end}$$
EOF

cdb2sql $SP_OPTIONS "exec procedure test_fp()"
