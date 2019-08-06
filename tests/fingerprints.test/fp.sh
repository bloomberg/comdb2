#!/usr/bin/env bash

cdb2sql $SP_OPTIONS - <<'EOF'
create procedure test_fp version '1' {
local function main()
  local rc
  local n, q, r

  q, rc = db:exec("SELECT 1")
  if (rc == 0) then
    n = q:fetch()
    while n do
      db:emit(n)
      n = q:fetch()
    end
  else
    db:emit(db:sqlerror())
  end

  q, rc = db:exec("INSERT INTO fp1(x) VALUES(0)")
  if (rc == 0) then
    n = q:fetch()
    while n do
      db:emit(n)
      n = q:fetch()
    end
  else
    db:emit(db:sqlerror())
  end

  q, rc = db:exec("SELECT x FROM fp1 ORDER BY x")
  if (rc == 0) then
    n = q:fetch()
    while n do
      db:emit(n)
      n = q:fetch()
    end
  else
    db:emit(db:sqlerror())
  end

  q, rc = db:exec("DELETE FROM fp1 WHERE x = 0")
  if (rc == 0) then
    n = q:fetch()
    while n do
      db:emit(n)
      n = q:fetch()
    end
  else
    db:emit(db:sqlerror())
  end

  q, rc = db:exec("SELECT * FROM fp1 ORDER BY x")
  if (rc == 0) then
    n = q:fetch()
    while n do
      db:emit(n)
      n = q:fetch()
    end
  else
    db:emit(db:sqlerror())
  end

  q, rc = db:prepare("SELECT * FROM fp1 ORDER BY x")
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
end}$$
EOF

cdb2sql $SP_OPTIONS "EXEC PROCEDURE test_fp()"
