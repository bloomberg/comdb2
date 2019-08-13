#!/usr/bin/env bash

cdb2sql --host $SP_HOST $SP_OPTIONS - <<'EOF'
CREATE PROCEDURE test_dupe_insert VERSION '1' {
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
local function exec_and_emit(sql)
  db:exec(sql):emit()
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
  exec_and_nothing("INSERT INTO dupe1(x) VALUES(0)") -- 1 row
  -- exec_and_nothing("INSERT INTO dupe2(x) VALUES(0)") -- 1 row
  exec_and_emit("INSERT INTO dupe2(x) VALUES(0)") -- 1 row
  -- exec_and_nothing("INSERT INTO dupe3(x) VALUES(0)") -- 1 row
  exec_fetch_and_emit("INSERT INTO dupe3(x) VALUES(0)") -- 1 row
end}$$
EOF

cdb2sql --host $SP_HOST $SP_OPTIONS - <<'EOF'
CREATE PROCEDURE test_dupe_update VERSION '1' {
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
local function exec_and_emit(sql)
  db:exec(sql):emit()
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
  exec_and_nothing("UPDATE dupe1 SET x = -1 WHERE x = 3") -- 1 row
  -- exec_and_nothing("UPDATE dupe2 SET x = -1 WHERE x = 3") -- 1 row
  exec_and_emit("UPDATE dupe2 SET x = -1 WHERE x = 3") -- 1 row
  -- exec_and_nothing("UPDATE dupe3 SET x = -1 WHERE x = 3") -- 1 row
  exec_fetch_and_emit("UPDATE dupe3 SET x = -1 WHERE x = 3") -- 1 row
end}$$
EOF

cdb2sql --host $SP_HOST $SP_OPTIONS - <<'EOF'
CREATE PROCEDURE test_dupe_delete VERSION '1' {
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
local function exec_and_emit(sql)
  db:exec(sql):emit()
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
  exec_and_nothing("DELETE FROM dupe1 WHERE x = 2") -- 1 row
  -- exec_and_nothing("DELETE FROM dupe2 WHERE x = 2") -- 1 row
  exec_and_emit("DELETE FROM dupe2 WHERE x = 2") -- 1 row
  -- exec_and_nothing("DELETE FROM dupe3 WHERE x = 2") -- 1 row
  exec_fetch_and_emit("DELETE FROM dupe3 WHERE x = 2") -- 1 row
end}$$
EOF

cdb2sql --host $SP_HOST $SP_OPTIONS "EXEC PROCEDURE test_dupe_insert()"
cdb2sql --host $SP_HOST $SP_OPTIONS "EXEC PROCEDURE test_dupe_update()"
cdb2sql --host $SP_HOST $SP_OPTIONS "EXEC PROCEDURE test_dupe_delete()"
