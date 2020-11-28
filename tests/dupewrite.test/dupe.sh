#!/usr/bin/env bash

cdb2sql --host $SP_HOST $SP_OPTIONS - <<'EOF'
CREATE PROCEDURE test_dupe_insert_1 VERSION '1' {
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
end}$$
EOF

cdb2sql --host $SP_HOST $SP_OPTIONS - <<'EOF'
CREATE PROCEDURE test_dupe_insert_2 VERSION '1' {
local function exec_and_emit(sql)
  db:exec(sql):emit()
end
local function main()
  exec_and_emit("INSERT INTO dupe2(x) VALUES(0)") -- 1 row
end}$$
EOF

cdb2sql --host $SP_HOST $SP_OPTIONS - <<'EOF'
CREATE PROCEDURE test_dupe_insert_3 VERSION '1' {
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
local function main()
  exec_fetch_and_emit("INSERT INTO dupe3(x) VALUES(0)") -- 1 row
end}$$
EOF

cdb2sql --host $SP_HOST $SP_OPTIONS - <<'EOF'
CREATE PROCEDURE test_dupe_insert_4 VERSION '1' {
local function exec_and_exec(sql)
  local rc
  local q
  q, rc = db:exec(sql)
  if (rc ~= 0) then
    db:emit(db:sqlerror())
  end
  q, rc = db:exec(sql)
  if (rc ~= 0) then
    db:emit(db:sqlerror())
  end
end
local function main()
  exec_and_exec("INSERT INTO dupe4(x) VALUES(0)") -- 2 rows
end}$$
EOF

cdb2sql --host $SP_HOST $SP_OPTIONS - <<'EOF'
CREATE PROCEDURE test_dupe_update_1 VERSION '1' {
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
end}$$
EOF

cdb2sql --host $SP_HOST $SP_OPTIONS - <<'EOF'
CREATE PROCEDURE test_dupe_update_2 VERSION '1' {
local function exec_and_emit(sql)
  db:exec(sql):emit()
end
local function main()
  exec_and_emit("UPDATE dupe2 SET x = -1 WHERE x = 3") -- 1 row
end}$$
EOF

cdb2sql --host $SP_HOST $SP_OPTIONS - <<'EOF'
CREATE PROCEDURE test_dupe_update_3 VERSION '1' {
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
local function main()
  exec_fetch_and_emit("UPDATE dupe3 SET x = -1 WHERE x = 3") -- 1 row
end}$$
EOF

cdb2sql --host $SP_HOST $SP_OPTIONS - <<'EOF'
CREATE PROCEDURE test_dupe_update_4 VERSION '1' {
local function exec_and_exec(sql)
  local rc
  local q
  q, rc = db:exec(sql)
  if (rc ~= 0) then
    db:emit(db:sqlerror())
  end
  q, rc = db:exec(sql)
  if (rc ~= 0) then
    db:emit(db:sqlerror())
  end
end
local function main()
  exec_and_exec("UPDATE dupe4 SET x = -1 WHERE x = 3") -- 1 rows
end}$$
EOF

cdb2sql --host $SP_HOST $SP_OPTIONS - <<'EOF'
CREATE PROCEDURE test_dupe_delete_1 VERSION '1' {
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
end}$$
EOF

cdb2sql --host $SP_HOST $SP_OPTIONS - <<'EOF'
CREATE PROCEDURE test_dupe_delete_2 VERSION '1' {
local function exec_and_emit(sql)
  db:exec(sql):emit()
end
local function main()
  exec_and_emit("DELETE FROM dupe2 WHERE x = 2") -- 1 row
end}$$
EOF

cdb2sql --host $SP_HOST $SP_OPTIONS - <<'EOF'
CREATE PROCEDURE test_dupe_delete_3 VERSION '1' {
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
local function main()
  exec_fetch_and_emit("DELETE FROM dupe3 WHERE x = 2") -- 1 row
end}$$
EOF

cdb2sql --host $SP_HOST $SP_OPTIONS - <<'EOF'
CREATE PROCEDURE test_dupe_delete_4 VERSION '1' {
local function exec_and_exec(sql)
  local rc
  local q
  q, rc = db:exec(sql)
  if (rc ~= 0) then
    db:emit(db:sqlerror())
  end
  q, rc = db:exec(sql)
  if (rc ~= 0) then
    db:emit(db:sqlerror())
  end
end
local function main()
  exec_and_exec("DELETE FROM dupe4 WHERE x = 2") -- 1 row
end}$$
EOF

cdb2sql --host $SP_HOST $SP_OPTIONS "EXEC PROCEDURE test_dupe_insert_1()" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "EXEC PROCEDURE test_dupe_insert_2()" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "EXEC PROCEDURE test_dupe_insert_3()" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "EXEC PROCEDURE test_dupe_insert_4()" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "EXEC PROCEDURE test_dupe_update_1()" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "EXEC PROCEDURE test_dupe_update_2()" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "EXEC PROCEDURE test_dupe_update_3()" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "EXEC PROCEDURE test_dupe_update_4()" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "EXEC PROCEDURE test_dupe_delete_1()" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "EXEC PROCEDURE test_dupe_delete_2()" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "EXEC PROCEDURE test_dupe_delete_3()" 2>&1

cdb2sql --host $SP_HOST $SP_OPTIONS "EXEC PROCEDURE test_dupe_delete_4()" >delete4.out 2>&1
cat delete4.out | sed 's/genid =[0-9]\+/genid =XXXXXX/g'
