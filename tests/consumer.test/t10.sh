#!/bin/bash

# Test: consumer:next() followed by DML in an explicit transaction.
# Reproduces bug where consumer:next() did not initialize shadow_tran,
# causing "wrong sql transaction" errors on subsequent DML.

cdb2sql="${CDB2SQL_EXE} -tabs -s ${CDB2_OPTIONS} ${DBNAME} default"

(
# ---- Test 1: consumer:next() + UPDATE ----

$cdb2sql "DROP TABLE IF EXISTS bug_src"
$cdb2sql "DROP TABLE IF EXISTS bug_tbl"

$cdb2sql <<'EOF' >/dev/null 2>&1
CREATE TABLE bug_src(tktnum int)$$
CREATE TABLE bug_tbl(tktnum int, status int)$$
CREATE PROCEDURE next_upd_test VERSION 'bar' {
local function main()
    local consumer = db:consumer()
    local event = consumer:get()
    db:begin()
    consumer:next()
    local stmt = db:prepare("UPDATE bug_tbl SET status=1 WHERE tktnum=@tktnum AND status=0")
    stmt:bind("tktnum", event.new.tktnum)
    local rc = stmt:exec()
    if rc ~= 0 then
        db:rollback()
        return -201, db:error()
    else
        db:commit()
    end
end
}$$
CREATE LUA CONSUMER next_upd_test ON (TABLE bug_src FOR INSERT);
EOF

$cdb2sql "INSERT INTO bug_tbl VALUES(13, 0), (14, 0)" >/dev/null 2>&1
$cdb2sql "INSERT INTO bug_src VALUES(13)" >/dev/null 2>&1

sleep 2

$cdb2sql "EXEC PROCEDURE next_upd_test()" >/dev/null 2>&1

echo "test1 updated rows:"
$cdb2sql "SELECT tktnum, status FROM bug_tbl ORDER BY tktnum"

echo "test1 queue depth:"
$cdb2sql "SELECT depth FROM comdb2_queues WHERE spname='next_upd_test'"

$cdb2sql "DROP LUA CONSUMER next_upd_test" >/dev/null 2>&1
$cdb2sql "DROP TABLE bug_src" >/dev/null 2>&1
$cdb2sql "DROP TABLE bug_tbl" >/dev/null 2>&1

# ---- Test 2: consumer:next() + INSERT ----

$cdb2sql <<'EOF' >/dev/null 2>&1
CREATE TABLE ins_src(i int)$$
CREATE TABLE ins_dst(i int)$$
CREATE PROCEDURE next_ins_test VERSION 'bar' {
local function main()
    local consumer = db:consumer()
    local event = consumer:get()
    db:begin()
    consumer:next()
    local stmt = db:prepare("INSERT INTO ins_dst VALUES(@i)")
    stmt:bind("i", event.new.i)
    local rc = stmt:exec()
    if rc ~= 0 then
        db:rollback()
        return -201, db:error()
    else
        db:commit()
    end
end
}$$
CREATE LUA CONSUMER next_ins_test ON (TABLE ins_src FOR INSERT);
EOF

$cdb2sql "INSERT INTO ins_src VALUES(99)" >/dev/null 2>&1

sleep 2

$cdb2sql "EXEC PROCEDURE next_ins_test()" >/dev/null 2>&1

echo "test2 inserted rows:"
$cdb2sql "SELECT i FROM ins_dst ORDER BY i"

echo "test2 queue depth:"
$cdb2sql "SELECT depth FROM comdb2_queues WHERE spname='next_ins_test'"

$cdb2sql "DROP LUA CONSUMER next_ins_test" >/dev/null 2>&1
$cdb2sql "DROP TABLE ins_src" >/dev/null 2>&1
$cdb2sql "DROP TABLE ins_dst" >/dev/null 2>&1

# ---- Test 3: consumer:next() + DELETE ----

$cdb2sql <<'EOF' >/dev/null 2>&1
CREATE TABLE del_src(i int)$$
CREATE TABLE del_tbl(i int)$$
CREATE PROCEDURE next_del_test VERSION 'bar' {
local function main()
    local consumer = db:consumer()
    local event = consumer:get()
    db:begin()
    consumer:next()
    local stmt = db:prepare("DELETE FROM del_tbl WHERE i=@i")
    stmt:bind("i", event.new.i)
    local rc = stmt:exec()
    if rc ~= 0 then
        db:rollback()
        return -201, db:error()
    else
        db:commit()
    end
end
}$$
CREATE LUA CONSUMER next_del_test ON (TABLE del_src FOR INSERT);
EOF

$cdb2sql "INSERT INTO del_tbl VALUES(1), (2), (3)" >/dev/null 2>&1
$cdb2sql "INSERT INTO del_src VALUES(2)" >/dev/null 2>&1

sleep 2

$cdb2sql "EXEC PROCEDURE next_del_test()" >/dev/null 2>&1

echo "test3 remaining rows:"
$cdb2sql "SELECT i FROM del_tbl ORDER BY i"

echo "test3 queue depth:"
$cdb2sql "SELECT depth FROM comdb2_queues WHERE spname='next_del_test'"

$cdb2sql "DROP LUA CONSUMER next_del_test" >/dev/null 2>&1
$cdb2sql "DROP TABLE del_src" >/dev/null 2>&1
$cdb2sql "DROP TABLE del_tbl" >/dev/null 2>&1

# ---- Test 4: consumer:next() + UPDATE in a loop (multiple events) ----

$cdb2sql <<'EOF' >/dev/null 2>&1
CREATE TABLE loop_src(tktnum int)$$
CREATE TABLE loop_tbl(tktnum int, status int)$$
CREATE PROCEDURE next_loop_test VERSION 'bar' {
local function main()
    local consumer = db:consumer()
    while true do
        local event = consumer:get()
        db:begin()
        consumer:next()
        local stmt = db:prepare("UPDATE loop_tbl SET status=1 WHERE tktnum=@tktnum AND status=0")
        stmt:bind("tktnum", event.new.tktnum)
        local rc = stmt:exec()
        if rc ~= 0 then
            db:rollback()
            return -201, db:error()
        else
            db:commit()
        end
    end
end
}$$
CREATE LUA CONSUMER next_loop_test ON (TABLE loop_src FOR INSERT);
EOF

$cdb2sql "INSERT INTO loop_tbl VALUES(1,0),(2,0),(3,0),(4,0),(5,0)" >/dev/null 2>&1
$cdb2sql "INSERT INTO loop_src VALUES(1),(3),(5)" >/dev/null 2>&1

sleep 2

$cdb2sql "EXEC PROCEDURE next_loop_test()" >/dev/null 2>&1 &
CONSUMER_PID=$!

sleep 4

kill $CONSUMER_PID 2>/dev/null
wait $CONSUMER_PID 2>/dev/null

echo "test4 updated rows:"
$cdb2sql "SELECT tktnum, status FROM loop_tbl ORDER BY tktnum"

echo "test4 queue depth:"
$cdb2sql "SELECT depth FROM comdb2_queues WHERE spname='next_loop_test'"

$cdb2sql "DROP LUA CONSUMER next_loop_test" >/dev/null 2>&1
$cdb2sql "DROP TABLE loop_src" >/dev/null 2>&1
$cdb2sql "DROP TABLE loop_tbl" >/dev/null 2>&1

# ---- Test 5: consumer:next() + INSERT in a loop (multiple events) ----

$cdb2sql <<'EOF' >/dev/null 2>&1
CREATE TABLE loop_ins_src(i int)$$
CREATE TABLE loop_ins_dst(i int)$$
CREATE PROCEDURE next_loop_ins VERSION 'bar' {
local function main()
    local consumer = db:consumer()
    while true do
        local event = consumer:get()
        db:begin()
        consumer:next()
        local stmt = db:prepare("INSERT INTO loop_ins_dst VALUES(@i)")
        stmt:bind("i", event.new.i)
        local rc = stmt:exec()
        if rc ~= 0 then
            db:rollback()
            return -201, db:error()
        else
            db:commit()
        end
    end
end
}$$
CREATE LUA CONSUMER next_loop_ins ON (TABLE loop_ins_src FOR INSERT);
EOF

$cdb2sql "INSERT INTO loop_ins_src VALUES(10),(20),(30)" >/dev/null 2>&1

sleep 2

$cdb2sql "EXEC PROCEDURE next_loop_ins()" >/dev/null 2>&1 &
CONSUMER_PID=$!

sleep 4

kill $CONSUMER_PID 2>/dev/null
wait $CONSUMER_PID 2>/dev/null

echo "test5 inserted rows:"
$cdb2sql "SELECT i FROM loop_ins_dst ORDER BY i"

echo "test5 queue depth:"
$cdb2sql "SELECT depth FROM comdb2_queues WHERE spname='next_loop_ins'"

$cdb2sql "DROP LUA CONSUMER next_loop_ins" >/dev/null 2>&1
$cdb2sql "DROP TABLE loop_ins_src" >/dev/null 2>&1
$cdb2sql "DROP TABLE loop_ins_dst" >/dev/null 2>&1

# ---- Test 6: consumer:next() + DELETE in a loop (multiple events) ----

$cdb2sql <<'EOF' >/dev/null 2>&1
CREATE TABLE loop_del_src(i int)$$
CREATE TABLE loop_del_tbl(i int)$$
CREATE PROCEDURE next_loop_del VERSION 'bar' {
local function main()
    local consumer = db:consumer()
    while true do
        local event = consumer:get()
        db:begin()
        consumer:next()
        local stmt = db:prepare("DELETE FROM loop_del_tbl WHERE i=@i")
        stmt:bind("i", event.new.i)
        local rc = stmt:exec()
        if rc ~= 0 then
            db:rollback()
            return -201, db:error()
        else
            db:commit()
        end
    end
end
}$$
CREATE LUA CONSUMER next_loop_del ON (TABLE loop_del_src FOR INSERT);
EOF

$cdb2sql "INSERT INTO loop_del_tbl VALUES(1),(2),(3),(4),(5)" >/dev/null 2>&1
$cdb2sql "INSERT INTO loop_del_src VALUES(2),(4)" >/dev/null 2>&1

sleep 2

$cdb2sql "EXEC PROCEDURE next_loop_del()" >/dev/null 2>&1 &
CONSUMER_PID=$!

sleep 4

kill $CONSUMER_PID 2>/dev/null
wait $CONSUMER_PID 2>/dev/null

echo "test6 remaining rows:"
$cdb2sql "SELECT i FROM loop_del_tbl ORDER BY i"

echo "test6 queue depth:"
$cdb2sql "SELECT depth FROM comdb2_queues WHERE spname='next_loop_del'"

$cdb2sql "DROP LUA CONSUMER next_loop_del" >/dev/null 2>&1
$cdb2sql "DROP TABLE loop_del_src" >/dev/null 2>&1
$cdb2sql "DROP TABLE loop_del_tbl" >/dev/null 2>&1

) > t10.output 2>&1

set -x
diff -q t10.output t10.expected
if [[ $? -ne 0 ]]; then
    diff t10.output t10.expected | head -30
    exit 1
fi
echo "passed t10"
exit 0
