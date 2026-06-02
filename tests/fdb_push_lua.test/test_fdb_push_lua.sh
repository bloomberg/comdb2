#!/usr/bin/env bash

# Test fdb push write from Lua stored procedures
################################################################################

a_remdbname=$1
a_remcdb2config=$2
a_dbname=$3
a_cdb2config=$4
a_dbdir=$5
a_testdir=$6

output=run.out

REM_CDB2_OPTIONS="--cdb2cfg ${a_remcdb2config}"
SRC_CDB2_OPTIONS="--cdb2cfg ${a_cdb2config}"

# Make sure we talk to the same host
mach=$(cdb2sql ${SRC_CDB2_OPTIONS} --tabs $a_dbname default "SELECT comdb2_host()")

S="cdb2sql -s ${SRC_CDB2_OPTIONS} --host $mach $a_dbname"
Q="cdb2sql ${SRC_CDB2_OPTIONS} --host $mach $a_dbname"
R="cdb2sql ${REM_CDB2_OPTIONS} --tabs $a_remdbname default"

# populate table on remote
cdb2sql -s ${REM_CDB2_OPTIONS} $a_remdbname default - < remdata.req > $output 2>&1

# ============================================================
# Test 1: db:exec with fdb push write (simple insert)
# ============================================================
echo "Test 1: db:exec fdb push write insert" >> $output

$S - >> $output 2>&1 <<EOF
create procedure test_exec_insert version 'test1' {
local function main()
    db:exec("INSERT INTO LOCAL_${a_remdbname}.t VALUES (10, 'lua_exec')")
end
}\$\$
exec procedure test_exec_insert()
EOF

# Verify the insert worked
$R "SELECT * FROM t WHERE id=10" >> $output 2>&1

# ============================================================
# Test 2: db:prepare + stmt:bind (positional) + stmt:exec
# ============================================================
echo "Test 2: db:prepare + bind positional + exec" >> $output

$S - >> $output 2>&1 <<EOF
create procedure test_prepare_bind version 'test2' {
local function main()
    local stmt, rc = db:prepare("INSERT INTO LOCAL_${a_remdbname}.t VALUES (?, ?)")
    if rc ~= 0 then
        return db:error(rc)
    end
    stmt:bind(1, 20)
    stmt:bind(2, 'lua_bind')
    stmt:exec()
end
}\$\$
exec procedure test_prepare_bind()
EOF

# Verify
$R "SELECT * FROM t WHERE id=20" >> $output 2>&1

# ============================================================
# Test 3: db:prepare + stmt:bind (named @param) + stmt:exec
# ============================================================
echo "Test 3: db:prepare + bind named + exec" >> $output

$S - >> $output 2>&1 <<EOF
create procedure test_named_bind version 'test3' {
local function main()
    local stmt, rc = db:prepare("INSERT INTO LOCAL_${a_remdbname}.t VALUES (@id, @name)")
    if rc ~= 0 then
        return db:error(rc)
    end
    stmt:bind('id', 30)
    stmt:bind('name', 'lua_named')
    stmt:exec()
end
}\$\$
exec procedure test_named_bind()
EOF

# Verify
$R "SELECT * FROM t WHERE id=30" >> $output 2>&1

# ============================================================
# Test 4: rebind + re-exec (loop insert)
# ============================================================
echo "Test 4: rebind + re-exec loop" >> $output

$S - >> $output 2>&1 <<EOF
create procedure test_rebind_loop version 'test4' {
local function main()
    local stmt, rc = db:prepare("INSERT INTO LOCAL_${a_remdbname}.t VALUES (@id, @name)")
    if rc ~= 0 then
        return db:error(rc)
    end
    for i = 40, 42 do
        stmt:bind('id', i)
        stmt:bind('name', 'loop' .. i)
        stmt:exec()
    end
end
}\$\$
exec procedure test_rebind_loop()
EOF

# Verify
$R "SELECT * FROM t WHERE id >= 40 AND id <= 42 ORDER BY id" >> $output 2>&1

# ============================================================
# Test 5: bind various types (int, real, string, blob)
# ============================================================
echo "Test 5: bind various types" >> $output

$S - >> $output 2>&1 <<EOF
create procedure test_types version 'test5' {
local function main()
    local stmt, rc = db:prepare("INSERT INTO LOCAL_${a_remdbname}.t2(i, r, s) VALUES (@i, @r, @s)")
    if rc ~= 0 then
        return db:error(rc)
    end
    stmt:bind('i', 99)
    stmt:bind('r', 3.14)
    stmt:bind('s', 'hello')
    stmt:exec()
end
}\$\$
exec procedure test_types()
EOF

# Verify
$R "SELECT i, r, s FROM t2 WHERE i=99" >> $output 2>&1

# ============================================================
# Test 6: bind NULL value
# ============================================================
echo "Test 6: bind NULL" >> $output

$S - >> $output 2>&1 <<EOF
create procedure test_null version 'test6' {
local function main()
    local stmt, rc = db:prepare("INSERT INTO LOCAL_${a_remdbname}.t2(i, r, s) VALUES (@i, @r, @s)")
    if rc ~= 0 then
        return db:error(rc)
    end
    stmt:bind('i', 100)
    stmt:bind('r', nil)
    stmt:bind('s', nil)
    stmt:exec()
end
}\$\$
exec procedure test_null()
EOF

# Verify
$R "SELECT i, r, s FROM t2 WHERE i=100" >> $output 2>&1

# ============================================================
# Test 7: db:exec with UPDATE
# ============================================================
echo "Test 7: db:exec fdb push update" >> $output

$S - >> $output 2>&1 <<EOF
create procedure test_update version 'test7' {
local function main()
    db:exec("UPDATE LOCAL_${a_remdbname}.t SET b1='updated' WHERE id=10")
end
}\$\$
exec procedure test_update()
EOF

# Verify
$R "SELECT * FROM t WHERE id=10" >> $output 2>&1

# ============================================================
# Test 8: db:exec with DELETE
# ============================================================
echo "Test 8: db:exec fdb push delete" >> $output

$S - >> $output 2>&1 <<EOF
create procedure test_delete version 'test8' {
local function main()
    db:exec("DELETE FROM LOCAL_${a_remdbname}.t WHERE id=10")
end
}\$\$
exec procedure test_delete()
EOF

# Verify (should be empty)
$R "SELECT * FROM t WHERE id=10" >> $output 2>&1

# ============================================================
# Test 9: bind out-of-range position (expect error)
# ============================================================
echo "Test 9: bind out-of-range position" >> $output

$S - >> $output 2>&1 <<EOF
create procedure test_bind_oob version 'test9' {
local function main()
    local stmt, rc = db:prepare("INSERT INTO LOCAL_${a_remdbname}.t VALUES (?, ?)")
    if rc ~= 0 then
        return db:error(rc)
    end
    stmt:bind(5, 99)
    stmt:exec()
end
}\$\$
exec procedure test_bind_oob()
EOF

# ============================================================
# Test 10: exec without bind (no parameters)
# ============================================================
echo "Test 10: prepare + exec without bind (no params)" >> $output

$S - >> $output 2>&1 <<EOF
create procedure test_no_params version 'test10' {
local function main()
    local stmt, rc = db:prepare("INSERT INTO LOCAL_${a_remdbname}.t VALUES (60, 'no_bind')")
    if rc ~= 0 then
        return db:error(rc)
    end
    stmt:exec()
end
}\$\$
exec procedure test_no_params()
EOF

# Verify
$R "SELECT * FROM t WHERE id=60" >> $output 2>&1

# ============================================================
# Test 11: mix local and remote writes in same SP
# ============================================================
echo "Test 11: mix local and remote writes" >> $output

$S - >> $output 2>&1 <<EOF
create procedure test_mix_local_remote version 'test11' {
local function main()
    db:exec("INSERT INTO t VALUES (70, 'local')")
    db:exec("INSERT INTO LOCAL_${a_remdbname}.t VALUES (70, 'remote')")
    db:exec("INSERT INTO t VALUES (71, 'local2')")
end
}\$\$
exec procedure test_mix_local_remote()
EOF

# Verify local
cdb2sql ${SRC_CDB2_OPTIONS} --tabs --host $mach $a_dbname "SELECT * FROM t WHERE id IN (70, 71) ORDER BY id" >> $output 2>&1
# Verify remote
$R "SELECT * FROM t WHERE id=70" >> $output 2>&1

# ============================================================
# Test 12: stmt:close() then re-prepare (no leak)
# ============================================================
echo "Test 12: close and re-prepare" >> $output

$S - >> $output 2>&1 <<EOF
create procedure test_close_reopen version 'test12' {
local function main()
    local stmt, rc = db:prepare("INSERT INTO LOCAL_${a_remdbname}.t VALUES (?, ?)")
    if rc ~= 0 then
        return db:error(rc)
    end
    stmt:bind(1, 80)
    stmt:bind(2, 'close1')
    stmt:exec()
    stmt:close()

    stmt, rc = db:prepare("INSERT INTO LOCAL_${a_remdbname}.t VALUES (?, ?)")
    if rc ~= 0 then
        return db:error(rc)
    end
    stmt:bind(1, 81)
    stmt:bind(2, 'close2')
    stmt:exec()
end
}\$\$
exec procedure test_close_reopen()
EOF

# Verify
$R "SELECT * FROM t WHERE id IN (80, 81) ORDER BY id" >> $output 2>&1

# ============================================================
# Test 13: empty string and zero values
# ============================================================
echo "Test 13: empty string and zero values" >> $output

$S - >> $output 2>&1 <<EOF
create procedure test_edge_values version 'test13' {
local function main()
    local stmt, rc = db:prepare("INSERT INTO LOCAL_${a_remdbname}.t VALUES (@id, @name)")
    if rc ~= 0 then
        return db:error(rc)
    end
    stmt:bind('id', 0)
    stmt:bind('name', '')
    stmt:exec()
end
}\$\$
exec procedure test_edge_values()
EOF

# Verify
$R "SELECT * FROM t WHERE id=0" >> $output 2>&1

# ============================================================
# Test 14: multiple execs in a transaction
# ============================================================
echo "Test 14: push writes in transaction" >> $output

$S - >> $output 2>&1 <<EOF
create procedure test_in_txn version 'test14' {
local function main()
    db:begin()
    db:exec("INSERT INTO LOCAL_${a_remdbname}.t VALUES (90, 'txn1')")
    db:exec("INSERT INTO LOCAL_${a_remdbname}.t VALUES (91, 'txn2')")
    db:commit()
end
}\$\$
exec procedure test_in_txn()
EOF

# Verify
$R "SELECT * FROM t WHERE id IN (90, 91) ORDER BY id" >> $output 2>&1


# ============================================================
# Test 15: push write error (duplicate key on remote)
# ============================================================
echo "Test 15: push write error from remote" >> $output

# b1 is cstring[10] — value too long should fail on remote
$S - >> /dev/null 2>&1 <<EOF
create procedure test_push_error version 'test15' {
local function main()
    local stmt, rc = db:exec("INSERT INTO LOCAL_${a_remdbname}.t VALUES (900, 'this_string_is_way_too_long_for_b1')")
    if rc ~= 0 then
        return rc, "push write failed"
    end
end
}\$\$
EOF
out=$($Q "exec procedure test_push_error()" 2>&1)
echo "$out" | grep -q "push write failed"
if [[ $? -ne 0 ]] ; then
    echo "FAILED: expected error from push write, got: $out" >> $output
    exit 1
fi
echo "got expected error" >> $output

# ============================================================
# Test 16: push write after SP on same connection (sockpool regression)
# ============================================================
echo "Test 16: push write after SP on same connection" >> $output

timeout 10 cdb2sql -s ${SRC_CDB2_OPTIONS} --host $mach $a_dbname - <<EOF
exec procedure sys.cmd.send('fdb init')
insert into LOCAL_${a_remdbname}.t values (999, 'sptest')
EOF
rc=$?
if [[ $rc -ne 0 ]] ; then
    echo "Failed: push write after SP on same connection returned rc=$rc"
    exit 1
fi
$R "delete from t where id=999" >> $output 2>&1

# ============================================================
# Test 17: bind a typed decimal NULL read from a query column
# ============================================================
echo "Test 17: bind typed decimal NULL from query column" >> $output

# seed a decimal NULL on the source so the SP can read a typed null back
$Q "insert into t2(i, d) values (200, NULL)" >> $output 2>&1

$S - >> $output 2>&1 <<EOF
create procedure test_typed_null version 'test17' {
local function main()
    -- row.d is a typed decimal null (a dbtype userdata), not a plain lua nil
    local sel, rc = db:exec("SELECT d FROM t2 WHERE i=200")
    local row = sel:fetch()
    local stmt, rc = db:prepare("INSERT INTO LOCAL_${a_remdbname}.t2(i, r, s) VALUES (@i, @r, @s)")
    if rc ~= 0 then
        return db:error(rc)
    end
    stmt:bind('i', 200)
    stmt:bind('r', 2.5)
    stmt:bind('s', row.d)
    stmt:exec()
end
}\$\$
exec procedure test_typed_null()
EOF

# Verify: row 200 must exist on the remote with s NULL
$R "SELECT i, r, s FROM t2 WHERE i=200" >> $output 2>&1

# ============================================================
# Test 18: two concurrently-held prepared push statements
# ============================================================
echo "Test 18: two concurrently-prepared push statements" >> $output

$S - >> $output 2>&1 <<EOF
create procedure test_two_prepared version 'test18' {
local function main()
    local a, rca = db:prepare("INSERT INTO LOCAL_${a_remdbname}.t VALUES (?, ?)")
    if rca ~= 0 then return db:error(rca) end
    local b, rcb = db:prepare("INSERT INTO LOCAL_${a_remdbname}.t2(i) VALUES (?)")
    if rcb ~= 0 then return db:error(rcb) end
    -- interleave: bind/exec 'a' (2 params) after 'b' (1 param) is prepared
    a:bind(1, 500)
    a:bind(2, 'twoprep')
    a:exec()
    b:bind(1, 501)
    b:exec()
end
}\$\$
exec procedure test_two_prepared()
EOF

# Verify both landed correctly on the remote
$R "SELECT * FROM t WHERE id=500" >> $output 2>&1
$R "SELECT i FROM t2 WHERE i=501" >> $output 2>&1

# ============================================================
# Test 19: SP invoked with a bind arg, then push-prepares (param-clone leak path)
# ============================================================
echo "Test 19: SP invoked with bind arg + push prepare" >> $output

$S - >> $output 2>&1 <<EOF
create procedure test_arg_prepare version 'test19' {
local function main(x)
    local stmt, rc = db:prepare("INSERT INTO LOCAL_${a_remdbname}.t VALUES (?, ?)")
    if rc ~= 0 then return db:error(rc) end
    stmt:bind(1, 502)
    stmt:bind(2, 'argprep')
    stmt:exec()
end
}\$\$
@bind CDB2_INTEGER a 7
exec procedure test_arg_prepare(@a)
EOF

# Verify
$R "SELECT * FROM t WHERE id=502" >> $output 2>&1

# ============================================================
# Validate results
# ============================================================
echo "Testcase passed." >> $output

if [ -f expected_output ]; then
    testcase_output=$(cat $output)
    expected_output=$(cat expected_output)
    if [[ "$testcase_output" != "$expected_output" ]]; then
        echo "  ^^^^^^^^^^^^"
        echo "The above testcase has failed!!!"
        echo " "
        echo "Use 'diff <expected-output> <my-output>' to see why:"
        echo "> diff ${PWD}/{expected_output,$output}"
        echo " "
        diff expected_output $output
        echo " "
        exit 1
    fi
fi

echo "Testcase passed."
