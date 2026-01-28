#!/bin/bash

source ${TESTSROOTDIR}/tools/runit_common.sh

DBNAME=$1
TIER="default"
cdb2sql="${CDB2SQL_EXE} ${DBNAME} ${TIER}" 

create_t1_with_check_constraints() {

$cdb2sql "drop table if exists t1"
$cdb2sql <<'EOF'
create table t1 
{
schema {
    int id
    int regular_col
    vutf8 metadata[500] null=yes
}
keys
{
    "PK" = id
}
constraints
{
    check "JSON_CHECK" = { where metadata IS NULL OR JSON_VALID(metadata) }
}
}
EOF

}

create_t1_without_constraints() {

    $cdb2sql "drop table if exists t1"
    $cdb2sql <<'EOF'
create table t1 
{
schema {
    int id
    int regular_col
    vutf8 metadata[500] null=yes        
}
}
EOF

}

insert_with_greater_than_500_chars() {
    # Generate a JSON string longer than 500 characters
    # This creates: {"key": "aaaa...aaa"} with enough 'a's to exceed 500 chars
    LONG_JSON='{"key": "'$(printf 'a%.0s' {1..600}0)'"}'
    echo "JSON length: ${#LONG_JSON}"

    echo "=== Test 1: Insert row with JSON longer than 500 chars ==="
    longquery="INSERT INTO t1 (id, regular_col, metadata) VALUES (1, 100, '$LONG_JSON')"
    echo "Executing query: $longquery"
    $cdb2sql "$longquery"
    echo "Insert result: $?"

    echo "=== Verify the insert ==="
    $cdb2sql "SELECT id, regular_col, length(metadata) as json_len FROM t1 WHERE id=1"
}

update_with_greater_than_500_chars() {
    # Generate a JSON string longer than 500 characters
    LONG_JSON='{"key": "'$(printf 'a%.0s' {1..600})'"}'
    echo "JSON length: ${#LONG_JSON}"

    echo "=== Test 2: Update row with JSON longer than 500 chars ==="
    updatequery="UPDATE t1 SET metadata = '$LONG_JSON' WHERE id=1"
    $cdb2sql "$updatequery"
    echo "Update result: $?"

    echo "=== Verify the update ==="
    $cdb2sql "SELECT id, regular_col, length(metadata) as json_len FROM t1 WHERE id=1"
    echo "select result: $?"
}

update_other_column() {

    updatequery="UPDATE t1 SET regular_col = 200 WHERE id=1"
    echo "=== Test 3: Update regular_col (BUG: fails if metadata > 500 chars) ==="
    $cdb2sql "$updatequery"
    echo "Update regular_col result: $?"
    $cdb2sql "SELECT id, regular_col, length(metadata) as json_len FROM t1 WHERE id=1"
    echo ""

}

reduce_and_update() {
    echo "=== Test 4: Reduce metadata to < 500, then update regular_col (should work) ==="
    SHORT_JSON='{"key": "short"}'
    $cdb2sql "UPDATE t1 SET metadata = '$SHORT_JSON' WHERE id=1"
    echo "Reduced metadata result: $?"

    $cdb2sql "UPDATE t1 SET regular_col = 300 WHERE id=1"
    echo "Update regular_col after reducing metadata result: $?"
    echo ""
}

create_t1_with_check_constraints
if [ $? == 0 ]; then
    insert_with_greater_than_500_chars
    update_with_greater_than_500_chars
    update_other_column
    reduce_and_update
else
    echo "Failed to create table with check constraints"
    exit 1
fi


create_t1_without_constraints
if [ $? == 0 ]; then
    insert_with_greater_than_500_chars
    update_with_greater_than_500_chars
    update_other_column
    reduce_and_update
else
    echo "Failed to create table without check constraints"
    exit 1
fi

echo "=== Final state ==="
$cdb2sql "SELECT id, regular_col, length(metadata) as json_len, metadata FROM t1 WHERE id=1"
