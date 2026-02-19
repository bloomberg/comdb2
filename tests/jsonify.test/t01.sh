#!/bin/bash

source ${TESTSROOTDIR}/tools/runit_common.sh

DBNAME=$1
TIER="default"
cdb2sql="${CDB2SQL_EXE} ${CDB2_OPTIONS} ${DBNAME} ${TIER}" 
create_t1_with_check_constraints() {

$cdb2sql "drop table if exists t1"
$cdb2sql <<'EOF'
create table t1 
{
schema {
    int id
    int regular_col
    vutf8 metadata[10] null=yes
    vutf8 extra_col[2] dbstore="temp" null=yes
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

insert_with_greater_than_fld_len() {
    # Generate a JSON string longer than 10 characters
    # This creates: {"key": "aaaa...aaa"} with enough 'a's to exceed 10 chars
    LONG_JSON='{"key": "'$(printf 'a%.0s' {1..10})'"}'
    echo "JSON length: ${#LONG_JSON}"
    longquery="INSERT INTO t1 (id, regular_col, metadata) VALUES (1, 100, '$LONG_JSON')"
    $cdb2sql "$longquery"
    [ $? == 0 ] || return 1

    longquery="INSERT INTO t1 (id, regular_col, extra_col) VALUES (2, 150, '$LONG_JSON')"
    $cdb2sql "$longquery"
    [ $? == 0 ] || return 1
    $cdb2sql "SELECT id, regular_col, metadata, extra_col FROM t1"
    return $?
}

update_with_greater_than_fld_len() {
    # Generate a JSON string longer than 500 characters
    LONG_JSON='{"key": "'$(printf 'a%.0s' {1..10})'"}'
    echo "JSON length: ${#LONG_JSON}"
    updatequery="UPDATE t1 SET metadata = '$LONG_JSON' WHERE id=1"
    $cdb2sql "$updatequery"
    [ $? == 0 ] || return 1
    $cdb2sql "SELECT id, regular_col, metadata, extra_col as json_len FROM t1 WHERE id=1"
    return $?
}

update_other_column() {
    updatequery="UPDATE t1 SET regular_col = 200 WHERE id=1"
    #=== Test 3: Update regular_col (BUG: fails if metadata > 500 chars) ===
    $cdb2sql "$updatequery"
    [ $? == 0 ] || return 1
    $cdb2sql "SELECT id, regular_col, metadata, extra_col as json_len FROM t1 WHERE id=1"
    updatequery="UPDATE t1 set extra_col = 'test' where id=1"
    $cdb2sql "$updatequery"
    [ $? == 0 ] || return 1
    $cdb2sql "SELECT id, extra_col, metadata, extra_col as json_len FROM t1 WHERE id=1"
    return $?

}

reduce_and_update() {
    SHORT_JSON='{"k":"a"}'
    $cdb2sql "UPDATE t1 SET metadata = '$SHORT_JSON' WHERE id=1"
    [ $? == 0 ] || return 1
    $cdb2sql "UPDATE t1 SET regular_col = 300 WHERE id=1"
    [ $? == 0 ] || return 1
}

check_if_add_record_loads_dbstore_values() {
    $cdb2sql "drop table if exists t1"
    $cdb2sql <<EOF
create table t1 { 
    schema { 
        int id 
        vutf8 metadata[10] dbstore="verylongrandomstr" 
    } 
    constraints { 
        check "length_check" = { where length(metadata) > 0} 
    }
}
EOF
    [ $? == 0 ] || return 1
    $cdb2sql "insert into t1 (id, metadata) values (1, '{"key":"value"}')"
    [ $? == 0 ]|| return 1
    $cdb2sql "select id, length(metadata) as json_len, metadata from t1 where id=1"
    [ $? == 0 ] || return 1
    $cdb2sql "insert into t1 (id) values (2)"
    [ $? == 0 ] || return 1
    $cdb2sql "select id, length(metadata) as json_len, metadata from t1 where id=2"
    return $?
}

create_t1_with_check_constraints
if [ $? == 0 ]; then
    insert_with_greater_than_fld_len
    update_with_greater_than_fld_len
    update_other_column
    reduce_and_update
else
    echo "Failed to create table with check constraints"
    exit 1
fi

check_if_add_record_loads_dbstore_values
if [ $? != 0 ]; then
    echo "Failed to add record and load dbstore values"
    exit 1
fi

echo "t01 passed!"
