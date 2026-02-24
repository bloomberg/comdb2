#!/bin/bash

source ${TESTSROOTDIR}/tools/runit_common.sh

DBNAME=$1
TIER="default"
cdb2sql="${CDB2SQL_EXE} -s --tabs ${CDB2_OPTIONS} ${DBNAME} ${TIER}" 
set -e 

create_tables() {
    $cdb2sql "drop table if exists t1"
    $cdb2sql "drop table if exists p"
$cdb2sql <<'EOF'
create table p {
schema {
    int i
    vutf8 txt[10]
}
keys {
    "key_i" = i
}
}
EOF
$cdb2sql <<'EOF'
create table t1 {
schema {
    int i
    vutf8 json[10] dbstore="{}"
}
keys {
    "key_i" = i
    //dup "prefix_i" = json_extract(json, "$.key") 
}
constraints {
    "key_i" -> <"p":"key_i">
}
}
EOF
}

insert_data() {
    nrows=$($cdb2sql "select 1000+abs(random())%1000" )
    echo "inserting $nrows rows: $($cdb2sql "select 1000+abs(random())%1000" )"
    $cdb2sql "INSERT INTO p SELECT value, 'texttexttexttext' || value FROM generate_series(1, $nrows);"
    $cdb2sql "INSERT INTO t1 SELECT value, json_object('key', 'texttexttexttext' || value) FROM generate_series(1, $nrows);"
    $cdb2sql "exec procedure sys.cmd.send('flush')"
}

schema_change() {
    $cdb2sql <<'EOF'
alter table t1 {
schema {
    int i
    vutf8 json[10] dbstore="{}"
}
keys {
    "key_i" = i
    dup "prefix_i" = (cstring[25])"json_extract(json, '$.key')" { where i=1 }
    // dup "prefix_i" = (cstring[25])"json_object('key', 't' || i)"
}
constraints {
    "key_i" -> <"p":"key_i">
}
}
EOF
}

cleanup() {
    $cdb2sql "drop table if exists t1"
    $cdb2sql "drop table if exists p"
    $cdb2sql 'exec procedure sys.cmd.send("flush")'
}
create_tables
insert_data
$cdb2sql "exec procedure sys.cmd.send('scconvert_finish_delay 1')"
$cdb2sql "exec procedure sys.cmd.send('debg 300')"
schema_change &> sc.out &
pid=$!
rowid=$($cdb2sql "select rowid from t1 order by rowid limit 1")
echo "before update, rowid is $rowid"
sleep 2
$cdb2sql "update t1 set i = i where rowid=(select rowid from t1 order by rowid limit 1)"
echo "rowid is $($cdb2sql "select rowid from t1 order by rowid limit 1")"
if wait $pid; then
    cleanup
    echo "Schema change completed successfully"
    exit 0
else
    echo "Schema change failed"
    exit 1
fi

exit 0
