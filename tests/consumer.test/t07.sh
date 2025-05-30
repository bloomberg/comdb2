#!/bin/bash

cdb2sql="${CDB2SQL_EXE} -s ${CDB2_OPTIONS} ${DBNAME} default"

(
$cdb2sql -<<'EOF'
drop table if exists t;
create table t(a double);$$
create procedure test version '1' {
local function main()
    db:num_columns(2)
    db:column_name('old', 1)
    db:column_type('cstring', 1)
    db:column_name('new', 2)
    db:column_type('cstring', 2)
    local consumer = db:consumer()
    while true do
        local change = consumer:poll(5000)
        if change == nil then
            return 0
        end
        local row = {}
        if change.old ~= nil then
            row.old = db:table_to_json(change.old)
        end
        if change.new ~= nil then
            row.new = db:table_to_json(change.new)
        end
        consumer:emit(row)
        consumer:consume()
    end
end
}
EOF
$cdb2sql -<<'EOF'
create lua consumer test on (table t for insert and update and delete);
insert into t values(1);
insert into t values('nan');
insert into t values('-inf');
insert into t values('inf');
update t set a=a
delete from t
exec procedure test()
EOF
$cdb2sql -<<'EOF'
create procedure test version '1' {
local function main()
    db:num_columns(2)
    db:column_name('old', 1)
    db:column_type('cstring', 1)
    db:column_name('new', 2)
    db:column_type('cstring', 2)
    local consumer = db:consumer()
    while true do
        local change = consumer:poll(5000)
        if change == nil then
            return 0
        end
        local row = {}
        if change.old ~= nil then
            row.old = db:table_to_json(change.old, {quote_special_fp_values=true})
        end
        if change.new ~= nil then
            row.new = db:table_to_json(change.new, {quote_special_fp_values=true})
        end
        consumer:emit(row)
        consumer:consume()
    end
end
}
EOF
$cdb2sql -<<'EOF'
insert into t values(1);
insert into t values('nan');
insert into t values('-inf');
insert into t values('inf');
update t set a=a
delete from t
exec procedure test()
EOF
) > t07.output 2>&1
set -x
diff -q t07.output t07.expected
if [[ $? -ne 0 ]]; then
    diff t07.output t07.expected | head -10
    exit 1
fi
exit 0
