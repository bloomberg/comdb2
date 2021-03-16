#!/usr/bin/env bash
TIER="default"
cdb2sql="${CDB2SQL_EXE} -tabs -s ${CDB2_OPTIONS} ${DBNAME} ${TIER}"
${cdb2sql} <<'EOF'
create table t03(i int)$$
create procedure t03consumer version 'test' {
local function main()
    local c = db:consumer()
    c:get()
    c:consume()
end
}$$
create lua consumer t03consumer on (table t03 for insert)
insert into t03 values(0)
insert into t03 values(1)
EOF

count=$(${cdb2sql} "select count(*) from t03")
depth=$(${cdb2sql} "select depth from comdb2_queues where spname='t03consumer'")
[[ $count -ne 2 ]] && exit 1
[[ $depth -ne 2 ]] && exit 1

echo "inserted data -> count:${count}  depth:${depth}"

${cdb2sql} <<'EOF'
begin
delete from t03 where i = 0
exec procedure t03consumer()
commit

begin
delete from t03 where i = 1
exec procedure t03consumer()
commit
EOF

count=$(${cdb2sql} "select count(*) from t03")
depth=$(${cdb2sql} "select depth from comdb2_queues where spname='t03consumer'")
[[ $count -ne 0 ]] && exit 1
[[ $depth -ne 0 ]] && exit 1

echo "consumed data -> count:${count}  depth:${depth}"
exit 0
