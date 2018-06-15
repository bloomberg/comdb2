#!/usr/bin/env bash

[[ -n "$3" ]] && exec >$3 2>&1
cdb2sql $SP_OPTIONS - <<'EOF'
create table t {
schema
{
    int i
    int j
}}$$
create procedure test version 'sptest' {
local function main(test_no, t)
  if(test_no == '1') then 
    db:exec(t)
    db:column_type("string", 1);
    db:column_type("string", 2);
    db:emit("124", "432")
    return 0
  end 
  if(test_no == '2') then 
    db:num_columns(2)
    db:column_type("integer", 1);
    db:column_name("jon", 1);
    db:column_type("integer", 2);
    db:column_name("doe", 2);
    db:emit("124", "432")
    return 0
  end 
  if(test_no == '3') then
    db:print("abcdefghij\n")
    return 0
  end
  if(test_no == '4') then
    db:emit("abcd")
    return -200, "abcdef"
  end
  if(test_no == '5') then
    db:emit("col1","col2")
    return 0
  end
  if(test_no == '6') then
    local resultset, rc = db:exec(t)
    db:emit(resultset)
    return 0
  end
end}$$
put default procedure test 'sptest'
exec procedure test('1', 'select 1, 2')
exec procedure test('1', 'select 1 as lua, 2 as test')

create table LongColumnNamesTable {
schema
{
    int ColumnNameWithMoreThanThirtyTwoCharacters
    int AnotherColumnNameWithMoreThanThirtyTwoCharacters
}}$$

insert into LongColumnNamesTable values(1,2)

exec procedure test('6', 'select * from LongColumnNamesTable')

create procedure test1 version 'sptest1' {
local function main(t)
 local qr, rc = db:exec(t)
 local nqr = qr:fetch()
 db:emit(nqr) --.i,nqr.j)
 local nqr1 = qr:fetch()
 db:emit(nqr1.i,nqr1.j)
 nqr = qr:fetch()
 db:emit(nqr.i,nqr.j)
 -- Make a dummy row.
 db:emit('213','323')
 -- Send a row will null columns.
 db:emit(nil,nil)
end}$$
put default procedure test1 'sptest1'
insert into t values(23,23)
insert into t values(32,32)
insert into t values(321,321)
exec procedure test1('select i, j from t order by i')


create procedure test3 version 'sptest3' {
local function main(test_no, t)
  if(test_no == '1') then
   local qr, rc = db:exec(t)
   local nqr = qr:fetch()
   while nqr do
     db:emit(nqr) --.i, nqr.j)
     nqr = qr:fetch()
   end
  end
  if(test_no == '2') then
   local qr, rc = db:exec(t)
   db:num_columns(2)
   db:column_name("column1", 1)
   db:column_type("real", 1)
   db:column_name("column2", 2)
   db:column_type("real", 2)
   local nqr = qr:fetch()
   while nqr do
     db:emit(nqr.column1, nqr.column2)
     nqr = qr:fetch()
   end
  end
end}$$
put default procedure test3 'sptest3'
exec procedure test3('1', 'select i, j from t order by i')


create procedure test4 version 'sptest4' {
local function main(test_no, t)
 if(test_no == '1') then
   local tab, rc = db:table(t, {{"id", "text"}, {"b", "text"}})
   if (rc == 0) then
       tab:insert({id="5",b="12"}) 
       tab:insert({id="25",b="12"}) 
       local resultset, rc = db:exec('select * from '..tab:name())
       db:emit(resultset)
    else
       db:emit(db:sqlerror())
    end   
 end
 if(test_no == '2') then
   local tab = db:table(t) 
   tab:emit()
   return 0
 end
 if(test_no == '3') then
   local tab, rc = db:table('newt', {{"id", "integer"}, {"b", "integer"}})
   if (rc == 0) then
       tab:insert({id="5",b="12"})
       tab:insert({id="25",b="12"})
       local resultset, rc = db:exec('select * from t, ' ..tab:name().. ' where id = i')
       db:emit(resultset)
  end
  return 0
 end
 if(test_no == '4') then   --return error
  db:emit(-1)
  return -200, "Got an error"
 end
 if(test_no == '5') then
   local tab = db:table(t) 
   return 0, tab
 end
end}$$
put default procedure test4 'sptest4'
exec procedure test4('1', '1234')
exec procedure test4('1', 'abc')


create procedure test5 version 'sptest5' {
local function main(t)
 local tab = db:table('t') 
 db:begin()
 tab:insert({i="5",j="12"}) 
 tab:insert({i="25",j="12"}) 
 if(t == 'dummy') then
   db:rollback()
   db:num_columns(1)
   db:column_name("rows inserted", 1)
   db:emit(-1);
 else  
   db:commit()
   db:num_columns(1)
   db:column_name("rows inserted", 1)
   db:emit(5);
 end  
end}$$
put default procedure test5 'sptest5'
exec procedure test5('dum')
select * from t order by i
exec procedure test5('dummy')
select * from t order by i


create procedure test6 version 'sptest6' {
local function main()
 local tab = db:table('tmp',{{"id", 'integer'}, {"j", 'integer'}})
 tab:insert({id="5",j="12"}) 
 tab:insert({id="25",j="12"}) 
 local result_set, rc = db:exec("select * from tmp, t where tmp.id = t.i")
 db:emit(result_set)
 return rc
end}$$
put default procedure test6 'sptest6'
exec procedure test6()


create procedure test7 version 'sptest7' {
local function main(test_number, arg)
  if(test_number == '1') then
    local result, rc = db:prepare("select * from t where i > @i order by i")
    result:bind(1,arg)
    result:emit()
    return rc
  end
  db:emit(-1)
end}$$
put default procedure test7 'sptest7'
exec procedure test7('1', '5')
exec procedure test('2')
exec procedure test('4')
exec procedure test('5')
exec procedure test('6', 'select * from t order by i')
exec procedure test4('2', 't')
exec procedure test4('3')
exec procedure test4('4')
exec procedure test4('4')


create table t1 {
schema
{
    decimal128  de
    double      db
    datetime    dt
}}$$
insert into t1 values ('123456789.123456789','123456789.123456789',1234)


create procedure test8 version 'sptest8' {
local function main(test_no, t)
 if(test_no == '1') then
      local result_set, rc = db:exec("select * from t1")
      local result = result_set:fetch()
      db:num_columns(2)
      db:column_name("decimal",1)
      db:column_name("double",2)
      db:emit(result.de, result.db)
      return 0
 end
 if(test_no == '2') then
      local result_set, rc = db:exec("select * from t1")
      local result = result_set:fetch()
      db:num_columns(3)
      db:column_name("year",1)
      db:column_type("integer",1)
      db:column_name("month",2)
      db:column_type("integer",2)
      db:column_name("month day",3)
      db:column_type("integer",3)
      db:emit(result.dt.year, result.dt.month, result.dt.day)
      return 0
 end
 db:emit(-1)
end}$$
put default procedure test8 'sptest8'
exec procedure test8('1', '')
exec procedure test8('2')


create procedure test9 version 'sptest9' {
local function main(test_no)
 local tab = db:table('tmp',{{"id", 'integer'},{"j", 'integer'}})
 tab:insert({id="5",j="12"}) 
 tab:insert({id="25",j="12"}) 
 tab:insert({id="35",j="12"}) 
 if(test_no == '1') then
	local table_res = tab:where('id > 5')
	db:column_type("text", 1)
	db:column_type("text", 2)
	db:emit(table_res)
	return 0
 end
 if(test_no == '2') then
      db:emit("skipped test")
      return 0
 end
 if(test_no == '3') then
      local tab1 = db:table('tmp1',{{"id", 'integer'}, {"j", 'integer'}})
      tab1:insert({id="51",j="221"}) 
      tab1:copyfrom(tab)
      tab1:emit()
      return 0
 end
 if(test_no == '4') then
      local tab1 = db:table('tmp1',{{"id", 'integer'},{"j", 'integer'}})
      tab1:insert({id="51",j="221"}) 
      tab1:copyfrom(tab, 'id = 5')
      tab1:emit()
      return 0
 end
end}$$
put default procedure test9 'sptest9'
exec procedure test9('1')
exec procedure test9('2')
exec procedure test9('3')
exec procedure test9('4')
EOF


cdb2sql $SP_OPTIONS - <<'EOF'
set transaction snapshot isolation
delete from t where 1
begin
insert into t values(3,4)
insert into t values(5,10)
exec procedure test3('1', 'select * from t')
rollback
EOF


cdb2sql $SP_OPTIONS - <<'EOF'
insert into t values(1,2)
insert into t values(2,3)
exec procedure test3('2', 'select i as column1, j as column2 from t order by column1')


create procedure test10 version 'sptest10' {
local function main(test_no, t)
 if(test_no == '1') then
      local result_set, rc = db:exec("select * from t1")
      local result = result_set:fetch()
      db:num_columns(2)
      db:column_name("column1", 1)
      db:column_name("column2", 2)
      db:emit(result.de, result.db)
      return 0
 end
 db:emit(-1)
end}$$
put default procedure test10 'sptest10'
exec procedure test10('1', 'select * from t')


create procedure test11 version 'sptest11' {
local function myfunc(tab, val)
  local rc = tab:insert({i = val,j = val})
  return 0;
end
local function main(t)
  local tab = db:table('t')
  db:begin()
  local rc = tab:insert({i = 29, j = 29})
  db:commit()
  local th = db:create_thread(myfunc,tab, 200)
  local th1 = db:create_thread(myfunc, tab,500)
  local th2 = db:create_thread(myfunc, tab, 900)
  local th3 = db:create_thread(myfunc, tab, 1900)
  th:join()
  th1:join()
  th2:join()
  th3:join()
  db:exec("select * from t order by i"):emit()
end}$$
put default procedure test11 'sptest11'
exec procedure test11()
select * from t order by i
EOF


cdb2sql $SP_OPTIONS - <<EOF
create table prepare {$(cat prepare.csc2)}\$\$
create procedure prepare version 'sptest' {$(cat prepare.lua)}\$\$
put default procedure prepare 'sptest'
exec procedure prepare()


create procedure close version 'sptest' {$(cat close.lua)}\$\$
put default procedure close 'sptest'
exec procedure close()


create procedure bound version 'sptest' {$(cat bound.lua)}\$\$
put default procedure bound 'sptest'
EOF
${TESTSBUILDDIR}/bound "$CDB2_CONFIG" $DBNAME


cdb2sql $SP_OPTIONS - <<EOF
create procedure blobliteral version 'sptest' {$(cat blobliteral.lua)}\$\$
put default procedure blobliteral 'sptest'
exec procedure blobliteral()


create procedure types version 'sptest' {$(cat types.lua)}\$\$
put default procedure types 'sptest'
exec procedure types()


create procedure hexarg version 'sptest' {$(cat hexarg.lua)}\$\$
put default procedure hexarg 'sptest'
exec procedure hexarg(x'', x'deadbeef', x'00112233445566778899001122334455667788990011223344556677889900112233445566778899')
EOF


# Number of concurrent sp's
numsp=10
# How many good iterations
iter=200
function sploop
{
    typeset cnt=0
    typeset rc=0
    while [[ $cnt -lt $iter ]] ; do
        cdb2sql $SP_OPTIONS "exec procedure commitsp()"
        rc=$?
        if [[ $rc != 0 ]]; then
            return $rc
        fi
        let cnt=cnt+1
    done
    return $rc
}
cdb2sql $SP_OPTIONS - <<EOF
create table commitsp {$(cat commitsp.csc2)}\$\$
create procedure commitsp {$(cat commitsp.lua)}\$\$
EOF
# Insert a 1
cdb2sql $SP_OPTIONS "insert into commitsp(a) values (1)"
r=$?
if [[ $r != 0 ]]; then
    echo "Unable to add a record!  rcode is $r"
    return 1
fi
j=0
while [[ $j -lt $numsp ]]; do
    sploop &
    let j=j+1
done
wait


cdb2sql $SP_OPTIONS - <<EOF
create table transactions {$(cat transactions.csc2)}\$\$
create procedure transactions version 'sptest' {$(cat transactions.lua)}\$\$
put default procedure transactions 'sptest'

select "test 0"
begin
insert into transactions values(1)
exec procedure transactions('["insert into transactions values(2)"]')
rollback
select count(*) from transactions
select * from transactions order by i

select "test 1"
begin
insert into transactions values(3)
exec procedure transactions('["insert into transactions values(4)"]')
commit
select count(*) from transactions
select * from transactions order by i

select "test 2"
exec procedure transactions('["begin", "insert into transactions values(5)", "insert into transactions values(6)", "rollback"]')
select count(*) from transactions
select * from transactions order by i

select "test 3"
exec procedure transactions('["begin", "insert into transactions values(7)", "insert into transactions values(8)", "commit"]')
select count(*) from transactions
select * from transactions order by i

select "test 4"
exec procedure transactions('["insert into transactions values(9)", "insert into transactions values(10)", "begin", "insert into transactions values(11)", "insert into transactions values(12)", "rollback", "begin", "insert into transactions values(13)", "insert into transactions values(14)", "commit", "insert into transactions values(15)", "insert into transactions values(16)"]')
select count(*) from transactions
select * from transactions order by i

select "test 5"
begin
exec procedure transactions('["begin"]')
rollback
select count(*) from transactions
select * from transactions order by i

select "test 6"
exec procedure transactions('["begin"]')
select count(*) from transactions
select * from transactions order by i

select "test 7"
exec procedure transactions('["commit"]')
select count(*) from transactions
select * from transactions order by i

select "test 8"
exec procedure transactions('["rollback"]')
select count(*) from transactions
select * from transactions order by i
EOF


cdb2sql $SP_OPTIONS - <<EOF
create table nums {$(cat nums.csc2)}\$\$
create procedure gen version 'sptest' {$(cat gen.lua)}\$\$
put default procedure gen 'sptest'
create procedure myavg version 'sptest' {$(cat myavg.lua)}\$\$
put default procedure myavg 'sptest'
create lua aggregate function myavg
exec procedure gen()
select avg(i) from nums
select myavg(i) from nums
drop lua aggregate function myavg
select myavg(i) from nums
EOF


cdb2sql $SP_OPTIONS - <<EOF
create table strings {$(cat strings.csc2)}\$\$
create procedure json_extract {$(cat json_extract.lua)}\$\$
create lua scalar function json_extract
insert into strings(str) values('{"i":10}')
insert into strings(str) values('{"i":20}')
select json_extract(str, 'i') as i from strings
drop lua scalar function json_extract
EOF


cdb2sql $SP_OPTIONS - <<'EOF'
#    _       _     _   ____  ____
#   / \   __| | __| | / ___||  _ \ ___
#  / _ \ / _` |/ _` | \___ \| |_) / __|
# / ___ \ (_| | (_| |  ___) |  __/\__ \
#/_/   \_\__,_|\__,_| |____/|_|   |___/
create procedure sp1 {
local function hdr(h)
    if h ~= nil then
        db:emit('sp1 hdr', 0)
        return
    end
    db:num_columns(2)
    db:column_name('name', 1)
    db:column_type('text', 1)
    db:column_name('ver', 2)
    db:column_type('int', 2)
end
local function main(h)
    hdr(h)
    db:emit({name='sp1', ver=1})
end
}$$
create procedure sp1 {
local function hdr()
    db:num_columns(2)
    db:column_name('name', 1)
    db:column_type('text', 1)
    db:column_name('ver', 2)
    db:column_type('int', 2)
end
local function main()
    hdr()
    db:emit({name='sp1', ver=2})
end
}$$
create procedure sp2 {
local function hdr(h)
    if h ~= nil then
        db:emit('sp2 hdr', 0)
        return
    end
    db:num_columns(2)
    db:column_name('name', 1)
    db:column_type('text', 1)
    db:column_name('ver', 2)
    db:column_type('int', 2)
end
local function main(h)
    hdr(h)
    db:emit({name='sp2', ver=1})
end
}$$
create procedure sp2 {
local function hdr()
    db:num_columns(2)
    db:column_name('name', 1)
    db:column_type('text', 1)
    db:column_name('ver', 2)
    db:column_type('int', 2)
end
local function main()
    hdr()
    db:emit({name='sp2', ver=2})
end
}$$
create procedure sp3 {
local function hdr(h)
    if h ~= nil then
        db:emit('sp3 hdr', 0)
        return
    end
    db:num_columns(2)
    db:column_name('name', 1)
    db:column_type('text', 1)
    db:column_name('ver', 2)
    db:column_type('int', 2)
end
local function main()
    hdr()
    local sp1 = db:sp('sp1', 1)
    local sp2 = db:sp('sp2', 1)
    sp1('hi')
    sp2('hi')
    hdr('hi')
    db:emit({name='sp3', ver=1})
end
}$$
create procedure sp4 {
local function main()
    db:num_columns(1)
    db:column_name('id', 1)
    db:column_type('integer', 1)
    db:emit({id=1})
end}$$
create procedure sp4 {
local function main()
    db:num_columns(1)
    db:column_name('id', 1)
    db:column_type('integer', 1)
    db:emit({id=2})
end}$$
create procedure sp4 version '8eb31cbc-f341-433b-9110-53713e0dd257' {
local function main()
    db:num_columns(1)
    db:column_name('id', 1)
    db:column_type('text', 1)
    db:emit({id='8eb31cbc-f341-433b-9110-53713e0dd257'})
end}$$
create procedure sp4 version '84c256f9-854f-406b-9444-92bd2a63cba1' {
local function main()
    db:num_columns(1)
    db:column_name('id', 1)
    db:column_type('text', 1)
    db:emit({id='84c256f9-854f-406b-9444-92bd2a63cba1'})
end}$$
# ____                ____  ____
#|  _ \ _   _ _ __   / ___||  _ \ ___
#| |_) | | | | '_ \  \___ \| |_) / __|
#|  _ <| |_| | | | |  ___) |  __/\__ \
#|_| \_\\__,_|_| |_| |____/|_|   |___/
select "SET AND EXEC DEFAULT VERSIONS" as testcase
put default procedure sp1 2
exec procedure sp1()
put default procedure sp1 1
exec procedure sp1()
put default procedure sp2 1
exec procedure sp2()
put default procedure sp2 2
exec procedure sp2()
put default procedure sp3 1
exec procedure sp3()
select "EXEC SP1 WITH DIFFERENT SP/SPVERSION OVERRIDES" as testcase
set spversion sp1 1
exec procedure sp1()
set spversion sp1 2
exec procedure sp1()
set spversion sp2 1
exec procedure sp1()
set spversion sp2 2
exec procedure sp1()
select "EXEC SP2 WITH DIFFERENT SP/SPVERSION OVERRIDES" as testcase
set spversion sp1 1
exec procedure sp2()
set spversion sp1 2
exec procedure sp2()
set spversion sp2 1
exec procedure sp2()
set spversion sp2 2
exec procedure sp2()
select "TEST SET SPVERSION WITH NEW/OLD STYLE VERSIONS" as testcase
set spversion sp4 1
exec procedure sp4()
set spversion sp4 2
exec procedure sp4()
set spversion sp4 '8eb31cbc-f341-433b-9110-53713e0dd257'
exec procedure sp4()
set spversion sp4 '84c256f9-854f-406b-9444-92bd2a63cba1'
exec procedure sp4()
# ____            _    ___                      _     _
#| __ )  __ _  __| |  / _ \__   _____ _ __ _ __(_) __| | ___  ___
#|  _ \ / _` |/ _` | | | | \ \ / / _ \ '__| '__| |/ _` |/ _ \/ __|
#| |_) | (_| | (_| | | |_| |\ V /  __/ |  | |  | | (_| |  __/\__ \
#|____/ \__,_|\__,_|  \___/  \_/ \___|_|  |_|  |_|\__,_|\___||___/
set spversion sp1 100
exec procedure sp1()
set spversion sp2 200
exec procedure sp2()
EOF
# _   _                 ____                _
#| \ | | _____      __ / ___|  ___  ___ ___(_) ___  _ __
#|  \| |/ _ \ \ /\ / / \___ \ / _ \/ __/ __| |/ _ \| '_ \
#| |\  |  __/\ V  V /   ___) |  __/\__ \__ \ | (_) | | | |
#|_| \_|\___| \_/\_/   |____/ \___||___/___/_|\___/|_| |_|
cdb2sql $SP_OPTIONS - <<'EOF'
select "SPVERSION APPLIES TO ONGOING SESSION ONLY" as testcase
set spversion sp4 2
exec procedure sp4()
EOF
cdb2sql $SP_OPTIONS - <<'EOF'
exec procedure sp4()
set spversion sp4 '8eb31cbc-f341-433b-9110-53713e0dd257'
exec procedure sp4()
EOF
cdb2sql $SP_OPTIONS "exec procedure sp4()"
#     _       __             _ _
#  __| | ___ / _| __ _ _   _| | |_ ___ _ __
# / _` |/ _ \ |_ / _` | | | | | __/ __| '_ \
#| (_| |  __/  _| (_| | |_| | | |_\__ \ |_) |
# \__,_|\___|_|  \__,_|\__,_|_|\__|___/ .__/
#                                     |_|
cdb2sql $SP_OPTIONS - <<'EOF'
select 'SET INVALID DEFAULTS - VERSIONED SPs HAVE CHECKS' as testcase
put default procedure sp2000 '2000'
put default procedure sp1 '2000'
select 'SHOULD GET SAME RESULTS AS comdb2sc.tsk' as testcase
put default procedure sp2000 2000
put default procedure sp1 2000
EOF


cdb2sql $SP_OPTIONS - <<EOF
create table dup {$(cat dup.csc2)}\$\$
create procedure dup {$(cat dup.lua)}\$\$
exec procedure dup()
EOF


cdb2sql $SP_OPTIONS - <<EOF
create table foraudit {$(cat foraudit.csc2)}\$\$
create table audit {$(cat audit.csc2)}\$\$
create procedure audit version 'sptest' {$(cat audit.lua)}\$\$
put default procedure audit 'sptest' 
create procedure cons version 'sptest' {$(cat cons.lua)}\$\$
put default procedure cons 'sptest'
create lua trigger audit on (table foraudit for insert and update and delete)
create lua consumer cons on (table foraudit for insert and update and delete)
EOF

for ((i=0;i<500;++i)); do
    echo "drop lua consumer cons"
    echo "create lua consumer cons on (table foraudit for insert and update and delete)"
done | cdb2sql $SP_OPTIONS - > /dev/null

sleep 3 # Wait for trigger to start
cdb2sql $SP_OPTIONS "exec procedure cons()" > /dev/null 2>&1 &
#GENERATE DATA
cdb2sql $SP_OPTIONS "exec procedure gen('foraudit', 500)"
#DELETE DATA
cdb2sql $SP_OPTIONS "delete from foraudit where 1"
sleep 20 # Wait for queues to drain
cdb2sql $SP_OPTIONS - <<'EOF'
select * from comdb2_triggers order by name, type, tbl_name, event
drop lua trigger audit
drop lua consumer cons
select * from comdb2_triggers order by name, type, tbl_name, event
select added_by, type, count(*) from audit group by added_by, type
EOF
wait

cdb2sql $SP_OPTIONS - <<'EOF'
create table for_trigger1 {schema{
  int i
  int j
  int k
}}$$
create table for_trigger2 {schema{
  int a
  int b
  int c
}}$$
create procedure a {local function main() end}$$
create procedure b {local function main() end}$$
create lua trigger a on (table for_trigger1 for insert and update and delete),(table for_trigger2 for insert and update and delete)
create lua consumer b on (table for_trigger1 for insert of i, j and update of j and delete of i, j, k),(table for_trigger2 for insert of c,b,a and update of c,b and delete of c)
select * from comdb2_triggers order by name, type, tbl_name, event
EOF

cdb2sql $SP_OPTIONS "select name, version, client_versioned, \"default\" from comdb2_procedures order by name, version, client_versioned, \"default\""

#verify commands
cdb2sql $SP_OPTIONS - <<'EOF'
select 1
exec procedure sys.cmd.verify("t1")
select 2
exec procedure sys.cmd.verify()
select 3
exec procedure sys.cmd.verify(\"nonexistent\")
select 4
exec procedure sys.cmd.verify("nonexistent")
select 5
EOF

cdb2sql $SP_OPTIONS - <<'EOF'
create procedure tmptbls version 'sptest' {
local function func(tbls)
    for i, tbl in ipairs(tbls) do
        tbl:insert({i=i})
    end
end
local function main()
    local tbl = db:table("tbl", {{"i", "int"}})
    for i = 1, 20 do
        local tbls = {}
        for j = 1, i do
            table.insert(tbls, tbl)
        end
        db:create_thread(func, tbls)
    end
    db:sleep(2) -- enough time for threads to finish
    db:exec("select i, count(*) from tbl group by i"):emit()
end
}$$
put default procedure tmptbls 'sptest'
exec procedure tmptbls()
exec procedure tmptbls()
exec procedure tmptbls()
exec procedure tmptbls()
exec procedure tmptbls()
EOF

cdb2sql $SP_OPTIONS - <<'EOF'
drop table if exists t
drop table if exists u
create table u {
    schema { int i }
    keys {
        dup "i" = i
    }
}$$
create table t {
    schema {
        int i
    }
    keys {
        "i" = i
    }
    constraints {
        "i" -> <"u":"i"> on delete cascade
    }
}$$
create procedure rcodes version 'sptest' {
local function init()
    db:begin()
    db:exec("delete from u where 1")
    db:exec("insert into u values(1),(2),(3),(4),(5)")
    db:commit()
end
local function dup()
    init()
    local t = db:table("t")
    local rc0 = db:begin()
    local rc1 = t:insert{i=0}
    local rc2 = t:insert{i=0}
    local rc3 = db:commit()
    if rc0 == 0 and rc1 == 0 and rc2 == 0 and rc3 == db.err_dup then
        db:emit("dup pass")
    else
        db:emit(db:table_to_json{rc0, rc1, rc2, rc3, db:error(), "dup failed"})
    end
end
local function del(where_clause)
    local rc0 = db:begin()
    local _, rc1 = db:exec("delete from u " .. where_clause)
    local rc3 = db:commit()
    return rc3
end
local function verify()
    init()
    local rc0 = db:begin()
    local stmt, rc1 = db:exec("update u set i = 99")
    stmt:close()
    local thd = db:create_thread(del, "where 1")
    local rc2 = thd:join()
    local rc3 = db:commit()
    if rc0 == 0 and rc1 == 0 and rc2 == 0 and rc3 == db.err_verify then
        db:emit("verify pass")
    else
        db:emit(db:table_to_json{rc0, rc1, rc2, rc3, db:error(), "verify failed"})
    end
end
local function fkey()
    init()
    local t = db:table("t")
    local rc0 = db:begin()
    local rc1 = t:insert{i=0}
    local rc2 = db:commit()
    if rc0 == 0 and rc1 == 0 and rc2 == db.err_fkey then
        db:emit("fkey pass")
    else
        db:emit(db:table_to_json{rc0, rc1, rc2, db:error(), "fkey failed"})
    end
end
local function null()
    init()
    local t = db:table("t")
    local rc0 = db:begin()
    local rc1 = t:insert{i=db:NULL()}
    local rc2 = db:commit()
    if rc0 == 0 and rc1 == 0 and rc2 == db.err_null_constraint then
        db:emit("null pass")
    else
        db:emit(db:table_to_json{rc0, rc1, rc2, db:error(), "null failed"})
    end
end
local function selectv_int(i)
    local rc0 = db:begin()
    local stmt, rc1 = db:exec("selectv * from u")
    while stmt:fetch() do end
    stmt:close()
    local thd = db:create_thread(del, "where i = " .. tostring(i))
    local rc2 = thd:join()
    local rc3 = db:commit()
    if rc0 == 0 and rc1 == 0 and rc2 == 0 and rc3 == db.err_selectv then
        db:emit("selectv pass")
    else
        db:emit(db:table_to_json{rc0, rc1, rc2, rc3, db:error(), "selectv failed"})
    end
end
local function selectv()
    init()
    selectv_int(5) -- Past EOF

    init()
    selectv_int(1) -- First

    init()
    selectv_int(3) -- Somewhere in the middle
end
local function conv()
    init()
    local rc0, rc1 = db:exec("insert into u values(5000000000)")
    if rc0 == nil and rc1 == db.err_conv then
        db:emit("conv pass")
    else
        db:emit(db:table_to_json{rc0, rc1, db:error(), "conv failed"})
    end
end
local function main()
    dup()
    verify()
    fkey()
    null()
    selectv()
    conv()
    db:emit("finished")
end
}$$
put default procedure rcodes 'sptest'
exec procedure rcodes()
EOF

cdb2sql $SP_OPTIONS - <<'EOF'
create procedure tmptbls version 'sptest' {
local function func(tbls)
    for i, tbl in ipairs(tbls) do
        tbl:insert({i=i})
    end
end
local function main()
    local tbl = db:table("tbl", {{"i", "int"}})
    for i = 1, 20 do
        local tbls = {}
        for j = 1, i do
            table.insert(tbls, tbl)
        end
        db:create_thread(func, tbls)
    end
    db:sleep(2) -- enough time for threads to finish
    db:exec("select i, count(*) from tbl group by i"):emit()
end
}$$
put default procedure tmptbls 'sptest'
exec procedure tmptbls()
exec procedure tmptbls()
exec procedure tmptbls()
exec procedure tmptbls()
exec procedure tmptbls()
EOF

cdb2sql $SP_OPTIONS - <<'EOF'
create procedure reset_test version 'sptest' {
local total = 100
local function setup()
    local s = db:prepare("insert into t values(?)")
    local t = db:table("t")
    local tmp = db:table("tmp", {{"i", "int"}})
    db:begin()
    for i = 1, total do
        s:bind(1, i)
        s:exec()
		tmp:insert({i = i})
	end
    db:commit()
    db:exec("select count(*) as i from t"):emit()
    db:exec("select count(*) as i from tmp"):emit()
    return s
end
local function main()
    db:setmaxinstructions(1000000)
    local s0 = setup()

    local t = db:table("t")
    local tmp = db:table("tmp")

    local s1 = db:exec("select i from t order by i")
    local s2 = db:exec("select i from tmp")
    local s3 = db:exec("select i from tmp")

    for i = 1, total/2 do
        db:emit(s1:fetch())
        db:emit(s2:fetch())
    end

    local s4 = db:exec("delete from t where 1")

    local row = s3:fetch()
    while row do
        db:begin() -- s0:reset;  s1,s4:finalized;  s2,s3:unaffected
        s0:bind(1, row.i)
        s0:exec()
        db:commit()
        row = s3:fetch()
    end

    row = s2:fetch()
    if row == nil then
        return -202, "bad tmp table"
    end
    repeat
        db:emit(row)
        row = s2:fetch()
    until row == nil

    row = s1:fetch()
    if row then
        db:emit(row)
        return -201, "bad real table"
    end
end
}$$
put default procedure reset_test 'sptest'
drop table if exists t
create table t {schema{int i} keys{dup "i" = i}}$$
exec procedure reset_test()
select 100 as expected, count(*) as actual from t
EOF


#TEST RUNNING PREPARED STMT IN A LOOP
cdb2sql $SP_OPTIONS - <<'EOF'
drop table if exists t
create table t {schema{int i}keys{"i"=i}}
EOF

#INSERT 50K rows
cdb2sql $SP_OPTIONS "insert into t(i) values $(for((i=00000;i<10000;++i));do echo -n "${COMMA}(${i})";COMMA=",";done)" > /dev/null &
sleep 0.25
cdb2sql $SP_OPTIONS "insert into t(i) values $(for((i=10000;i<20000;++i));do echo -n "${COMMA}(${i})";COMMA=",";done)" > /dev/null &
sleep 0.25
cdb2sql $SP_OPTIONS "insert into t(i) values $(for((i=20000;i<30000;++i));do echo -n "${COMMA}(${i})";COMMA=",";done)" > /dev/null &
sleep 0.25
cdb2sql $SP_OPTIONS "insert into t(i) values $(for((i=30000;i<40000;++i));do echo -n "${COMMA}(${i})";COMMA=",";done)" > /dev/null &
sleep 0.25
cdb2sql $SP_OPTIONS "insert into t(i) values $(for((i=40000;i<50000;++i));do echo -n "${COMMA}(${i})";COMMA=",";done)" > /dev/null &
wait

#LOOKUP EACH INDIVIDUAL ROW USING SAME STMT
cdb2sql $SP_OPTIONS - <<'EOF'
create procedure binder version 'sptest' {
local function main()
    db:setmaxinstructions(10000000)
    local stmt = db:prepare("select i first from t where i > @first and i <= @last")
    local row = { first = -1 }
    local first
    local last
    local count = 0
    while true do
        stmt:bind("first", row.first)
        stmt:bind("last", row.first + 1)
        row = stmt:fetch()
        if row == nil then break end
        if first == nil then
            first = row.first
        end
        last = row.first
        count = count + 1
    end
    --setup bogus column info
    for i = 10, 6, -1 do
        db:column_name("foo"..i, i)
        db:column_type("datetime", i)
    end
    for i = 1, 5 do
        db:column_name("foo"..i, i)
        db:column_type("datetime", i)
    end
    --set actual column info in no particular order
    db:column_name("first", 2); db:column_type("int", 2)
    db:num_columns(3)
    db:column_name("last",  3); db:column_type("int", 3)
    db:column_name("count", 1); db:column_type("int", 1)
    --emit columns
    db:emit(count, first, last)
    --this now should fail
    db:column_name("count", 1); db:column_type("int", 1)
end
}$$
put default procedure binder 'sptest'
exec procedure binder()
EOF

cdb2sql $SP_OPTIONS "select name, version, client_versioned, \"default\" from comdb2_procedures order by name, version, client_versioned, \"default\""

${TESTSBUILDDIR}/utf8 "$CDB2_CONFIG" $DBNAME
cdb2sql $SP_OPTIONS - <<'EOF'
create procedure json_utf8 version 'sptest' {
local function main(strategy)
    db:num_columns(3)
    db:column_name("strategy", 1); db:column_type("text", 1)
    db:column_name("json", 2);     db:column_type("text", 2)
    db:column_name("rc", 3);       db:column_type("int", 3)
    local row = db:exec("select * from t"):fetch()
    local json, rc
    if strategy then
        json, rc = db:table_to_json(row, {invalid_utf8 = strategy})
    else
        json, rc = db:table_to_json(row)
    end
    db:emit(strategy, json, rc)
end
}$$
put default procedure json_utf8 'sptest'
exec procedure json_utf8()
exec procedure json_utf8('fail')
exec procedure json_utf8('truncate')
exec procedure json_utf8('hex')

create procedure loop version 'sptest' {
local function main()
    local x = {}
    x.x = x
    db:table_to_json(x)
end
}$$
put default procedure loop 'sptest'
exec procedure loop()

create procedure json_annotate version 'sptest' {
local function check(x, y)
    for k, v0 in pairs(x) do
        local v1 = y[k]
        db:emit(tostring(k), type(k), tostring(v0), type(v0), tostring(v1), type(v1))
        if type(v0) ~= type(v1) then
            return -1
        end
        if db:isnull(v0) then
            if not db:isnull(v1) then
                return -2
            end
        elseif v0 ~= v1 then
            return -3
        end
    end
    return 0
end
local function get_nice_decimal()
    declare d1, d2 "decimal"
    d1 := "0.1"
    d2 := "0.2"
    return d1 + d2
end
local function main()
    declare i "int"
    declare r "real"
    declare d "decimal"
    declare c "cstring"
    declare b "blob"
    declare dt "datetime"
    declare id "intervalds"
    declare iy "intervalym"
    local s = "lua string"
    local n = 3.14159
    local bo = true
    local x = { i, r, d, c, b, dt, id, iy, s, n, bo}
    i := 592
    r := 3.14159
    d := get_nice_decimal()
    c := "comdb2 string"
    b := x'deadbeef'
    dt := "2017-04-18T160323.416 America/New_York"
    id := 60
    iy := 10
    table.insert(x, i)
    table.insert(x, r)
    table.insert(x, d)
    table.insert(x, c)
    table.insert(x, b)
    table.insert(x, dt)
    table.insert(x, id)
    table.insert(x, iy)
    local json = db:table_to_json(x, {type_annotate = true})
    local y = db:json_to_table(json, {type_annotate = true})
    local r0 = check(x, y)
    local r1 = check(y, x)
    if (r0 ~= 0) or (r1 ~= 0) then
        return -1, "failed"
    else
        return 0
    end
end
}$$
put default procedure json_annotate 'sptest'
exec procedure json_annotate()

create procedure nested_json version 'sptest' {
local function main()
    db:num_columns(2)
    db:column_name("val0", 1)
    db:column_type("text", 1)
    db:column_name("val1", 2)
    db:column_type("text", 2)

    local black = {r = 0, g = 0, b = 0}
    local gray = {r = 128, g = 128, b = 128}
    local white = {r = 255, g = 255, b = 255}
    local t0 = {
        colors = {black, gray, white},
        black = black,
        gray = gray,
        white = white
    }
    local j = db:table_to_json(t0, {type_annotate = true})
    local t1 = db:json_to_table(j, {type_annotate = true})
    db:emit(t0.black.r, t1.black.r)
    db:emit(t0.colors[3].r, t1.colors[3].r)
end}$$
put default procedure nested_json 'sptest'
exec procedure nested_json()

drop table if exists vsc
create table vsc(i int)$$
insert into vsc select * from generate_series(1,10)
create procedure verify_stmt_caching version 'sptest' {
local function main()
	local stmt1 = db:prepare("select * from vsc")
	local stmt2 = db:prepare("select * from vsc")

    for i = 1, 5 do
        stmt1:fetch()
    end

    local c = 0
    local row = stmt2:fetch()
    while row do
        row = stmt2:fetch()
        c = c + 1
    end
    db:emit(c)
end}$$
put default procedure verify_stmt_caching 'sptest'
exec procedure verify_stmt_caching()
exec procedure verify_stmt_caching()

EOF

wait
