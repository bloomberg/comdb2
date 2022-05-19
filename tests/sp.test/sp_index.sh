#!/usr/bin/env bash

# check lua scalar function usage in table indexes
# 1. create procedures pending and resolved
# 2. create corresponding scalar functions
# 3. try creating a table with scalar function
# 4. expect failure because deterministic flag is required
# 5. drop and recreate scalar functions with deterministic flag
# 6. expect proper table recreation
# 7. create another table ticks with no index function usage
# 8. Try out creating index with CREATE INDEX on ticks
# 9. Now dropping proc or scalar func shouldn't be succesful for tickets and ticks
# 10. Verifies that we print message with the first table using scalar functions
[[ -n "$3" ]] && exec >$3 2>&1
drop_func_proc() {
  cdb2sql $SP_OPTIONS <<EOF
  drop lua scalar function resolved
  drop lua scalar function pending
  drop procedure resolved version 'sptest'
  drop procedure pending version 'sptest'
EOF
}

cdb2sql $SP_OPTIONS - <<EOF
create procedure resolved version 'sptest' {$(cat resolved.lua)}\$\$
create procedure pending version 'sptest' {$(cat pending.lua)}\$\$
create lua scalar function resolved
EOF

if ! cdb2sql $SP_OPTIONS -- "create table tickets {$(cat tickets.csc2)}"; then
  true
fi

cdb2sql $SP_OPTIONS - <<EOF
drop lua scalar function resolved
create lua scalar function pending deterministic
create lua scalar function resolved deterministic
create table tickets {$(cat tickets1.csc2)}\$\$
insert into tickets values (1, 2)
insert into tickets values (2, 4)
insert into tickets values (3, 11)
select * from tickets where resolved(status)=1
select * from tickets where pending(status)=1
create table ticks {$(cat ticks.csc2)}\$\$
create index id_pending_resolved on ticks (id, cast(pending(status) as int), cast(resolved(status) as int))
EOF
if ! drop_func_proc; then true; fi
cdb2sql $SP_OPTIONS -- "drop table tickets"
if ! drop_func_proc; then true; fi
cdb2sql $SP_OPTIONS -- "drop index id_pending_resolved on ticks"
drop_func_proc
cdb2sql $SP_OPTIONS -- "drop table ticks"

unset -f drop_func_proc
