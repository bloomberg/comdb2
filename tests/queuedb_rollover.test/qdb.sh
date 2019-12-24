cdb2sql $SP_OPTIONS - <<EOF
create table foraudit {$(cat foraudit.csc2)}\$\$
create procedure nop0 version 'noptest' {$(cat nop_consumer.lua)}\$\$
create procedure log1 version 'logtest' {$(cat log_consumer.lua)}\$\$
create lua consumer nop0 on (table foraudit for insert and update and delete)
create lua consumer log1 on (table foraudit for insert and update and delete)
EOF
