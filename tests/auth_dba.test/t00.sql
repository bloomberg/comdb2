select 'disabled' as authentication_status;

select * from comdb2_users order by username;
select * from comdb2_tunables where name like '%_dba_user' order by name;

create table t1(i int)$$
insert into t1 values(1);
select * from t1;

-- test 'lock_dba_user'
grant op to 'dba';
revoke op from 'dba';
put password off for 'dba';
grant op to 'dba';
revoke op from 'dba';
revoke read on 't1' from 'dba';

put password 'root' for 'root'
grant op to 'root';
select * from comdb2_users;

-- login using 'root' user and enable authentication
set user 'root'
set password 'root'
select 'root' as current_user;
put authentication on

select 'enabled' as authentication_status;

select * from comdb2_users order by username;
select * from t1;
-- another op user must not still not be able to change 'dba' user permissions
grant op to 'dba';
revoke op from 'dba';
put password off for 'dba';
grant op to 'dba';
revoke op from 'dba';
revoke read on 't1' from 'dba';

-- login using 'dba' user
set user 'dba'
set password ''
select 'dba' as current_user;
put authentication off
put authentication on
put password off for 'root'

select * from comdb2_users order by username;

-- cleanup
put authentication off
drop table t1;
