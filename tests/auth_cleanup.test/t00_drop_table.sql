select * from comdb2_users;
select * from comdb2_tables order by tablename;

-- create users
put password 'root' for 'root';
put password 'foo' for 'foo';

-- set OP user
grant op to 'root'
set user 'root'
set password 'root'

-- turn on authentication
put authentication on

create table t1(i int)$$
insert into t1 values(1);
select * from t1;

-- grant READ to 'foo'
grant READ on 't1' to 'foo';

-- switch to 'foo' user
set user 'foo'
set password 'foo'
select * from t1;

-- switch back to the 'root' user and recreate the table
set user 'root'
set password 'root'
drop table t1;
create table t1(i int)$$
insert into t1 values(1);
select * from t1;

-- switch back to 'foo' user, should no longer have access to 't1'
set user 'foo'
set password 'foo'
select * from t1;

-- cleanup
set user 'root'
set password 'root'
drop table t1;
put authentication off
put password off for 'root';
put password off for 'foo';
select * from comdb2_users;
select * from comdb2_tables order by tablename;
