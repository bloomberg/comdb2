select * from comdb2_users;
put password 'foo' for 'foo'
put password 'bar' for 'bar'
grant op to 'foo'

# unauthenticated user with authentication disabled
create table t1(i int)$$
create table t2(i int, i int)$$
create table t3{schema { int i }}$$
select * from comdb2_tables;
drop table t1;
drop table t2;
drop table t3;

# OP user
set user 'foo'
set password 'foo'
put authentication on
create table t1(i int)$$
create table t2(i int, i int)$$
create table t3{schema { int i }}$$
select * from comdb2_tables;
drop table t1;
drop table t2;
drop table t3;

# non-OP user
set user 'bar'
set password 'bar'
create table t1(i int)$$
create table t2(i int, i int)$$
create table t3{schema { int i }}$$
select * from comdb2_tables;
drop table t1;
drop table t2;
drop table t3;

# cleanup
set user 'foo'
set password 'foo'
put password off for 'bar'
put authentication off
put password off for 'foo'
select * from comdb2_users;
select * from comdb2_tables;
