drop table if exists t
create table t(i int)$$
exec procedure sys.cmd.send('fail_to_create_default_cons 1');
create default lua consumer t_cons on (table t for insert and update and delete)
exec procedure sys.cmd.send('fail_to_create_default_cons 0');
select count(*) from comdb2_procedures where name="t_cons"
create default lua consumer t_cons on (table t for insert and update and delete)
select count(*) from comdb2_procedures where name="t_cons"
