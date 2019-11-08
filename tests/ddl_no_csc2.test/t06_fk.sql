select "transactional create (bad case)"
begin
create table c (i int, constraint fk foreign key (i) references m(i)) $$
create table m (i int primary key) $$
commit
select name, csc2 from sqlite_master where name = "c"
select name, csc2 from sqlite_master where name = "d"
select name, csc2 from sqlite_master where name = "e"
select name, csc2 from sqlite_master where name = "m"

select "transactional create"
begin
create table m (i int primary key) $$
create table c (i int, constraint fkc foreign key (i) references m(i)) $$
create table d (i int, constraint fkd foreign key (i) references m(i)) $$
commit
select name, csc2 from sqlite_master where name = "c"
select name, csc2 from sqlite_master where name = "d"
select name, csc2 from sqlite_master where name = "e"
select name, csc2 from sqlite_master where name = "m"

select "transactional alter (bad case 1)"
alter table m drop index "COMDB2_PK" $$
select "transactional alter (bad case 2)"
begin
alter table m drop index "COMDB2_PK" $$
alter table c drop foreign key fkc $$
commit
select "transactional alter (bad case 3)"
begin
alter table c drop foreign key fkc $$
alter table m drop index "COMDB2_PK" $$
commit
select "transactional alter (bad case 4)"
begin
alter table c drop foreign key fkc $$
alter table m drop index "COMDB2_PK" $$
alter table d drop foreign key fkd $$
commit
select name, csc2 from sqlite_master where name = "c"
select name, csc2 from sqlite_master where name = "d"
select name, csc2 from sqlite_master where name = "e"
select name, csc2 from sqlite_master where name = "m"

select "transactional alter (good case)"
begin
alter table c drop foreign key fkc $$
alter table d drop foreign key fkd $$
alter table m drop index "COMDB2_PK" $$
commit
select name, csc2 from sqlite_master where name = "c"
select name, csc2 from sqlite_master where name = "d"
select name, csc2 from sqlite_master where name = "e"
select name, csc2 from sqlite_master where name = "m"

select "alter & add (bad case)"
alter table c add constraint fkc foreign key (i) references m(i) $$
create table e (i int, constraint fke foreign key (i) references m(i)) $$
select name, csc2 from sqlite_master where name = "c"
select name, csc2 from sqlite_master where name = "d"
select name, csc2 from sqlite_master where name = "e"
select name, csc2 from sqlite_master where name = "m"

select "transactional alter & add (good case)"
begin
alter table m add primary key (i) $$
alter table c add constraint fkc foreign key (i) references m(i) $$
alter table d add constraint fkd foreign key (i) references m(i) $$
create table e (i int, constraint fke foreign key (i) references m(i)) $$
commit
select name, csc2 from sqlite_master where name = "c"
select name, csc2 from sqlite_master where name = "d"
select name, csc2 from sqlite_master where name = "e"
select name, csc2 from sqlite_master where name = "m"

select "transactional alter & drop (bad case)"
begin
drop table c
drop table d
alter table m drop index "COMDB2_PK" $$
drop table e
commit
select name, csc2 from sqlite_master where name = "c"
select name, csc2 from sqlite_master where name = "d"
select name, csc2 from sqlite_master where name = "e"
select name, csc2 from sqlite_master where name = "m"

select "transactional alter & drop (good case)"
begin
drop table c
drop table d
drop table e
alter table m drop index "COMDB2_PK" $$
commit
select name, csc2 from sqlite_master where name = "c"
select name, csc2 from sqlite_master where name = "d"
select name, csc2 from sqlite_master where name = "e"
select name, csc2 from sqlite_master where name = "m"

drop table m;

select "add self-referenced bad 1"
create table s (i int unique, j int, constraint sfk foreign key (k) references s(i)) $$
select "add self-referenced bad 2"
create table s (i int, j int, constraint sfk foreign key (j) references s(k)) $$
select "add self-referenced good"
create table s (i int unique, j int, constraint sfk foreign key (j) references s(i)) $$
select name, csc2 from sqlite_master where name = "s"
select "truncate self-referenced"
truncate s
select name, csc2 from sqlite_master where name = "s"
select "alter self-referenced drop constraints"
alter table s drop foreign key sfk $$
select name, csc2 from sqlite_master where name = "s"
select "alter self-referenced bad 1"
alter table s add constraint sfk foreign key (k) references s(i) $$
select "alter self-referenced bad 2"
alter table s add constraint sfk foreign key (j) references s(k) $$
select "alter self-referenced good"
alter table s add constraint sfk foreign key (j) references s(i) $$
select name, csc2 from sqlite_master where name = "s"
select "drop self-referenced table"
drop table s
select name, csc2 from sqlite_master where name = "s"

select "tests for nullfkey (enabled by default)";
create table t1(i int unique)$$
create table t2(i int unique, foreign key (i) references t1(i))$$
# the following insert must fail
insert into t2 values(1);
insert into t1 values(1);
insert into t2 values(1);
# the following delete must fail
delete from t1 where i=1;
insert into t2 values(null);
insert into t1 values(null);
select * from t1 order by i;
select * from t2 order by i;
drop table t2;
drop table t1;
