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

select "tests for FK order desc keys";
create table t1(i int)$$
create index "idx1" on t1(i desc);
create table t2(i int unique, foreign key (i) references t1(i))$$
select csc2 as "t1 schema" from sqlite_master where name = "t1";
select csc2 as "t2 schema" from sqlite_master where name = "t2";
insert into t2 values(1);
insert into t1 values(1);
insert into t2 values(1);
select * from t1;
select * from t1;
drop table t2;
drop table t1;

create table t1(i int)$$
create index "idx1" on t1(i desc);
create table t2(i int)$$
create index "idx2" on t2(i desc);
alter table t2 add foreign key (i) references t1(i)$$
select csc2 as "t1 schema" from sqlite_master where name = "t1";
select csc2 as "t2 schema" from sqlite_master where name = "t2";
insert into t2 values(1);
insert into t1 values(1);
insert into t2 values(1);
select * from t1;
select * from t2;
drop table t2;
drop table t1;

select "test for self-reference constraints - part 1"
create table t1(i int unique, j int unique, foreign key (i) references t1(j) on update cascade)$$
insert into t1 values(1,1);
update t1 set j=2 where j=1;
select * from t1;
drop table t1;

select "test for self-reference constraints - part 2"
create table t2(i int index, j int index, foreign key (i) references t2(j) on update cascade)$$
insert into t2 values(1,2),(2,1);
update t2 set j=2 where j=1;
select * from t2;
drop table t2;

select "test for self-reference constraints - part 3"
create table t3(i int index, j int index, foreign key (i) references t3(j) on update cascade)$$
insert into t3 values(1,2),(2,1);
update t3 set j=2 where j=1;
select * from t3;
drop table t3

select "test for self-reference constraints - part 4"
create table t4(i int unique, j int unique, foreign key (i) references t4(j) on update cascade, foreign key (j) references t4(i) on update cascade)$$
insert into t4 values(1,1);
update t4 set j=2 where j=1;
select * from t4;
drop table t4;

select "test for self-reference constraints - part 5"
create table t5(i int unique, j int unique, foreign key (i) references t5(j) on update cascade)$$
create table t6(i int unique, foreign key (i) references t5(i) on update cascade)$$
insert into t5 values(1,1);
insert into t6 values(1);
update t5 set j=2 where j=1;
select * from t5;
select * from t6;
drop table t6;
drop table t5;

select "multi-level cascades - part 1";
create table t1(i int unique, j int unique)$$
insert into t1 values(1,1);
create table t2(i int unique, j int unique, foreign key (i) references t1(i) on update cascade on delete cascade)$$
insert into t2 values(1,2);
create table t3(i int unique, j int unique, foreign key (i) references t2(i) on update cascade on delete cascade)$$
insert into t3 values(1,3);
create table t4(i int unique, j int unique, foreign key (i) references t3(i) on update cascade on delete cascade)$$
insert into t4 values(1,4);
select * from t1;
select * from t2;
select * from t3;
select * from t4;
# this update must fail
update t2 set i=2 where i=1;
update t1 set i=2 where i=1;
select * from t1;
select * from t2;
select * from t3;
select * from t4;
delete from t2 where i=2;
drop table t4;
drop table t3;
drop table t2;
drop table t1;

select "multi-level cascades - part 2";
create table t1(i int unique, j int unique)$$
insert into t1 values(1,1);
create table t2(i int unique, j int unique, foreign key (i) references t1(i) on update cascade on delete cascade)$$
insert into t2 values(1,1);
alter table t1 add constraint "fk" foreign key (j) references t2(j) on update cascade on delete cascade$$
update t1 set i=2 where i=1;
select * from t1;
select * from t2;
drop table t2;
drop table t1;
alter table t1 drop constraint "fk"$$
drop table t2;
drop table t1;

select "multi-level cascades (cycle) - part 3";
create table t1(i int unique, j int unique)$$
insert into t1 values(1,1);
create table t2(i int unique, j int unique, constraint "t2_fk_1" foreign key (i) references t1(i) on update cascade on delete cascade, constraint "t2_fk_2" foreign key (j) references t2(i) on update cascade on delete cascade)$$
insert into t2 values(1,1);
alter table t1 add constraint "t1_fk_1" foreign key (i) references t1(j) on update cascade on delete cascade$$
alter table t1 add constraint "t1_fk_2" foreign key (j) references t2(j) on update cascade on delete cascade$$
update t1 set i=2 where i=1;
select * from t1;
select * from t2;
alter table t1 drop constraint "t1_fk_1"$$
alter table t1 drop constraint "t1_fk_2"$$
alter table t2 drop constraint "t2_fk_1"$$
alter table t2 drop constraint "t2_fk_2"$$
drop table t2;
drop table t1;

select "on delete set null";
create table t1(i int unique, j int)$$
create table t2(i int unique, j int, foreign key (i) references t1(i) on delete set null)$$
insert into t1 values(1,1);
insert into t2 values(1,2);
delete from t1 where i = 1;
select * from t1 order by 1;
select * from t2 order by 1;
drop table t2;
drop table t1;
