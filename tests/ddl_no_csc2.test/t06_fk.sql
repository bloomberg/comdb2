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
