drop table if exists a
drop table if exists b

create table a(i int, j int, k int)$$
create unique index a_ij on a(i, j)
create unique index a_k on a(k)

insert into a(i, j, k) values(1, 1, 1)
insert into a(i, j, k) values(2, 1, 1) ON CONFLICT (i, j) do update set k = 1 -- should be dup error

create table b(i int)$$
create unique index b_i on b(i)
insert into b values(1)

alter table a add foreign key (i) references b(i)$$
insert into a(i, j, k) values(2, 1, 1) ON CONFLICT (i, j) do update set k = 1 -- should be either fk error or dup error
insert into a(i, j, k) values(2, 1, 0) ON CONFLICT (i, j) do update set k = 1 -- should be fk error

insert into b values(2)
insert into a(i, j, k) values(2, 1, 1) ON CONFLICT (i, j) do update set k = 1 -- should be dup error

SELECT 1 FROM comdb2_metrics m, comdb2_tunables t WHERE m.name LIKE 'verify_replays' AND t.name='osql_verify_retry_max' AND m.value >= CAST(t.value AS int)
