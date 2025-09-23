set timezone US/Eastern
create table t(d datetime)$$
insert into t (d) values (cast("2007-10-01T" as datetime))
insert into t (d) values (cast("2025-10-01T" as datetime))
select case when d <= '2020-10-01T' then "2020 and change" else d end as mixes from t group by mixes
drop table t
