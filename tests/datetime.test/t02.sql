set timezone US/Eastern
insert into t1 (id, date) values (1, cast("2007-10-01" as datetime))
insert into t1 (id, date) values (2, cast("2010-09-01" as datetime))
insert into t1 (id, date) values (3, cast("2001-12-31T23:59:59.999" as datetime))
insert into t1 (id, date) values (4, cast("2001-12-31T23:59:59.998" as datetime))
insert into t1 (id, date) values (5, cast("1912-02-29" as datetime))
insert into t1 (id, date) values (6, cast("1913-02-29" as datetime))
select * from t1 order by date
select * from t1 where date > cast ("2001-12-31T235959.998" as datetime) order by id
select * from t1 where date >= cast ("2001-12-31T235959.998" as datetime) order by date
set timezone GMT
select * from t1 order by date
