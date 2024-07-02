set timezone UTC
insert into t1 (id, date) values (1, cast("2007-10-01" as datetime))
insert into t1 (id, date) values (2, cast("2007-10-02" as datetime))
insert into t1 (id, date) values (3, cast("2007-10-02" as datetime))
insert into t1 (id, date) values (4, cast("2007-10-02" as datetime))
select count(*) from t1 where date='2007-10-02' union all select count(*) from t1 where date='2007-10-02'
set timezone US/Eastern
select count(*) from t1 where date='2007-10-02' union all select count(*) from t1 where date='2007-10-02'
