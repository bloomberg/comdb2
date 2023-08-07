set timezone US/Eastern
insert into t1 (id, ymint) values (1, 500)
insert into t1 (id, ymint) values (2, 1000)
insert into t1 (id, ymint) values (3, 100000)
insert into t1 (id, ymint, dsint) values (4, 1, 100000)
insert into t1 (id, ymint, dsint) values (5, 10, 100000)
insert into t1 (id, ymint, dsint) values (6, 100, 100000)
insert into t1 (id, ymint, dsint) values (7, 1000, 100000)
insert into t1 (id, ymint, dsint) values (8, -19, -100000)
insert into t1 (id, ymint, dsint) values (9, -30, -100001)
select * from t1 where ymint < dsint order by ymint
select * from t1 where ymint > dsint order by ymint
select cast (dsint as REAL) from t1 order by dsint
select cast (dsint as INT) from t1 order by dsint
select cast (dsint as TEXT) from t1 order by dsint
select cast (ymint as REAL) from t1 order by ymint
select cast (ymint as INT) from t1 order by ymint
select cast (ymint as TEXT) from t1 order by ymint
