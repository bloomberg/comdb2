delete from t1 where 1
set transaction read committed
begin
insert into t1 (a,b) values (2,1)
insert into t1 (a,b) values (2,1)
insert into t1 (a,b) values (2,1)

insert into t1 (a,b) values (1,2)
insert into t1 (a,b) values (1,2)
insert into t1 (a,b) values (1,2)
insert into t1 (a,b) values (1,2)
insert into t1 (a,b) values (0,4)

select a,b from t1 where b = 5 order by a asc
select a,b from t1 where b = 5 order by a desc
select a,b from t1 where b = 4 order by a asc
select a,b from t1 where b = 4 order by a desc
select a,b from t1 where b = 3 order by a asc
select a,b from t1 where b = 3 order by a desc
select a,b from t1 where b = 2 order by a asc
select a,b from t1 where b = 2 order by a desc
select a,b from t1 where b = 1 order by a asc
select a,b from t1 where b = 1 order by a desc
select a,b from t1 where b = 0 order by a asc
select a,b from t1 where b = 0 order by a desc

select a,b from t1 where b < 5 order by a asc
select a,b from t1 where b < 5 order by a desc
select a,b from t1 where b < 4 order by a asc
select a,b from t1 where b < 4 order by a desc
select a,b from t1 where b < 3 order by a asc
select a,b from t1 where b < 3 order by a desc
select a,b from t1 where b < 2 order by a asc
select a,b from t1 where b < 2 order by a desc
select a,b from t1 where b < 1 order by a asc
select a,b from t1 where b < 1 order by a desc
select a,b from t1 where b < 0 order by a asc
select a,b from t1 where b < 0 order by a desc

select a,b from t1 where b > 5 order by a asc
select a,b from t1 where b > 5 order by a desc
select a,b from t1 where b > 4 order by a asc
select a,b from t1 where b > 4 order by a desc
select a,b from t1 where b > 3 order by a asc
select a,b from t1 where b > 3 order by a desc
select a,b from t1 where b > 2 order by a asc
select a,b from t1 where b > 2 order by a desc
select a,b from t1 where b > 1 order by a asc
select a,b from t1 where b > 1 order by a desc
select a,b from t1 where b > 0 order by a asc
select a,b from t1 where b > 0 order by a desc

select a,b from t1 where b <= 5 order by a asc
select a,b from t1 where b <= 5 order by a desc
select a,b from t1 where b <= 4 order by a asc
select a,b from t1 where b <= 4 order by a desc
select a,b from t1 where b <= 3 order by a asc
select a,b from t1 where b <= 3 order by a desc
select a,b from t1 where b <= 2 order by a asc
select a,b from t1 where b <= 2 order by a desc
select a,b from t1 where b <= 1 order by a asc
select a,b from t1 where b <= 1 order by a desc
select a,b from t1 where b <= 0 order by a asc
select a,b from t1 where b <= 0 order by a desc

select a,b from t1 where b >= 5 order by a asc
select a,b from t1 where b >= 5 order by a desc
select a,b from t1 where b >= 4 order by a asc
select a,b from t1 where b >= 4 order by a desc
select a,b from t1 where b >= 3 order by a asc
select a,b from t1 where b >= 3 order by a desc
select a,b from t1 where b >= 2 order by a asc
select a,b from t1 where b >= 2 order by a desc
select a,b from t1 where b >= 1 order by a asc
select a,b from t1 where b >= 1 order by a desc
select a,b from t1 where b >= 0 order by a asc
select a,b from t1 where b >= 0 order by a desc

rollback
