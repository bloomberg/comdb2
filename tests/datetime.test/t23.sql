set timezone US/Eastern
create table t(b blob, date datetime)$$
insert into t (b, date) values (x'600dcafe', cast("2007-10-01" as datetime))
insert into t (b, date) values (x'', cast("2007-10-01" as datetime))
select "datetime column less than blob"
select * from t where date < x'600dcafe'
select "datetime column equal to blob"
select * from t where date = x'600dcafe'
select "datetime column equal to empty blob"
select * from t where date = x''
select "datetime column greater than blob"
select * from t where date > x'600dcafe'
select "blob column less than datetime"
select * from t where b < NOW()
select "blob column equal to datetime"
select * from t where b = NOW()
select "blob column greater than datetime"
select * from t where b > NOW()
drop table t
