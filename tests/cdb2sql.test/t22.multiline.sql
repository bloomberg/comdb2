-- Test incomplete statements with the 'over' keyword
create table t1(i int);
insert into t1 values(1), (2), (2), (3), (3), (3);
select count(i) over;
select count(i) over (;)
select count(i) over (part;
select count(i) over (partition b;
select count(i) over (partition by ;)
select count(i) over (partition by i) fr;
select count(i) over (partition by i) from t1;
