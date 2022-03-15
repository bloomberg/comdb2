CREATE TABLE t(a INT, b INT, c INT, d INT); $$
CREATE INDEX a ON t(a) INCLUDE (b, c);

select * from comdb2_keys where tablename='t';
select * from comdb2_partial_datacopies where tablename='t';

insert into t with recursive r(n) as (select 1 union all select n + 4 from r where n < 97) select n, n + 1, n + 2, n + 3 from r;
select * from t where a < 40;
select a, b, c from t where a < 40;

drop table t;
