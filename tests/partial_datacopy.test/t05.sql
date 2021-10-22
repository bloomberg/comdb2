create table t {
    schema {
        int a
        int b
        int c
        int d
    }
    keys {
        datacopy(a) "a" = a
    }
};$$

create table t2 {
    schema {
        int a
        int b
        int c
        int d
    }
    keys {
        datacopy(a, b, c, d) "abcd" = a + b + c + d
    }
};$$

select * from comdb2_keys where tablename='t';
select * from comdb2_partial_datacopies where tablename='t';

select * from comdb2_keys where tablename='t2';
select * from comdb2_partial_datacopies where tablename='t2';

insert into t with recursive r(n) as (select 1 union all select n + 4 from r where n < 97) select n, n + 1, n + 2, n + 3 from r;
select * from t where a < 40;
select a from t where a < 40;

insert into t2 with recursive r(n) as (select 1 union all select n + 4 from r where n < 97) select n, n + 1, n + 2, n + 3 from r;
select * from t2 where a < 40;

drop table t;
drop table t2;
