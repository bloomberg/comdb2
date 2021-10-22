create table t {
    schema {
        int a
        vutf8 b[6]
        int c
        int d
    }
    keys {
        datacopy(b, c) "a" = a
    }
};$$


select * from comdb2_keys where tablename='t';
select * from comdb2_partial_datacopies where tablename='t';

insert into t with recursive r(n) as (select 1 union all select n + 4 from r where n < 97) select n, "cdb" || (n + 1), n + 2, n + 3 from r;
select * from t where a < 40;
select a, b, c from t where a < 40;

alter table t {
    schema {
        int a
        vutf8 b[4]
        int c
        int d
    }
    keys {
        datacopy(b, c) "a" = a
    }
};$$

select * from comdb2_keys where tablename='t';
select * from comdb2_partial_datacopies where tablename='t';

select * from t where a < 40;
select a, b, c from t where a < 40;

drop table t;
