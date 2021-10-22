create table t {
    schema {
        int a
        int b
        int c
        int d
    }
    keys {
        datacopy(b, c) "a" = a
    }
};$$

set transaction read committed;
begin;
insert into t with recursive r(n) as (select 1 union all select n + 4 from r where n < 97) select n, n + 1, n + 2, n + 3 from r;
select * from t where a < 40;
select a, b, c from t where a < 40;
commit;

select * from t where a < 40;
select a, b, c from t where a < 40;

drop table t;
