create table t {
    schema {
        int a
        int b
        int c
    }
    keys {
        datacopy(b) "a" = a
    }
};$$


select * from comdb2_keys where tablename='t';
select * from comdb2_partial_datacopies where tablename='t';

insert into t values (1,1,1);
insert into t values (1,2,1);

drop table t;
