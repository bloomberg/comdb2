select '-- Test#1: Ensure ALTER ignores IX_EMPTY error --' as test;
create table t1 {
tag ondisk{
    int id
}

keys{
        "idx1"       =   id {WHERE id IN (0)}
}
}$$

create table t2 {
tag ondisk{
    int id
}

keys{
    dup "idx1" = id
}

constraints{        
    "idx1" ->  <"t1":"idx1">
}
}$$

insert into t1 (id) values (1);

alter table t1 add column i int$$

select * from t1 order by 1;
select * from t2 order by 1;

drop table t2;
drop table t1;

select '-- Test#2 : Test to ensure ALTER fails if we change partial index in the referenced key --' as test;

create table t1 {
    tag ondisk{
        int id
    }

    keys{
        "idx1" = id {WHERE id IN (0,1)}
    }
}$$

create table t2 {
    tag ondisk{
        int id
    }

    keys{
        dup "idx1" = id
    }

    constraints{        
        "idx1" ->  <"t1":"idx1">
    }
}$$

insert into t1 (id) values (1);
insert into t2 (id) values (1);

alter table t1 {
    tag ondisk{
        int id
    }

    keys{
        "idx1" = id {WHERE id IN (0)}
    }
}$$

select * from t1 order by 1;
select * from t2 order by 1;

drop table t2;
drop table t1;

select '-- Test#3: Ensure ALTER ignores IX_PASTEOF error --' as test;

create table t1 {
    tag ondisk{
        int id
    }

    keys{
        "idx1" = id {WHERE id IN (0,1)}
    }
}$$

create table t2 {
    tag ondisk{
        int id
    }

    keys{
        dup "idx1" = id
    }

    constraints{        
        "idx1" ->  <"t1":"idx1">
    }
}$$

insert into t1 (id) values (0);
insert into t1 (id) values (1);
insert into t2 (id) values (0);

alter table t1 {
    tag ondisk{
        int id
    }

    keys{
        "idx1" = id {WHERE id IN (0)}
    }
}$$

select * from t1 order by 1;
select * from t2 order by 1;

drop table t2;
drop table t1;

select '-- Test#4: Ensure ALTER ignores IX_NOTFND error --' as test;

create table t1 {
    tag ondisk{
        int id
    }

    keys{
        "idx1" = id {WHERE id IN (0,1,2)}
    }
}$$

create table t2 {
    tag ondisk{
        int id
    }

    keys{
        dup "idx1" = id
    }

    constraints{        
        "idx1" ->  <"t1":"idx1">
    }
}$$

insert into t1 (id) values (0);
insert into t1 (id) values (1);
insert into t1 (id) values (2);
insert into t2 (id) values (0);
insert into t2 (id) values (2);

alter table t1 {
    tag ondisk{
        int id
    }

    keys{
        "idx1" = id {WHERE id IN (0,2)}
    }
}$$

select * from t1 order by 1;
select * from t2 order by 1;

drop table t2;
drop table t1;
