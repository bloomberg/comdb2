create table t7 { 
schema
{
    int i
    datetime     dt dbstore="CURRENT_TIMESTAMP"
}
}$$

insert into t7(i) values(2)
insert into t7(i) values(1)

select i from t7 order by dt
