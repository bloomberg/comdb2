create table t10 {
schema
{
    int i
    datetime     dt dbstore="123456789012"
}
}$$
insert into t10(i) values(1),(2),(3)
select * from t10
