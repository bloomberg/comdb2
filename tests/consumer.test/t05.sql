create table t(i int)$$
create procedure len_is_28___________________ version 'sample' {
        local function main()
        end
}$$
create procedure len_is_29____________________ version 'sample' {
        local function main()
        end
}$$
create procedure len_is_31______________________ version 'sample' {
        local function main()
        end
}$$
create procedure len_is_32_______________________ version 'sample' {
        local function main()
        end
}$$
create lua consumer len_is_28___________________ on (table t for update of i)
create lua consumer len_is_29____________________ on (table t for update of i)
select * from comdb2_triggers where name like 'len_is_%'
