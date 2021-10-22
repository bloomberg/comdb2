create table t {
    schema {
        int a
        int b
        int c
        int d
    }
    keys {
        datacopy() "a" = a
    }
};$$
