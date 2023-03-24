create table t {
    schema {
        int a
        int b
        int c
        int d
    }
    keys {
        datacopy(b, b, c, c, b, d) "a" = a
    }
};$$
