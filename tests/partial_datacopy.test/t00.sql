create table t {
    schema {
        int a
        int b
        int c
        int d
    }
    keys {
        datacopy datacopy(b, c) "a" = a
    }
};$$

create table t {
    schema {
        int a
        int b
        int c
        int d
    }
    keys {
        datacopy(b, c) datacopy "a" = a
    }
};$$

create table t {
    schema {
        int a
        int b
        int c
        int d
    }
    keys {
        datacopy(b, c) datacopy(d) "a" = a
    }
};$$
