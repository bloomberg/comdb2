create table t {
    schema {
        int a
        int b
        int c
        int d
    }
    keys {
        datacopy(e) "a" = a
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
        datacopy(b, e, c) "a" = a
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
        datacopy(b, e, c, a, f, g) "a" = a
    }
};$$
