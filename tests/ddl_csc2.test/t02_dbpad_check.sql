create table t {
    schema {
        byte a[2]
    }
};$$

# not allowed: need dbpad to change size
alter table t {
    schema {
        byte a[4]
    }
};$$

# not allowed: need dbpad to change size
alter table t {
    schema {
        byte a[1]
    }
};$$

# not allowed: need dbpad to change size
alter table t {
    schema {
        byte a[4] dbstore=0
    }
};$$

# should pass
alter table t {
    schema {
        byte a[3] dbpad=0
    }
};$$

create table t2 {
    schema {
        byte a[2]
    }
};$$


# should pass
alter table t2 {
    schema {
        byte a[2]
        int b dbstore=0
    }
};$$

drop table t;
drop table t2;
