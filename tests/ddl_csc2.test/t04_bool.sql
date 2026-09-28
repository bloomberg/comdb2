DROP TABLE IF EXISTS t6;

CREATE TABLE t6 {
    schema {
        bool b
        bool d dbstore=7 null=yes
    }
    keys {
        dup "B" = b
    }
};$$

SELECT sql FROM sqlite_master WHERE name = 't6';
SELECT columnname, type, size, defaultvalue FROM comdb2_columns WHERE tablename = 't6';

INSERT INTO t6(b) VALUES (-1), (0), (1), (10), (2147483647);
SELECT b, d FROM t6 ORDER BY b;
SELECT b FROM t6 WHERE b = 10;

DROP TABLE t6;
