DROP TABLE t1;
DROP TABLE t2;
CREATE TABLE t1(i INT PRIMARY KEY) $$
CREATE TABLE t2(i INT PRIMARY KEY, FOREIGN KEY (i) REFERENCES t1(i)) $$
@ls
@list
@ls tables
@list tables
@ls foo

@desc
@describe
@desc t1
@desc t2
@describe t2

DROP TABLE t2;
DROP TABLE t1;
