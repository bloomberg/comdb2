DROP TABLE IF EXISTS t1;
CREATE TABLE t1 { tag ondisk { int i } keys { " WHITESPACE " = i } } $$
CREATE TABLE t1 { tag ondisk { int i } } $$
INSERT INTO t1 VALUES(1)
EXEC PROCEDURE sys.cmd.send("stat autoanalyze")
