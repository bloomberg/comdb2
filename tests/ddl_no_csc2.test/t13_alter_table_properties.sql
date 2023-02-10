DROP TABLE IF EXISTS t1;

CREATE TABLE t1(i INT) $$
SELECT * FROM comdb2_table_properties WHERE table_name = 't1';

ALTER TABLE t1 ALTER OPTIONS (ODH OFF)$$
SELECT * FROM comdb2_table_properties WHERE table_name = 't1';

ALTER TABLE t1 ALTER OPTIONS (ODH ON, IPU OFF)$$
SELECT * FROM comdb2_table_properties WHERE table_name = 't1';

ALTER TABLE t1 ALTER OPTIONS (IPU ON, ISC OFF)$$
SELECT * FROM comdb2_table_properties WHERE table_name = 't1';

ALTER TABLE t1 ALTER OPTIONS (ODH OFF, IPU OFF, ISC OFF)$$
SELECT * FROM comdb2_table_properties WHERE table_name = 't1';

ALTER TABLE t1 ALTER OPTIONS (ODH ON, IPU ON, ISC ON)$$
SELECT * FROM comdb2_table_properties WHERE table_name = 't1';

ALTER TABLE t1 ALTER OPTIONS (BLOBFIELD RLE)$$
SELECT * FROM comdb2_table_properties WHERE table_name = 't1';

ALTER TABLE t1 ALTER OPTIONS (REC RLE)$$
SELECT * FROM comdb2_table_properties WHERE table_name = 't1';

ALTER TABLE t1 ALTER OPTIONS (REC ZLIB, BLOBFIELD CRLE)$$
SELECT * FROM comdb2_table_properties WHERE table_name = 't1';

ALTER TABLE t1 ALTER OPTIONS (REC NONE, BLOBFIELD NONE)$$
SELECT * FROM comdb2_table_properties WHERE table_name = 't1';

ALTER TABLE t1 ALTER OPTIONS (PAGEORDER, READONLY)$$
SELECT * FROM comdb2_table_properties WHERE table_name = 't1';

ALTER TABLE t1 ALTER OPTIONS (BLOBFIELD NONE, REC NONE, ODH OFF, IPU OFF, ISC OFF)$$
SELECT * FROM comdb2_table_properties WHERE table_name = 't1';

ALTER TABLE t1 ALTER OPTIONS ODH ON$$
ALTER TABLE t1 ALTER OPTIONS (ODH ON, ODH OFF) $$
ALTER TABLE t1 ALTER OPTIONS (BLOBFIELD LZ4, BLOBFIELD ZLIB)$$

SELECT * FROM comdb2_table_properties WHERE table_name = 't1';

DROP TABLE t1;
