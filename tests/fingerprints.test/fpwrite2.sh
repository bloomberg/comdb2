#!/usr/bin/env bash

# Same check as fpwrite.sh, for the isolation levels that buffer writes in
# shadow tables and drain them at commit: read committed, snapshot isolation,
# and serial (enabled via enable_serial_isolation in lrl.options).
#
# Each level gets its own table so its UPDATE and DELETE have fingerprints of
# their own. Only those two are asserted -- the INSERT shares a fingerprint with
# the pre-inserts, which ran under SOSQL. The rows must pre-exist: a row
# inserted and deleted in one transaction cancels in the shadow table.

master=$(cdb2sql --tabs --host $SP_HOST $SP_OPTIONS "SELECT host FROM comdb2_cluster WHERE is_master='Y'")

check_level() {
    local level="$1" tbl="$2"

    cdb2sql --host $SP_HOST $SP_OPTIONS - > /dev/null 2>&1 <<EOF
CREATE TABLE $tbl(x INTEGER)\$\$
INSERT INTO $tbl(x) VALUES(1)
INSERT INTO $tbl(x) VALUES(2)
INSERT INTO $tbl(x) VALUES(3)
set transaction $level
begin
UPDATE $tbl SET x=x+10 WHERE x=1
DELETE FROM $tbl WHERE x=2
INSERT INTO $tbl(x) VALUES(4)
commit
EOF

    local ufp dfp
    ufp=$(cdb2sql --tabs --host $SP_HOST $SP_OPTIONS "SELECT fingerprint FROM comdb2_fingerprints WHERE normalized_sql LIKE 'UPDATE%$tbl%'")
    dfp=$(cdb2sql --tabs --host $SP_HOST $SP_OPTIONS "SELECT fingerprint FROM comdb2_fingerprints WHERE normalized_sql LIKE 'DELETE%$tbl%'")

    cdb2sql --host $master $SP_OPTIONS "SELECT '$level' AS level, 'update' AS op, (total_write_pagein_read > 0) AS writes_present, (total_write_pagein_read_io <= total_write_pagein_read) AS io_le_total FROM comdb2_fingerprints WHERE fingerprint='$ufp'"
    cdb2sql --host $master $SP_OPTIONS "SELECT '$level' AS level, 'delete' AS op, (total_write_pagein_read > 0) AS writes_present, (total_write_pagein_read_io <= total_write_pagein_read) AS io_le_total FROM comdb2_fingerprints WHERE fingerprint='$dfp'"
}

check_level "read committed" fp_iso_recom
check_level "snapshot isolation" fp_iso_snap
check_level "serial" fp_iso_serial
