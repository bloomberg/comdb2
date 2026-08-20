#!/usr/bin/env bash

# The master should attribute write-apply page-ins to the originating
# statement's fingerprint, via comdb2_fingerprints.total_write_pagein_read
# (osql_send_fingerprint is set in lrl.options).
#
# Writes are applied on the master, which may not be $SP_HOST and has no query
# text, so look the fingerprint up on $SP_HOST and assert on the master by hash.

# All five inserts normalize to one fingerprint. Only the assertion is diffed.
cdb2sql --host $SP_HOST $SP_OPTIONS - > /dev/null 2>&1 <<'EOF'
CREATE TABLE fp_wpagein(x INTEGER)$$
INSERT INTO fp_wpagein(x) VALUES(1)
INSERT INTO fp_wpagein(x) VALUES(2)
INSERT INTO fp_wpagein(x) VALUES(3)
INSERT INTO fp_wpagein(x) VALUES(4)
INSERT INTO fp_wpagein(x) VALUES(5)
EOF

# The INSERT fingerprint (from the node that ran the SQL engine).
fp=$(cdb2sql --tabs --host $SP_HOST $SP_OPTIONS "SELECT fingerprint FROM comdb2_fingerprints WHERE normalized_sql LIKE 'INSERT%fp_wpagein%'")

# The master (where the writes were actually applied).
master=$(cdb2sql --tabs --host $SP_HOST $SP_OPTIONS "SELECT host FROM comdb2_cluster WHERE is_master='Y'")

# Page counts are not deterministic, so assert structural invariants only.
cdb2sql --host $master $SP_OPTIONS "SELECT (total_write_pagein_read > 0) AS writes_present, (total_write_pagein_read_io <= total_write_pagein_read) AS io_le_total FROM comdb2_fingerprints WHERE fingerprint='$fp'"
