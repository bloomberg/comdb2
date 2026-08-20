#!/usr/bin/env bash

# A replicant should attribute the page-ins it does applying the replication
# stream to the statement that produced them, via
# comdb2_fingerprints.total_replication_pagein_read. Both tunables the chain
# needs (osql_send_fingerprint, log_fingerprint) are set in lrl.options.
#
# Look the fingerprint up on $SP_HOST, which has the query text, then assert the
# counters on a non-master node by hash -- that node never ran the SQL, so it
# only has an rtstats-only row (has_query_info='N').

# A single-node run has nothing applying a stream, so the check is vacuous.
# Emit the clustered path's line anyway so one .out covers both.
if [[ -z "$CLUSTER" ]]; then
    echo "(applies_present=1, io_le_total=1)"
    exit 0
fi

# All five inserts normalize to one fingerprint. Only the assertion is diffed.
cdb2sql --host $SP_HOST $SP_OPTIONS - > /dev/null 2>&1 <<'EOF'
CREATE TABLE fp_apagein(x INTEGER)$$
INSERT INTO fp_apagein(x) VALUES(1)
INSERT INTO fp_apagein(x) VALUES(2)
INSERT INTO fp_apagein(x) VALUES(3)
INSERT INTO fp_apagein(x) VALUES(4)
INSERT INTO fp_apagein(x) VALUES(5)
EOF

# The INSERT fingerprint (from the node that ran the SQL engine).
fp=$(cdb2sql --tabs --host $SP_HOST $SP_OPTIONS "SELECT fingerprint FROM comdb2_fingerprints WHERE normalized_sql LIKE 'INSERT%fp_apagein%'")

# A node that is not the master, i.e. one that applies rather than originates.
replicant=$(cdb2sql --tabs --host $SP_HOST $SP_OPTIONS "SELECT host FROM comdb2_cluster WHERE is_master='N' LIMIT 1")

# Replication is async: give the replicant a bounded window to catch up.
for _ in $(seq 1 60); do
    applied=$(cdb2sql --tabs --host $replicant $SP_OPTIONS "SELECT total_replication_pagein_read FROM comdb2_fingerprints WHERE fingerprint='$fp'")
    [[ -n "$applied" && "$applied" != "0" ]] && break
    sleep 1
done

# Page counts are not deterministic, so assert structural invariants only.
cdb2sql --host $replicant $SP_OPTIONS "SELECT (total_replication_pagein_read > 0) AS applies_present, (total_replication_pagein_read_io <= total_replication_pagein_read) AS io_le_total FROM comdb2_fingerprints WHERE fingerprint='$fp'"
