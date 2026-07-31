#!/bin/bash
#
# One-shot cluster initializer. Creates the example database once and seeds
# every node's data directory with a copy, so the cluster forms with no manual
# steps. Re-running is a no-op if the database already exists.
set -euo pipefail

DB=testdb
NODES="node1 node2 node3"

if [ -f "/nodes/node1/${DB}.lrl" ]; then
    echo "[init] ${DB} already initialized; nothing to do"
    exit 0
fi

echo "[init] creating ${DB} in node1's data directory"
# Create the database files directly in node1's data directory. comdb2 --create
# writes the data files into --dir.
mkdir -p /nodes/node1
comdb2 --create --dir /nodes/node1 "$DB"

# The lrl every node runs with: data lives at /db on each node, plus the
# cluster membership line so the nodes find each other and elect a master.
cat > "/tmp/${DB}.lrl" <<EOF
name    ${DB}
dir     /db
cluster nodes ${NODES}
EOF

echo "[init] seeding node data directories"
for n in ${NODES}; do
    dest="/nodes/$n"
    mkdir -p "$dest"
    # Copy the freshly created (never-started) database files to each peer.
    [ "$n" = node1 ] || cp -a /nodes/node1/. "$dest/"
    cp "/tmp/${DB}.lrl" "$dest/${DB}.lrl"
done

echo "[init] ${DB} initialized on: ${NODES}"
