#!/bin/bash

cdb2sql="${CDB2SQL_EXE} -tabs -s ${CDB2_OPTIONS} ${DBNAME} default"

(
$cdb2sql <<'EOF'
DROP TABLE IF EXISTS nt;
CREATE TABLE nt(i int, j int, k int)$$
CREATE DEFAULT LUA CONSUMER batch_consume FOR (TABLE nt ON INSERT INCLUDE i,j);
EOF

echo "=== Starting consumer (listening for events) ==="
$cdb2sql \
  "EXEC PROCEDURE batch_consume('{\"batch_consume\": true, \"with_txn_sentinel\": true, \"with_id\": false}')" &
CONSUMER_PID=$!


sleep 2


$cdb2sql \
  "INSERT INTO nt(i,j,k) SELECT value, value+100, value+200 FROM generate_series(1,10)"

sleep 2


kill $CONSUMER_PID

$cdb2sql <<'EOF'
DROP LUA CONSUMER batch_consume;
DROP TABLE nt;
EOF
) > t08.output
diff -q t08.output t08.expected
if [[ $? -ne 0 ]]; then
    diff t08.output t08.expected | head -10
    exit 1
fi
echo "passed t08"
exit 0


