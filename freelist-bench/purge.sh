#!/opt/homebrew/bin/bash
# Usage: purge.sh <dir>.  Inserts run while two purge jobs free pages:
# one deletes the oldest rows (frees data pages at the left edge), one
# deletes ranges of the R1 index (frees index pages).
D=$1
B=/Users/mponomarenko/comdb2/multi/build
writers=${BENCH_WRITERS:-16}
txns=${BENCH_TXNS:-20}
rows=${BENCH_ROWS:-200}
rounds=${BENCH_ROUNDS:-3}
sql() { $B/tools/cdb2sql/cdb2sql -c $D/cdb2.cfg --tabs benchdb local "$@"; }
die() { echo "FAIL: $*"; exit 1; }
now() { python3 -c 'import time; print(time.time())'; }

for n in 1 8; do
    sql "drop table if exists p$n" > /dev/null
    sql "put tunable 'freelist_meta_pages' $n" > /dev/null || die "tunable $n"
    sql "create table p$n { schema { int a int r1 int r2 } keys { dup \"A\" = a dup \"R1\" = r1 dup \"R2\" = r2 } }" > /dev/null ||
        die "create p$n"
done

insert() {
    sql "insert into $1 select $2, abs(random() % 1000000000), abs(random() % 1000000000) from generate_series(1, $rows)" \
        > /dev/null 2>> $D/purge.err
}

total=$((writers * txns * rows))
echo "writers=$writers txns/writer=$txns rows/txn=$rows rows/round=$total, 2 purge jobs"
seq=0
for ((r = 1; r <= rounds; r++)); do
    for n in 1 8; do
        tbl=p$n
        # Preload one round of rows, so that the purge jobs have work.
        for ((i = 0; i < writers * txns; i++)); do insert $tbl $((seq)) & ((i % 16 == 15)) && wait; done; wait
        lo=$seq
        seq=$((seq + 1))
        start=$(now); pids=()
        for ((w = 0; w < writers; w++)); do
            (for ((i = 0; i < txns; i++)); do insert $tbl $seq || exit 1; done) &
            pids+=($!)
        done
        # Purge 1: oldest rows by A, in transactions of $rows rows.
        (while :; do sql "delete from $tbl where a <= $lo limit $rows" | grep -q "rows deleted=0" && break; done) > /dev/null 2>&1 &
        p1=$!
        # Purge 2: ranges of R1.
        (for ((k = 0; k < 1000; k++)); do sql "delete from $tbl where r1 >= $((k * 1000000)) and r1 < $(((k + 1) * 1000000)) and a > $lo"; done) > /dev/null 2>&1 &
        p2=$!
        failed=0
        for pid in "${pids[@]}"; do wait $pid || failed=1; done
        end=$(now)
        kill $p1 $p2 2> /dev/null; wait $p1 $p2 2> /dev/null
        [[ $failed == 0 ]] || die "writer failed on $tbl, see $D/purge.err"
        left=$(sql "select count(*) from $tbl where a <= $lo")
        python3 -c "s = $end - $start; print('round $r meta_pages=$n insert_seconds=%.2f rows/sec=%d old_rows_left=$left' % (s, $total / s))"
        seq=$((seq + 1))
    done
done
