#!/opt/homebrew/bin/bash
# Usage: bench.sh <dir>.  Database benchdb must be up.
D=$1
B=/Users/mponomarenko/comdb2/multi/build
writers=${BENCH_WRITERS:-16}
txns=${BENCH_TXNS:-10}
rows=${BENCH_ROWS:-1000}
rounds=${BENCH_ROUNDS:-3}
sql() { $B/tools/cdb2sql/cdb2sql -c $D/cdb2.cfg --tabs benchdb local "$@"; }
die() { echo "FAIL: $*"; exit 1; }
now() { python3 -c 'import time; print(time.time())'; }

# Smoke test.
sql "put tunable 'freelist_meta_pages' 8" > /dev/null || die tunable
sql "drop table if exists smoke" > /dev/null; sql "drop table if exists t1" > /dev/null; sql "drop table if exists t8" > /dev/null
sql 'create table smoke { schema { int a cstring s[32] } keys { "A" = a } }' > /dev/null || die "create smoke"
for ((i = 1; i <= 1000; i++)); do echo "insert into smoke values ($i, 'row $i')"; done |
    $B/tools/cdb2sql/cdb2sql -c $D/cdb2.cfg -s benchdb local - > /dev/null || die "insert smoke"
[[ "$(sql "select count(*), sum(a) from smoke")" == "1000	500500" ]] || die "smoke count"
[[ "$(sql "select s from smoke where a = 777")" == "row 777" ]] || die "smoke read"
echo "smoke test passed"

for n in 1 8; do
    sql "put tunable 'freelist_meta_pages' $n" > /dev/null || die "tunable $n"
    sql "create table t$n { schema { int a int r1 int r2 } keys { dup \"R1\" = r1 dup \"R2\" = r2 } }" > /dev/null ||
        die "create t$n"
done

writer() {
    local tbl=$1 i j
    for ((i = 0; i < txns; i++)); do
        # One statement per transaction, so that the master apply, not the
        # client, is the bottleneck.
        $B/tools/cdb2sql/cdb2sql -c $D/cdb2.cfg -s benchdb local \
            "insert into $tbl select $i, abs(random() % 1000000000), abs(random() % 1000000000) from generate_series(1, $rows)" \
            > /dev/null 2>> $D/writer.err || return 1
    done
}

sample_waits() {
    while :; do
        sql "select object, page from comdb2_locks where status = 'WAIT' and locktype = 'PAGE'" 2> /dev/null
        sleep 0.1
    done
}

total=$((writers * txns * rows))
echo "writers=$writers txns/writer=$txns rows/txn=$rows rows/round=$total"
for ((r = 1; r <= rounds; r++)); do
    for n in 1 8; do
        sample_waits > $D/waits.$n.$r &
        spid=$!
        start=$(now); pids=()
        for ((w = 0; w < writers; w++)); do writer t$n & pids+=($!); done
        failed=0
        for pid in "${pids[@]}"; do wait $pid || failed=1; done
        end=$(now)
        kill $spid; wait $spid 2> /dev/null
        [[ $failed == 0 ]] || die "writer failed on t$n, see $D/writer.err"
        # Meta pages: 0 always, 2..n when n > 1 (page 1 is the root).
        meta=$(awk -v n=$n '$2 == 0 || ($2 >= 2 && $2 <= n)' $D/waits.$n.$r | wc -l)
        all=$(wc -l < $D/waits.$n.$r)
        python3 -c "s = $end - $start; print('round $r meta_pages=$n seconds=%.2f rows/sec=%d page_waits: meta=$meta all=$all' % (s, $total / s))"
    done
done
for n in 1 8; do
    [[ "$(sql "select count(*) from t$n")" == $((rounds * total)) ]] || die "count t$n"
done
echo "counts ok"
