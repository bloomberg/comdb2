#!/opt/homebrew/bin/bash
# Usage: ten.sh <dir>.  Database benchdb must be up.
# Table with 10 random-key indexes, single-row single-statement
# transactions.  Each writer keeps one connection and streams its
# inserts in autocommit, so each insert is one transaction.
D=$1
B=/Users/mponomarenko/comdb2/multi/build
writers=${BENCH_WRITERS:-32}
stmts=${BENCH_STMTS:-1000}
rows=${BENCH_ROWS:-1}
rounds=${BENCH_ROUNDS:-3}
sql() { $B/tools/cdb2sql/cdb2sql -c $D/cdb2.cfg --tabs benchdb local "$@"; }
die() { echo "FAIL: $*"; exit 1; }
now() { python3 -c 'import time; print(time.time())'; }

cols=""; keys=""; vals=""
for ((k = 0; k < 10; k++)); do
    cols+="int c$k "
    keys+="dup \"K$k\" = c$k "
    vals+="${vals:+, }abs(random() % 1000000000)"
done
for n in 1 8; do
    sql "drop table if exists x$n" > /dev/null
    sql "put tunable 'freelist_meta_pages' $n" > /dev/null || die "tunable $n"
    sql "create table x$n { schema { $cols } keys { $keys } }" > /dev/null || die "create x$n"
done

if ((rows == 1)); then
    stmt() { echo "insert into $1 values ($vals)"; }
else
    stmt() { echo "insert into $1 select $vals from generate_series(1, $rows)"; }
fi

writer() {
    local tbl=$1 i
    for ((i = 0; i < stmts; i++)); do stmt $tbl; done |
        $B/tools/cdb2sql/cdb2sql -c $D/cdb2.cfg -s benchdb local - > /dev/null 2>> $D/ten.err
    # cdb2sql exits 0 after a failed statement in script mode; count errors.
}

sample_waits() {
    while :; do
        sql "select object, page from comdb2_locks where status = 'WAIT' and locktype = 'PAGE'" 2> /dev/null
        sleep 0.1
    done
}

total=$((writers * stmts * rows))
echo "writers=$writers txns/writer=$stmts rows/txn=$rows rows/round=$total, 10 random indexes"
: > $D/ten.err
for ((r = 1; r <= rounds; r++)); do
    for n in 1 8; do
        sample_waits > $D/tenwaits.$n.$r &
        spid=$!
        start=$(now); pids=()
        for ((w = 0; w < writers; w++)); do writer x$n & pids+=($!); done
        for pid in "${pids[@]}"; do wait $pid; done
        end=$(now)
        kill $spid; wait $spid 2> /dev/null
        errs=$(grep -c . $D/ten.err)
        # Meta pages: 0 always, 2..n when n > 1 (page 1 is the root).
        meta=$(awk -v n=$n '$2 == 0 || ($2 >= 2 && $2 <= n)' $D/tenwaits.$n.$r | wc -l)
        all=$(wc -l < $D/tenwaits.$n.$r)
        python3 -c "s = $end - $start; print('round $r meta_pages=$n seconds=%.2f txns/sec=%d page_waits: meta=$meta all=$all errors=$errs' % (s, $writers * $stmts / s))"
    done
done
for n in 1 8; do
    echo "x$n rows: $(sql "select count(*) from x$n")"
done
