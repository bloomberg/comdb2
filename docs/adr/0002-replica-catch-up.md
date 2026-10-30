# Replica catch-up under heavy write load

## Status

Proposed — 2026-10-30

## Context

A replica that restarts while the master is busy has to replay everything the
master wrote while it was down before it can serve queries again. Under heavy
load it can take minutes, or never finish.

The test: an 8-node cluster, 1000 clients inserting single rows into their own
tables (about 19–20k inserts/s, `async_dist_commit` on), 128 MB cache. One
replica is stopped for 60 s and restarted; by then it is 1.5–2 GB of log behind
the master. We measure how long until the master marks it coherent again.
With the defaults:

| behind at restart | coherent again after |
|------------------:|----------------------|
|           1575 MB | 472 s                |
|           1533 MB | 427 s                |
|           1639 MB | not within 600 s; still 549 MB behind |

At the lower load of `async_dist_commit` off (about 14k inserts/s), a replica
1260 MB behind was coherent again after 74 s.

### Why it is slow

All replication traffic on a replica is handled by one receive thread. During
catch-up that thread was 78–88% busy, and more than half of its time went to
records that arrived out of order.

A restarted replica gets the log two ways at once:

1. **Gap fill.** It asks the master for the records it is missing, and the
   master sends them, in order. These requests are asynchronous: the replica
   sends one whenever it notices a gap, and can easily ask again for records
   it has already been sent.
2. **Live broadcast.** The master also sends it every new record as it is
   written, the same as to every other node.

The live records are gigabytes ahead of where the replica is, so each one is
early. The replica parks it in the repdb (an on-disk btree by default), and
reads it back and deletes it later. When the gap fill reaches the same record,
the replica gets it a second time and drops the copy. Parking costs more per
record than writing it in order, so at a high enough write rate the replica
falls behind faster than it catches up.

Records arrive early even on a healthy replica: the master's writer threads
send after they release the log lock, so two sends can overtake each other.
14–45% of records arrived early at steady load, and parking took about a third
of the receive thread.

### Settings tried

Each setting was run twice, interleaved with the others, 900 s per run.

| setting | behind at restart | coherent again after | MB/s closed |
|---------|------------------:|---------------------:|------------:|
| defaults | 1575 / 1533 / 1639 MB | 472 s / 427 s / not in 600 s | 3.3 / 3.6 / — |
| `logput_window 40000000` | 1615 / 1558 MB | 82 s / 75 s | 19.7 / 20.8 |
| `inmem_repdb 1`, `inmem_repdb_maxlog 100000` | 1948 / 1885 MB | 122 s / 115 s | 16.0 / 16.4 |
| `logput_window` + `inmem_repdb` | 1852 / 1740 MB | 93 s / 91 s | 19.9 / 19.1 |
| `logput_window` + `inmem_repdb`, `maxlog 1000` | 1776 / 1822 MB | 104 s / 82 s | 17.1 / 22.2 |
| `logput_window` + `sql_logfill 1` | 1450 / 1500 MB | 47 s / 48 s | 30.9 / 31.3 |
| `sql_logfill 1` | 1535 / 1521 MB | 284 s / 350 s | 5.4 / 4.3 |
| `sql_logfill` + `inmem_repdb` | 1746 / 1688 MB | 83 s / 111 s | 21.0 / 15.2 |
| `sql_logfill` + `inmem_repdb` + `inmem_repdb_most_recent` | 1702 / 1738 MB | 124 s / 128 s | 13.7 / 13.6 |
| `lkr_hash 1024` | 1608 / 1611 MB | 455 s / 509 s | 3.5 / 3.2 |

"MB/s closed" is the starting distance divided by the time; the master keeps
writing throughout, so the replica's real replay rate is higher.

- **`logput_window`** stops the live broadcast to an incoherent node more than
  this many bytes behind. That node then gets the log only from gap fill, in
  order, so nothing is parked and nothing arrives twice. Once it is within the
  window it gets live broadcasts again. It was added in 2018 and was on by
  default until 2020: from mid-2018 its default of 0 meant "throttle every
  incoherent node", stricter than today. The 2020 network rewrite dropped the
  call; it came back in 2022 with 0 meaning off.
- **`inmem_repdb`** parks early records in an in-memory list instead of the
  on-disk btree. It makes parking cheap, which lifts steady throughput, but
  with `logput_window` on there is almost nothing to park during catch-up, so
  it does not make catch-up faster. Its runs started further behind because
  the master was faster.
- **`sql_logfill`** (beta) replaces gap fill: when it is on, the replica sends
  no gap-fill requests, and a separate thread streams the log from the master
  over SQL and replays it itself. On its own the live broadcast still floods
  the receive thread; together with `logput_window` it is the fastest.
- **`lkr_hash`** speeds up lock-owner lookups everywhere, but the master gains
  as much as the replica, so catch-up does not improve.

Steady load with every node coherent (300 s each, same day):

| setting | inserts/s |
|---------|----------:|
| defaults | 18,904 |
| `inmem_repdb`, `maxlog 1000` | 23,762 |
| `inmem_repdb`, `maxlog 100000` | 23,719 |

The list size makes no difference here, so the 1000 default is enough.
`logput_window` adds a coherent-state check per node per log broadcast; on a
separate day, `inmem_repdb` with it ran at 23,637 inserts/s and without it at
23,514, so the check has no measurable cost.

## Decision

- Turn on `logput_window` by default at 40000000 bytes, the `CATCHUP_WINDOW`
  default, so a node gets live broadcasts again at the same point the master
  starts waiting for it.
- Turn on `inmem_repdb` by default for its steady-load gain, keeping the
  `inmem_repdb_maxlog` default of 1000.
- Leave `sql_logfill` off while it is in beta. Once it is on, catch-up with
  `logput_window` drops from about 80 s to about 47 s in this test.

## Consequences

A replica more than 40 MB behind depends only on pulling the log to catch up
(gap-fill requests, or `sql_logfill` when it is on). If those stall, nothing else moves it forward; in every
run above they did not.

Coherent nodes are not affected by `logput_window`, and an incoherent node
within 40 MB behaves exactly as before.

`inmem_repdb` holds at most `inmem_repdb_maxlog` records per replica (about
16 MB at 100000 in this test). When the list is full, records later than
everything held are dropped and requested again once the gap is filled.

Revisit if a replica more than 40 MB behind is seen to stop catching up, or if
`sql_logfill` becomes the default.

## References

- `logput_window` default: https://github.com/bloomberg/comdb2/pull/6431
- `inmem_repdb` default: https://github.com/bloomberg/comdb2/pull/6287
- `sql_logfill`: https://github.com/bloomberg/comdb2/pull/5604
- The same restart scenario can be run with `BENCH=catchup` in
  `tests/async_dist_commit_bench.test` (https://github.com/bloomberg/comdb2/pull/6144)
