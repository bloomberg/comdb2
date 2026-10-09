# Multiple freelists per btree

## Status

Rejected — 2026-10-09

## Context

Btree page allocation and freeing (`__db_new()` and `__db_free()`) require a
write lock on meta page 0 that is held until transaction commit.

We considered introducing multiple meta pages, each with its own free list, so
concurrent transactions could perform page allocation without contending on a
single metadata page. A transaction would select one meta page for a btree and
use its free list for the lifetime of the transaction.

The hypothesis was that serialization on page 0 was a significant
write-performance bottleneck, particularly for workloads involving random-key
indexes where transactions may split pages throughout the btree.

A prototype supporting eight freelists was implemented and benchmarked against
the existing single-freelist implementation.

Across the tested workloads, throughput with eight freelists was
indistinguishable from throughput with one:

| Workload                                           | 1 meta page            | 8 meta pages           |
|----------------------------------------------------|-----------------------:|-----------------------:|
| 2 random-key indexes, 16 writers, 1000 rows/txn    |        94k–122k rows/s |        96k–109k rows/s |
| 2 random-key indexes, 16 writers, 50 rows/txn      |         35k–38k rows/s |         37k–38k rows/s |
| 3 indexes, 16 writers, 200 rows/txn + 2 purge jobs | 55k / 25k / 16k rows/s | 55k / 25k / 16k rows/s |
| 10 random-key indexes, 48 writers, 1 row/txn       |         18k–20k txns/s |         17k–20k txns/s |

The differences were within the range of run-to-run noise.

Additional measurements help explain why removing the shared freelist did not
improve throughput:

- During 60 lock dumps from the 10-index workload, no transaction was observed
  waiting on a meta page.
- An individual index's page 0 was write-held only about 6% of the time.
- Stack sampling found 18 samples waiting on the meta-page lock, compared with
  roughly 2,500 samples in the `__log_put()` log-region path and roughly 2,000
  in `bdb_osql_trn_repo_lock()` during commit.
- In other workloads, contention appeared on index leaf, internal, or root pages
  before meta-page contention became significant.

The system therefore reaches other bottlenecks before serialization on page
allocation becomes a meaningful limiter of throughput.

Completing the implementation would also require substantial additional work
involving file growth, logging and recovery, freelist rebalancing, prepared
transactions, feature disablement and downgrade handling, verification, and
on-disk format behavior.

## Decision

We will not implement multiple freelists per btree at this time.

The existing single freelist and meta-page locking scheme will remain unchanged.

The prototype, benchmark scripts, tests, and results should be retained so that
this decision can be revisited without repeating the investigation from scratch.

## Consequences

We avoid substantial complexity in the btree file format, recovery paths, and
transaction machinery for an optimization that has not demonstrated a measurable
performance benefit.

Page allocation continues to serialize through the meta-page lock. This remains
a potential bottleneck in principle; the current measurements show only that it
is not a meaningful bottleneck in the workloads tested.

This decision should therefore not be interpreted as evidence that multiple
freelists can never help.

The decision should be revisited if new measurements show either:

- frequent waits on page 0 on a busy production master; or
- materially greater page-0 contention in a real cluster, for example because
  waiting for replicants during commit extends the lifetime of page locks.

## References

- Prototype branch: `experiment-multi-metapage`
- Prototype commits: `1f4117d96`, `f753eaa69`, `40ff1e4cf`
- Benchmark scripts: `freelist-bench/`
- Tests: `freelist_meta.test`, `freelist_meta_bench.test`
- Investigation notes and raw benchmark results: `Multiple freelists.md`
