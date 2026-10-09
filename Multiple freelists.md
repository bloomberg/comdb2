# Multiple freelists per btree

Goal: remove the serialization on page allocation. Today `__db_new` and `__db_free` write-lock meta page 0 and hold the lock until commit. Each file gets extra meta pages, each with its own free list. A transaction picks a slot separately in each file, at its first allocation or free in that file, and uses that meta page until it ends.

Branch: `multi-metapage` (in `~/comdb2/multi`). First discussion: Claude session `d1edd431` in `~/comdb2/snap`, 2026-10-07.

## First version (2026-10-08, uncommitted in `~/comdb2/multi`)

Fixed number of meta pages at file create, for benchmarks. No grow, no rebalance, no turn-off.

- Tunable `freelist_meta_pages` (default 8, 1..92). New named, logged, transactional btree files get pages 2..N as extra meta pages (root stays page 1). Page 0: `BTMETA.nmeta` + `metapgno[91]` (was `unused[92]`).
- `MPOOLFILE`: `nmeta`, `metapgno[]`, `metaslot_cnt[]` (atomic hints), `metaslot_next`, `slot_refs` (delays the discard; not `mpf_cnt`, because `__fop_remove` reports `DB_FILEOPEN` when `mpf_cnt != 0`), `grow_lk`. Loaded from page 0 in `__bam_read_root`.
- `__db_meta_pgno` (`db_meta.c`): slot per top-level txn per file, lowest count; **ties rotate the scan start** (with "first lowest" every uncontended txn took slot 0, so all free pages went to page 0). Released in `__txn_end` after the locks.
- File growth when `nmeta > 1`: `grow_lk` from reading `mfp->last_pgno` to `DB_MPOOL_NEW`; panic if they differ. `nmeta <= 1` keeps the old path.
- Limbo: `meta_array` next to `pgno_array`; abort frees with `dbc->use_free_meta_pgno`; recovery builds one list per meta page in page-number order. `pg_prepare` pages go to page 0.
- Test `tests/freelist_meta.test`: 8 writers + aborts on a random-key index, range deletes, all 8 lists in use, `cdb2_verify` offline, `kill -9` during load + recovery.

Findings on `main` while testing (not caused by this work):
- `bdb_asof_current_lsn_mutex` (`bdb/bdb_osqltrn.c:208`, from `3c0ead7ae`) is never initialized. Works on Linux by chance; on macOS every create aborts with `EINVAL`. Local fix: `PTHREAD_MUTEX_INITIALIZER`.
- `__db_pg_alloc_snap_recover` casts `__db_pg_alloc_args` to `__db_pg_freedata_args` for `__db_pg_free_meta_undo`. The layouts differ, so the snapshot copy of a meta page gets a wrong `free` and LSN. Should call `__db_pg_alloc_meta_undo`.
- Writes right after a separate `create index` can hang: one writer waits for `sc_live_lk` in `live_sc_post_add` while holding index page locks, the rest wait for those locks. Seen 1 in 8 runs with the feature off too.

## Open points

- [ ] **Is there a gain at all? Measure first.** A split holds the locks on the page that splits, its parent, and the new page until commit (`__TLPUT` in `bt_split.c`). So after the meta page, the next lock is the parent, which only splits under the same parent share (about 1 in the fanout).
    - Data and blob stripes: inserts in time order all go to the rightmost leaf, which is held until commit. Each stripe is a separate file with its own page 0 (`gbl_dtastripe = 8`). **No gain.**
    - Exception: a purge job that deletes at the left edge of a stripe while inserts run at the right edge. They share only the meta lock (`__db_free` and `__db_new`). **Gain.**
    - Indexes with random keys (UUIDs, hashes, most compound keys): meta is the only lock that all splits share. Big transactions almost always split a page and hold meta to commit. **Gain.**
    - Indexes with keys in increasing order: rightmost leaf is the bottleneck. **No gain.**
    - Measure: on a busy master, take samples of `select object, page, count(*) from comdb2_locks where status = 'WAIT' group by object, page order by 3 desc`. Test: one table with a UUID index, about 8 writers, 100+ rows per transaction, plus a purge job.
    - **First measurement (2026-10-08, single node on the Mac, first version): no gain.** Scripts in `freelist-bench/` (top of this repo) (`bench.sh`, `purge.sh`; start a db `benchdb` first). Table with 2 random-key indexes, 16 writers, 1 meta page against 8:
        - 1000 rows/txn: 94k–122k rows/s both, differences inside the noise between rounds.
        - 50 rows/txn: about 37k rows/s both.
        - 200 rows/txn + 2 purge jobs (oldest rows, ranges of R1): 55k/25k/16k rows/s both (slows per round as the table grows). The purge jobs deleted only about 20k rows per round against 320k inserts, so they freed few pages: not a strong test of the purge case yet.
        - Lock samples: waits are on index leaf pages; page 0 waits almost never (0–3 samples per round). A 1000-row txn on random keys holds about 1 in 8 leaves until commit, so leaf locks collide before meta. The meta lock is held most of the time, but almost nobody waits for it.
        - Writers must send one `insert ... select from generate_series` per txn. With 1000 single-row statements from `cdb2sql`, the client round trips are the bottleneck and the master applies one txn at a time.
        - 10 random-key indexes, single-row single-statement txns (`ten.sh`), 48 writers: about 18k–22k txns/s both, no gain. Stack samples of the 1-meta run: threads wait mostly on mutexes, the log region in `__log_put` (about 2500 samples) and `bdb_osql_trn_repo_lock` at commit (about 2000). Lock-manager waits: about 690, nearly all in `__bam_search`; the meta page wait in `__db_new_original` is 18. The server used 6–7.5 of 18 cores. On this box the log mutex is the limit, so meta contention cannot show.
        - Same load, 60 dumps of `exec procedure sys.cmd.send('bdb lockinfo lockers')` (0.2 s apart, the dump comes back to the client): 36 page waits in total, 40 of 60 dumps with no wait. All waits are on index leaf, internal or root pages; no wait on page 0 or on pages 2..8. In the 1-meta half (about 30 dumps × 10 index files), page 0 is write-held 18 times: each index page 0 is held about 6% of the time, too low to collide.
        - Still to try: a real cluster (commit waits for replicants, so locks are held longer), a purge job that frees more pages, `comdb2_locks` samples on a busy production master.

- [ ] **File growth without `meta->last_pgno` on page 0.** Proposal (2026-10-08), looks correct:
    - The in-memory value is the existing `mfp->last_pgno` (shared `MPOOLFILE`). mpool sets it from the OS file size at open (`mp_fopen.c:980`) and never reads `meta->last_pgno`. `DB_MPOOL_NEW` already uses `mfp->last_pgno + 1` under the mpool lock (`mp_fget.c:570`).
    - Growth: take a new short growth mutex, `pgno = mfp->last_pgno + 1`, write `pg_alloc`, `DB_MPOOL_NEW` (keeps `DB_ASSERT(last == pgno)`), set `last_pgno` on the transaction's meta page, release. Never wait on a lock-manager lock under the mutex. Alternative: `DB_MPOOL_NEW` first, log after, no mutex; a failed log write loses one page.
    - Each meta page's `last_pgno` needs no new log field: `pg_alloc` does not log it, recovery raises it from `argp->pgno` (`db_rec.c:1220`, outside the LSN check, so it never decreases, also on abort). Needs the max rule to use `argp->meta_pgno`. Same rule in `db_snaprec.c:298`.
    - At open: `mfp->last_pgno = max(OS file size, all meta pages)`, raise only (handles share the `MPOOLFILE`). This is a safety check: after recovery or a clean shutdown the OS size is already correct. Warn if a meta value is above the OS size (truncated file).
    - Replicants raise `mfp->last_pgno` when they apply `pg_alloc` with `DB_MPOOL_CREATE`, so a new master needs no reopen.
    - Code that reads `meta->last_pgno` changes to `__memp_last_pgno`: `__db_filesz`, `rep_util.c:1263`, verify, the sparse-file check in `__db_new_original`, `db_snaprec.c`.
    - When the count is 0 or 1, keep the update of `last_pgno` on page 0. The page 0 lock is already held, so it costs nothing, and an old binary can still read the file.
    - Hash reads `mmeta->last_pgno` directly (`hash/hash.c:1166`): btree files only.
- [ ] **Slot assignment.** Proposal (2026-10-08), looks correct. **Design change:** one slot per transaction per file, picked at first use. This replaces the one slot number per transaction, picked at birth.
    - Array of counters (max 92 slots) in `MPOOLFILE` (not `DB_MPOOLFILE`: one file can have more than one handle). Scan only the slots in use.
    - The transaction keeps a hash (or short array) file → slot. On a miss, scan for the lowest counter (usually 0), increment it. Decrement at the end of commit or abort, after the locks are released and after limbo.
    - The counters are a hint only: correctness depends on the meta page lock. Atomics are enough, no mutex. A race or a leaked count costs only contention.
    - **Lifetime:** each entry holds a reference on the `mfp`. Otherwise a table drop or close during the transaction can free the `mfp`, and the decrement writes to freed memory or makes a new counter negative.
    - The hash lives on the top-level transaction; children look it up through `txn->parent`. A child that makes the first entry and aborts leaves the count high until the top-level transaction ends (harmless).
    - Undo and limbo use `meta_pgno` from the log, not the hash (see the limbo item). The hash is only for the pick at first use.
    - `dbc->txn == NULL`: `__TLPUT` releases the meta lock at once, use slot 0 with no count. The grow transaction uses slot 0 and counts. The rebalance picks slots with count 0, then try-lock.
    - Non-block-processor transactions (schema change converters, queues) use the same mechanism.
    - Metric for the grow decision: count the misses that find no slot with count 0, per file.
- [ ] **Grow record.** New log record: changes the count and the list on page 0, formats the new meta page. Needs a `*_snap_recover` handler (snapshot readers rebuild old versions of page 0) and verify support.
    - Clear the full `BTMETA` on the new page. `__db_init_meta` (`db/db_meta.c:96`) clears only the 72-byte `DBMETA` header.
    - The in-memory count changes only after commit. Replicants change it when they apply the record, or read page 0 again when they become master.
- [ ] **Rebalance system transaction.** A system transaction that only does the rebalance. Fixed-size record that moves the first k pages from one list to another: changes `metaA->free`, the next pointer of page k, and `metaB->free`. Locks only meta pages that no transaction owns, in page-number order, so two rebalances cannot deadlock. Page k needs `__db_lock_page_write` for snapshot readers. Needs `*_snap_recover` and verify support. It also empties the lists of retired slots.
- [ ] **Turn-off order (before a downgrade).**
    1. Turn the flag off.
    2. Make file growth write `last_pgno` to page 0 under the lock, or wait for the transactions that started with the flag on.
    3. Write `last_pgno` to page 0 in a logged record.
    4. Optionally combine the lists into page 0.
    5. Make sure that the replicants checkpoint.
    6. Truncate the logs.

    A wrong `last_pgno` on page 0 corrupts data on an old binary. Lost free pages only waste space (a rebuild fixes it).
- [ ] **Prepared (2PC) transactions.** Not answered yet. A prepared transaction can hold its slot for a long time. After a restart, a recovered prepared transaction must lock its meta page again, make its slot hash again from the `meta_pgno` of its log records, and increment the counters. `__txn_add_prepared_child` (`txn/txn_util.c`) must free pages to the meta page of that transaction.
- [ ] **Existing files: is `unused[]` on page 0 always 0?** `__bam_new_file` (`btree/bt_open.c:426`) clears the full `BTMETA`. Not checked: older paths that write page 0, for example the upgrade code. Safety check at open: reject a count above 91, and check that each listed page is `P_BTREEMETA`.

## Clear code changes

- [x] Callers of `pg_alloc`, `pg_free`, `pg_freedata`, `pg_new`, `pg_prealloc` pass the meta page of the transaction in the existing `meta_pgno` field (today always `PGNO_BASE_MD`). No new record types.
- [x] Recovery reads `argp->meta_pgno`, not page 0: `db_rec.c:1096`, `db_rec.c:1374`.
- [x] `*_snap_recover` in `db_snaprec.c` compares with `argp->meta_pgno`, not `PGNO_BASE_MD`.
- [x] Limbo is driven by the log: a page goes back to the meta page it was allocated from (`meta_pgno` of its `pg_alloc` record), in recovery and at abort. Same answer as the slot hash at abort (one slot per transaction per file), so `ctxn` needs no access to the hash of `txn`.
    - `__db_add_limbo` / `__db_add_limbo_fid` keep `meta_pgno` per entry; callers in `db_rec.c` (`pg_alloc`, `pg_new`) pass `argp->meta_pgno`.
    - `__db_limbo_bucket` builds one free list per meta page (today one `last_pgno` for page 0).
    - `pg_prepare` (`db.src:221`) has no `meta_pgno`, and `__db_pg_prepare_undo` adds to limbo. New record version, or take the slot from the `pg_alloc` record of the same page.
    - Lock moves that use page 0 directly: `__db_limbo_move` (`db_dispatch.c:1677`, `PGNO_BASE_MD`) and `__db_limbo_fix` (`db_dispatch.c:1955`, `0`).
    - `LIMBO_COMPENSATE` writes `__db_pg_new_log` with `PGNO_BASE_MD`: use the entry's `meta_pgno`.
- [x] Verify (`__db_vrfy_freelist`, `db_vrfy.c:950`) walks the free list of each meta page. For the pages on the page 0 list, skip three checks: free list on a non-zero meta page (`db_vrfy.c:1372`), valid `root` in `__bam_vrfy_meta` (`bt_verify.c`, "nonsensical root page"), and "unreferenced page". Salvage also calls `__bam_vrfy_meta` per page.
- [x] `bt_stat`, `__db_dump_freepages` read all the lists. (`db_pr.c` not done.)
- [ ] Remove the dead `page_extent_size` path in `__db_new` (nothing calls `set_page_extent_size`). First version only skips it when `nmeta > 1`.
- [x] The new flag is a tunable: update `tests/tunables.test`.

## Decisions

- **Extra meta pages are plain `P_BTREEMETA`, no new type or flag.** Outside verify, berkdb uses the type only for the page format (512-byte checksum, IV, byte swap, pgin/pgout; also `bdb/summarize.c`, `archive/ar_wrap.c`, `cdb2_pgdump`) and for alloc/free. A new type would need changes in all of those. Verify finds sub-databases only through `BTM_SUBDB` on page 0, not by page position. Cursors, `bt_stat`, reclaim and truncate start at the root and never reach the extra pages.
- No code takes the meta lock only to serialize other work. The lock protects the undo of free-list changes and file growth.
- Correctness depends on the meta page lock held until commit, not on exclusive slot ownership. A wrong mapping costs only contention.
- The list of meta pages lives on page 0 in `BTMETA.unused[]` (bytes 92–459, 368 bytes): one word for the count, then up to 91 page numbers. So 92 slots with page 0 is the maximum without a format change. Count = total slots, 0 means 1. Nothing uses `unused[]` today; `archive/ar_page.h:128` has a copy of the layout to change too. Not llmeta, because berkdb recovery cannot read llmeta.
- **The count can grow.** A table starts with count 0 (same as 1, no format change). A grow transaction adds meta pages to one file at a time, so only the files with contention pay. The grow transaction locks page 0 like any slot-0 allocation, and also changes the in-memory copy.
- Transactions do not read page 0 to find their meta page (the slot 0 owner holds a write lock). The count and list live in memory, for example in the shared `MPOOLFILE`.
- The meta page checksum already covers all 512 bytes (`DBMETASIZE`, `db_conv.c:126`, `db_conv.c:368`, `getchksumsz`). A count of 0 gives the same checksum as today. An old binary checks a page with a count above 0 correctly and ignores the field.
- **Shrink is deferred indefinitely.** Stop assigning the slots to retire; the rebalance moves their pages. A retired meta page is one lost page.
- No round robin in recovery limbo: nodes recover from different points, so placement must come from the log. Note: `LIMBO_RECOVER` writes `meta->free` with no log record and no LSN change, so nodes already differ today; the redo of the next logged record on that meta page sets `meta->free` again and the nodes match (local additions are lost space).
- Abort runs on the same thread as the transaction, so `ctxn` can use the meta page of the transaction.
- Replicants: a transaction locks at most one meta page per file, so N meta pages are not a problem.
- Page order is already bad (`__db_free` puts freed pages at the head of the list, so allocation after a delete goes backwards). N lists do not make it worse. The rebalance could sort pages, but then the record has no fixed size.
- Deploy: deploy the binary everywhere before the feature is enabled. Downgrade: turn off, then truncate logs (see turn-off order).
- `__db_new` always calls `__db_new_original`; the system-transaction growth path is dead code.
- Abort of a file growth needs nothing new: undo does not lower `last_pgno` or shrink the file; the new page goes to limbo, and `__db_limbo_fix` frees it with `__db_free` under `ctxn`, which uses the aborted transaction's meta page.

## Outcome (2026-10-09): stopped, not going ahead for now

The benchmarks show no measurable gain, so the work stops here. Details of each run are under "Is there a gain at all?" in Open points.

Benchmark results (single node on the Mac, first version, table with 1 meta page against table with 8):

| Load | 1 meta page | 8 meta pages |
|---|---|---|
| 2 random-key indexes, 16 writers, 1000 rows/txn | 94k–122k rows/s | 96k–109k rows/s |
| 2 random-key indexes, 16 writers, 50 rows/txn | 35k–38k rows/s | 37k–38k rows/s |
| 3 indexes, 16 writers, 200 rows/txn + 2 purge jobs | 55k / 25k / 16k rows/s | 55k / 25k / 16k rows/s |
| 10 random-key indexes, 48 writers, 1 row/txn | 18k–20k txns/s | 17k–20k txns/s |

- The differences are inside the noise between rounds.
- Lock dumps (`bdb lockinfo lockers`, 60 dumps during the 10-index load): no wait on any meta page. Each index page 0 is write-held about 6% of the time.
- Stack samples: the meta page lock wait is 18 samples, against about 4500 on the log region mutex (`__log_put`) and `bdb_osql_trn_repo_lock` at commit. The log mutex limits the commit rate before the meta page lock does.

Why stop: the meta page lock is about 1% of the wait time or less, and the remaining work is large (file growth without `last_pgno` on page 0, grow record, rebalance, 2PC, turn-off order), with format and recovery changes.

What could change the decision: `comdb2_locks` or `bdb lockinfo lockers` samples on a busy production master that show frequent waits on page 0, or a cluster where a commit holds page locks while it waits for the replicants (not checked in the code).

Code: branch `multi-metapage` on github.com/mponomar/comdb2 (`1f4117d96`, `f753eaa69`). It has the first version, a copy of these notes, the benchmark scripts in `freelist-bench/`, and the tests `freelist_meta.test` and `freelist_meta_bench.test`. Not for merge.
