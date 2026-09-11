# Design: Collect index statistics atomically with schema change

## Problem

When a new index is created or altered on a table, SQLite starts with default
(essentially empty) statistics for that index. Until a subsequent `ANALYZE`
runs, the query planner has no cardinality/selectivity data for the new index
and can pick poor plans — skipping the index entirely, choosing a bad join
order, or misjudging range/skew selectivity.

Today the schema-change path only *invalidates* stats: `scdone_sc_analyze`
bumps `gbl_analyze_gen`, which forces query-plan caches to re-plan. But there is
no fresh stat *collection* — the new index simply has no stats until autoanalyze
or a manual `ANALYZE` eventually runs.

## Goal

Collect statistics for newly built indexes as part of the schema change, such
that on **every node** the good stats are visible no later than the moment the
new index becomes queryable. No window in which a client can see the new index
with default stats.

## Key correctness constraint: replication ordering

It is **not** sufficient to run `ANALYZE` right after the schema change commits
on the master. Consider:

1. Master commits the schema change (new index visible).
2. Schema-change transaction replicates to replicant R.
3. A client queries R against the new index — with default stats — and gets a
   bad plan.
4. Only later does the stats transaction arrive at R.

Because replication is **ordered**, the fix is to ensure the stats transaction
commits *before* the schema-change transaction on the master. Then every
replicant receives (and applies) the stats before it receives the schema
change, closing the window on all nodes.

Concretely: the stats must be committed before `finalize_alter_table` runs.

## Options considered

| # | Approach | Verdict |
|---|----------|---------|
| 1 | Run `ANALYZE` synchronously after `finalize_alter_table` commits, before returning to client | **Rejected.** Replication race above — another client on a replicant can query before stats replicate. |
| 2 | Write `sqlite_stat1`/`sqlite_stat4` records directly as BDB ops inside the finalize transaction (`transac`) | Truly atomic, but requires reimplementing stat serialization — especially `sqlite_stat4`'s binary key-encoded `sample` blob — at the BDB level. High complexity. |
| 3 | Run analyze against the pre-commit `newdb`, commit the stats in their own transaction *before* finalize | **Chosen.** Reuses the entire existing analyze pipeline; solves the replication race; requires a bypass of `dispatch_sql_query` and backout cleanup. |

### Why stat4 matters (and why option 2's cost is real)

`sqlite_stat1` stores average selectivity per index-column-prefix — enough for
the planner to decide whether to use an index at all. `sqlite_stat4` stores
sampled rows with per-value `nEq`/`nLt`/`nDLt` counts, and is what the planner
uses for **skewed distributions** and **range predicates**. For a new index on
a skewed column or one used in range scans, stat1 alone can still yield a bad
plan. So we want both stat1 and stat4.

The `sqlite_stat4.sample` column (see `csc2files/comdb2_stats4.csc2`) is a blob
holding a complete index key in **SQLite's internal Vdbe key encoding**
(varint, affinity-aware) — not comdb2's BDB key format. Producing it outside
SQLite means reimplementing that encoding. This is the crux of why option 2 is
expensive and why option 3 (which lets the VDBE do the encoding) is preferred.

## Chosen design (Option 3)

Run the existing SQLite `analyzesqlite` command against the **pre-commit
`newdb`** temp table, so the analysis sees the new indexes and writes correctly
encoded stat1 + stat4 rows under the real table/index names, committing in its
own transaction before `finalize_alter_table`.

### Background: how the pieces fit

- **Rootpages.** comdb2 repurposes SQLite's "root page" concept. `thd->rootpages`
  (`master_entry_t[]`) maps each SQLite rootpage number to a comdb2
  `(dbtable, index)` pair. When the VDBE runs `OP_OpenRead <tnum>`, comdb2's
  btree layer indexes `thd->rootpages[tnum]` (`get_sqlite_db`) to find the BDB
  handle to open a cursor against. Whatever is loaded there defines both which
  objects SQLite sees and which physical files its cursors read.

- **`sqlite3Analyze` loop.** For `ANALYZE <table>`, SQLite locates the table
  (`sqlite3LocateTable`) and iterates `pTab->pIndex` — one `OP_OpenRead` +
  stat computation per index (`analyze.c:1307`). The set of indexes analyzed is
  exactly what is present in the loaded schema.

- **Sampling.** For large tables, `sample_indexes(dbtable*)` pre-scans each
  index into an in-memory sampler; during analyze, cursor opens on a sampled
  index are redirected to the sampler (`is_sampled_idx`, `sqlglue.c:7376`).
  This redirects the *data source* only — it does **not** make new indexes
  visible to the analyze loop. Schema visibility still comes from rootpages.

- **`sql_syntax_check` (`sqlglue.c:2278`).** The existing precedent for making a
  *non-live* `newdb` visible to SQLite. It calls `create_sqlmaster_record(newdb)`
  → `create_master_entry_array(&newdb, 1, ...)` → `get_copy_rootpages_custom()`
  to load `newdb`'s entries, then opens the connection with
  `sqlite3_open_serial` and runs SQL via `sqlite3_exec` **directly** — never
  through `dispatch_sql_query`.

### The obstacle, and its resolution

`run_internal_sql_clnt` → `dispatch_sql_query` → `prepare_engine` calls
`get_copy_rootpages_nolock` (`sqlinterfaces.c:4473`), which overwrites
`thd->rootpages` with the **live** schema. So the standard analyze path
(`analyze_regular_table` → `run_internal_sql_clnt("analyzesqlite …")`) cannot
see `newdb`'s new indexes: they'd be clobbered before the query runs.

**Threading detail that shapes the fix.** `enqueue_sql_query` always hands the
clnt to a *pool thread* via
`thdpool_enqueue(pool, sqlengine_work_appsock_pp, clnt, …)`
(`sqlinterfaces.c:5114`); the caller merely blocks in `wait_for_sql_query`. The
analyze `force_dispatch` path (line 5261) is misleadingly commented — the
`THDPOOL_FORCE_DISPATCH` flag bypasses *queueing*, not the pool. The query
therefore executes on a pool thread with its own `sqlthdstate`/`sqlthd`, so
pre-loading rootpages onto the calling thread would have no effect. Rootpages
must travel on the **clnt**, which is the work item passed to the pool.

**Resolution: carry the entries on the clnt, and reconcile per query.**

The caller uses the ordinary analyze path:

```c
clnt.pPool                    = get_named_sql_pool("scanalyze", 1, n);
clnt.custom_rootpages         = ents;   /* newdb + the stat tables */
clnt.custom_rootpage_nentries = nents;
clnt.custom_dbtable           = newdb;  /* storage, not just schema */
run_internal_sql_clnt(&clnt, "BEGIN");
run_internal_sql_clnt(&clnt, "analyzesqlite main.\"<new index name>\"");  /* per index */
run_internal_sql_clnt(&clnt, "COMMIT");
```

#### The custom entry array: `newdb` plus the stat tables

`sql_syntax_check` passes a single table (`create_master_entry_array(&db, 1, …)`)
because its `select 1 from sqlite_master` touches nothing else. **We need more
than that.** `openStatTable` resolves the stat tables through the loaded
schema — `sqlite3FindTable(db, "sqlite_stat1", …)` then `aRoot[i] = pStat->tnum`
and `OP_OpenWrite` (`analyze.c:233`, `:240`). If `sqlite_stat1` is absent from
the rootpage set, the comdb2 branch at line 228 silently does nothing for stat1
and sets `skip4` for stat4: **the analyze runs and writes no statistics, with no
error reported.** `analyze_regular_table`'s backup-stats statements
(`delete from sqlite_stat1 …`, etc.) likewise need those tables visible.

But only those. The transaction touches exactly `newdb` (scanned) plus
`sqlite_stat1` / `sqlite_stat4` (written), so the array is:

```c
struct dbtable *dbs[3] = { newdb,
                           get_dbtable_by_name("sqlite_stat1"),
                           get_dbtable_by_name("sqlite_stat4") };  /* stat4 optional */
ents = create_master_entry_array(dbs, n, NULL /* no views */, &nents);
```

Notes:
- `newdb->tablename` is already the real table name (the `.NEW.` prefixing
  applies to tags, not the dbtable name), so stats are written under the correct
  table/index names.
- Pass `view_hash = NULL`: views could reference tables outside the array.
- `sqlite_stat4` is optional — guard with `get_dbtable_by_name` as the existing
  analyze code does.
- `create_master_entry_array` assigns `rootpage = position + RTPAGE_START` and
  the emitted sqlite_master rows encode those same numbers, so the array is
  dense and self-consistent and the default positional lookup in `get_sqlite_db`
  works. `selective_rootpages` is not needed.
- Partial rootpage sets are an established pattern:
  `get_copy_rootpages_selectfire` exists for this and is used by
  `logical_cron.c:125`. We cannot reuse it directly (it selects from the live
  `sqlmaster`, which has no `newdb`), but it confirms the approach.

Consequence for cost: each rootpage swap is ~10–20 entries rather than
full-DB-sized, which keeps swapping cheap even across repeated schema changes.

#### Pool choice: use a dedicated pool

`clnt.admin = 1` does *not* select a separate pool — `get_sql_pool`
(`sqlpool.c:201`) returns `clnt->pPool` if set and otherwise the **default**
pool, with `admin` affecting only queue-bypass and rule checking. So by default
our analyze runs on the same threads serving normal clients.

With the minimal array above the *cost* of that is modest (~10–20 entries per
swap). The remaining concern is **containment, not throughput**: a polluted
default-pool thread would expose a three-table schema to an ordinary client
until the reconciliation corrects it. That is a severe failure if it ever
escapes, even though the reconciliation is designed to prevent it.

**Decision: give the SC analyze its own pool** via
`clnt->pPool = get_named_sql_pool("scanalyze", 1, n)`, sized 2–4 (not 1, so
back-to-back schema changes do not serialize on it). Then no normal-client
thread is ever loaded with a custom set at all, and the reconciliation becomes a
backstop rather than the only thing standing between us and a wrong-schema
query. Cost is a couple of mostly-idle threads.

The per-clnt reconciliation below is still implemented, as defense-in-depth: it
keeps the dedicated pool an optimization rather than a load-bearing correctness
assumption, and protects any future caller that sets `custom_rootpages` without
a dedicated pool.

#### Rootpage pollution hazard — and why a simple branch is not enough

A first cut would just branch at the existing reload site in `prepare_engine`
(`sqlinterfaces.c:4473`). **That is unsafe.** That call lives *inside* the
`if (!thd->sqldb || rc == SQLITE_SCHEMA_REMOTE)` block (opens 4425, closes
4494), so it only runs when the engine is opened or refreshed — **not on every
query**. A pool thread with a warm `thd->sqldb` and no staleness never reloads
rootpages.

Consequence: after our analyze runs on pool thread P, P's
`sqlthd->rootpages` still holds `newdb`'s entries. The next query from an
unrelated client on P proceeds as follows:

1. `thd->sqldb` is warm, so `check_thd_gen` runs.
2. It sees the `analyze_gen` bump our own analyze caused, calls
   `reload_analyze`, and returns `SQLITE_OK` — **not** `SQLITE_SCHEMA`.
3. The guard at 4404 therefore does not fire, and the block at 4425 is skipped.
4. That client executes against `newdb`'s rootpages — a single-table, wrong
   schema view.

The window lasts until something forces P to reopen its engine (this SC's own
`dbopen_gen` bump at finalize, which is strictly later). Clearing the field on
our clnt does not help: the stale copy lives on the *pool thread*, not the clnt.

**Fix — make thread rootpages self-correcting per clnt.** Track on
`struct sql_thread` whether the current rootpages came from a custom source, and
reconcile at the top of `prepare_engine` (after the existing `sqlthd` lookup,
before `check_version`):

```c
if ((clnt->custom_rootpages || sqlthd->rootpages_custom) &&
    sqlthd->rootpages_owner != clnt)
    get_copy_rootpages_for_clnt(sqlthd, clnt);   /* sets rootpages_owner */
```

The `rootpages_owner` tag avoids re-copying when the thread already holds the
right set — important because each copy is full-DB-sized, and our clnt issues
several statements.

`get_copy_rootpages_for_clnt` (in `sqlmaster.c`) loads `clnt->custom_rootpages`
when set and marks `rootpages_custom`, otherwise loads the live `sqlmaster` and
clears the flag. The existing call site at 4473 becomes the same helper so
first-open also honors the clnt.

| Case | Behavior |
|------|----------|
| Our clnt, first query on a thread | Block runs (no `sqldb`) → loads custom |
| Our clnt, later query on a warm thread | Early check fires on `custom_rootpages` → loads custom |
| Other clnt on a thread we polluted | Early check fires on `rootpages_custom` → restores live, clears flag |
| Normal clnt, clean thread | Both false → no work |

Cost on normal traffic is two predictable tests; the deep copy happens only in
the rare case. Note this means the reconciliation *must* live in
`prepare_engine` (the per-query chokepoint); confining the change to
`sqlmaster.c` alone would not be sufficient, though the helper itself belongs
there.

**Concurrency on the clnt.** There is none — the clnt is local to the analyze
routine and its statements run serially, each blocking in `wait_for_sql_query`.
The hazard runs the other way: the three statements may land on *different* pool
threads, so each can leave a polluted thread behind. That is precisely why the
check must key off the clnt on every query rather than being one-time setup.

**Teardown ordering.** Clear `clnt->custom_rootpages` first, then free the
entries (`destroy_sqlite_master`), then `end_internal_sql_clnt` — so no late
cleanup path can dereference freed entries.

#### Concurrency with other stat writers

`set_analyze_running` (`sqlanalyze.c:1055`) is an `XCHANGE32` test-and-set — a
global mutex over the analyze drivers (`analyze_table`, `analyze_database`, and
autoanalyze through them). Our inline analyze takes it, which serializes us
against user and automatic analyzes. This matters: two overlapping runs of
`analyze_rename_table_for_backup_stats` would corrupt each other's
`cdb2.X.sav` bookkeeping.

**Scope of the flag.** We hold it only for our analyze window (seconds), **not**
across the whole schema change. `convert_all_records` can run for hours on a
large table, and blocking every analyze database-wide for that long — including
autoanalyze — is not acceptable. Consequently a user or automatic analyze may
start at any point during the SC and may still be running when we reach our
analyze point.

**If the flag is already held: skip and log.** The schema change proceeds
without fresh stats, degrading to today's behavior. Failing an SC because
someone ran `ANALYZE` would be far worse than losing the optimization. (A short
bounded wait before giving up is a possible later refinement, and would reduce
how often the case below arises.)

#### When we skip, leave existing stats alone

A tempting idea is to delete the stat rows for rebuilt indexes when we cannot
analyze them, on the theory that stale stats are worse than none. **Rejected.**

`ix_plan == -1` means *the index file was rebuilt*, which is not the same as
*the statistics are invalid*. The two diverge in a common case:
`prepare_sc_plan` forces a rebuild whenever
`(newixs->flags & SCHEMA_DATACOPY) && plan->dta_plan == -1`
(`sc_schema.c:908`), so any change to the table's data layout — a plain
`ALTER TABLE ADD COLUMN`, for instance — rebuilds every datacopy index while
leaving its **key structure untouched**. Those statistics remain fully valid;
deleting them would discard good data and leave the planner with nothing.

Note also where a delete would actually land: a brand-new index has no stat rows
at all, so the delete is a no-op there. It would only ever affect indexes that
*already had* stats — precisely the population most likely to still be correct.

The stale case is milder than it first appears. If index X goes from `(a)` to
`(a, b)`, its stat1 string simply carries fewer integers than the index has
columns; SQLite consumes what is present and leaves defaults for the remainder,
so leading-column selectivity stays correct. Only a genuine repurposing (X was
`(a)`, now `(b)`) produces actively misleading numbers.

It is also rare. `do_analyze` **rejects** analyze outright while a schema change
is in progress (`sqlanalyze.c:1441`), so a user `ANALYZE` cannot start mid-SC.
The remaining paths are an analyze that began before the SC started, or
autoanalyze, which waits ~50s (`autoanalyze.c:114`) and then proceeds anyway.

**Decision: on skip, change nothing** — identical to today's behavior, zero
regression risk. Deleting only when the *key structure* actually changed is a
coherent refinement, but it requires the datacopy-masked `cmp_index_int` variant
that was already judged not worth building.

#### Implementation constraint from the SC-in-progress guard

Because `do_analyze` refuses to run during a schema change, our inline analyze
must call **`analyze_regular_table` directly** — never `do_analyze` or
`analyze_table`, which would reject us, since by definition a schema change is
in progress. `analyze_running_flag` is taken by `analyze_table`, not
`analyze_regular_table`, so we set and clear it ourselves.

Consequences to be aware of:
- Because the flag is global, two schema changes that finish converting at
  roughly the same time cannot both analyze — the second skips. Sequential
  schema changes are unaffected. This also means concurrent SCs cannot thrash
  each other's rootpages: their analyzes never overlap.
- Direct user writes to the stat tables are not covered by the flag, but they
  are ordinary transactional writes; under RECOM a genuine conflict surfaces as
  a verify error at commit. Log and continue — do **not** fail the SC.
- Readers on other pool threads see nothing anomalous: our writes are
  uncommitted until `COMMIT`.

#### Snapshot staleness

No schema lock is held at our insertion point, so in principle another schema
change could commit while our analyze runs and make our entry array stale.
Impact is bounded: `get_sqlite_db` resolves entries **by name**
(`get_dbtable_by_name`, `sqlmaster.c:336`) and returns NULL when the table is
gone, producing a clean SQLite error rather than a use-after-free; the entries
themselves are deep copies (strdup'd name, malloc'd blob), so the array never
dangles.

With the minimal three-table array the realistic exposure is close to nil: our
own table cannot be concurrently schema-changed (SC is serialized per table),
and the stat tables are not normal SC targets.

Crucially, because writes still flow through `dispatch_sql_query`, the normal
osql commit path (`do_commitrollback` → `recom_commit` → `osql_sock_commit`)
works unchanged — which was the whole reason we could not simply follow the
read-only `sql_syntax_check` pattern.

**What this removes from the design.** No `sqlite3_open_serial` /
`sqlite3_close_serial`, no manual `get_curtran` / `put_curtran`, no
`sql_syntax_check`-modeled setup routine, and no rootpage save/restore on the
caller's thread (it is never touched). The core changes are the per-query
reconciliation above plus the `sqlmaster.c` helper; the rest reuses
`analyze_regular_table` / `run_internal_sql_clnt` as-is.

### Flow

In `do_alter_table` (`sc_alter_table.c`), after `convert_all_records` succeeds
and after `check_for_idx_rename`, but before returning `SC_OK` (which precedes
`finalize_alter_table`), `analyze_sc_new_indexes()` runs:

1. **Detect built indexes.** Iterate `newdb->nix`, marking indexes where
   `newdb->plan->ix_plan[ixnum] == -1`. If none, skip entirely. Skipped on
   schema-change resume.
2. **Build `newdb`'s master entries.**
   `create_sqlmaster_record_flags(newdb, NULL, 1)` then
   `create_master_entry_array(dbs, n, NULL, &nents)` over
   `[newdb, sqlite_stat1, sqlite_stat4]`. The `strip_new_prefix` flag names
   `newdb`'s indexes the way they will be named once the schema change commits
   (see below).
3. **Take `analyze_running_flag`.** If another analyze holds it, log and skip.
4. **Resolve coverage** from `get_saved_scale()`, defaulting to
   `BDB_ATTR_DEFAULT_ANALYZE_PERCENT` (20%).
5. **`flush_db()`.** Sampling reads index files straight off disk, so pages the
   schema change just wrote must be flushed first — otherwise the sampler sees
   an empty index and produces empty statistics.
6. **Set up the internal clnt** — `TRANLEVEL_RECOM`, `admin`,
   `osql_max_trans = 0`, `sc_analyze = 1`, a dedicated `scanalyze` pool, plus
   `custom_rootpages` / `custom_rootpage_nentries` / `custom_dbtable = newdb`.
7. **`BEGIN`**, then sample only the built indexes
   (`sample_indexes(..., new_ix)`).
8. **Analyze each built index by name** — `analyzesqlite main."<sqlitetag>"`.
   Per-index rather than whole-table; see below.
9. **`COMMIT`** via the ordinary osql path. Because this commits before
   `finalize_alter_table`, it replicates ahead of the schema change on all
   nodes.
10. **Teardown.** `cleanup_sampled_indices`, clear the custom rootpages/dbtable
    on the clnt, `end_internal_sql_clnt`, free the entries
    (`destroy_sqlite_master`), clear `analyze_running_flag`. Returns 0
    unconditionally — statistics never fail a schema change.

#### Rootpages give the schema; `custom_dbtable` gives the storage

Loading `newdb`'s rootpages is only half of what is needed. `get_sqlite_db()`
resolves a rootpage to a dbtable **by name** through `thedb->db_hash`
(`sqlmaster.c:336`), and `newdb` is not in that hash until finalize — no
`add_dbtable_to_thedb_dbs` call exists anywhere in `sc_alter_table.c`. Without
an override, sqlite would see `newdb`'s schema while every cursor resolved to
the *old* table's storage, indexing `olddb->ixschema[]` with an index number the
old table may not even have.

`clnt->custom_dbtable` closes that gap: `get_sqlite_db()` returns it for its own
name. Only our table is affected — the stat tables still resolve to the real
live tables, which is what we want.

#### Index naming: strip `.NEW.` at generation time

Sqlite index names come from `form_new_style_name()`, which formats
`"$<csctag>_<crc>"`. `newdb`'s tags carry the schema change's `.NEW.` prefix, so
statistics would land under `$.NEW.IXA_<crc>` while the committed index is
`$IXA_<crc>` — the same crc, since it is computed over the table and columns
rather than the tag, but a name the planner will never look up.

Renaming the rows afterwards does **not** work: they were inserted by the same
osql transaction, and updating them fails in `delete_synthetic_row`. So the
names are made correct at generation time instead, via the `strip_new_prefix`
flag on `create_sqlmaster_record_flags()`.

#### Per-index, not whole-table

`analyze_regular_table()` moves *every* stat row for the table aside to
`cdb2.<tbl>.sav` and then re-analyzes the whole table. That is wrong here: in a
planned schema change the reused indexes hold **no data in `newdb`** — their
files are adopted by pointer swap at finalize — so they sample empty, produce no
replacement rows, and their previously good statistics are stranded in `.sav`.
Adding one index would destroy the statistics of every other index on the table.

Instead each built index is analyzed by name. `sqlite3Analyze` routes an index
argument to `analyzeTable(pTab, pOnlyIdx)` with
`openStatTable(..., pOnlyIdx->zName, "idx")`, so both the delete and the insert
are scoped to that index and every other index is left untouched. This also
drops the `.sav` backup cycle entirely.

#### Guards that had to be relaxed

Two guards abort an analyze while a schema change is in progress. Both exist to
stop an *unrelated* analyze from competing with a schema change; neither
premise holds when the schema change is analyzing the table it just built:

- `bdb/summarize.c:404`, in the sampler's page loop — bypassed by the new
  `sc_analyze` argument to `bdb_summarize_table()`.
- `db/sqlglue.c`, in `cursor_move_preprop` (gated on `clnt->is_analyze`) —
  bypassed by `clnt->sc_analyze`.

A third, in `do_analyze()` (`sqlanalyze.c:1441`), is avoided by calling
`analyze_regular_table`/`analyze_table_dbtable` directly rather than going
through `do_analyze` or `analyze_table`, which would reject us outright.

### Backout: nothing to undo

If `finalize_alter_table` backs out after we committed, the statistics we wrote
describe indexes that never went live. Deleting them was considered and
**rejected**, for the same reason the skip case leaves statistics alone:

- If the index definition was unchanged (a datacopy-driven rebuild, say), the
  crc and therefore the sqlite name are identical to the surviving index's, and
  the statistics we gathered are still perfectly valid for it. Deleting them
  would discard good data.
- If the definition did change, our rows sit under a name no index has. The
  planner looks statistics up by the index's current name, so they are inert,
  and a later full `ANALYZE` reaps them through the `cdb2.<tbl>.sav` cycle.

So there is no backout hook. This also keeps the two paths consistent: we never
delete statistics we did not just write.

## Affected code

- `db/sql.h` — `custom_rootpages`, `custom_rootpage_nentries`,
  `custom_dbtable`, `sc_analyze` on `struct sqlclntstate`; `rootpages_custom`,
  `rootpages_owner` on `struct sql_thread`.
- `db/sqlmaster.c` — `get_copy_rootpages_for_clnt()` and
  `rootpages_need_reload_for_clnt()`; `get_copy_rootpages_nolock()` now clears
  the custom tag; `get_sqlite_db()` honors `clnt->custom_dbtable`.
- `db/sqlinterfaces.c` — `prepare_engine()`: per-query rootpage reconciliation
  near the top, and the existing reload routed through the same helper.
- `db/sqlglue.c` — `create_sqlmaster_record_flags()` with `strip_new_prefix`
  (`create_sqlmaster_record()` becomes a wrapper, no longer static);
  `cursor_move_preprop()` honors `clnt->sc_analyze`.
- `db/sqlanalyze.c` — `analyze_new_indexes()`; `analyze_table_dbtable()` split
  out of `analyze_regular_table()` so a caller can supply the dbtable;
  `sample_indexes()` takes an index mask and the `sc_analyze` flag;
  `sc_analyze` added to the index/table descriptors.
- `bdb/summarize.c`, `bdb/bdb_api.h` — `bdb_summarize_table()` takes
  `sc_analyze` and skips the schema-change abort when set.
- `schemachange/sc_alter_table.c` — `analyze_sc_new_indexes()` builds the
  built-index mask from the plan and invokes the analyze after
  `convert_all_records` / `check_for_idx_rename`.
- `db/db_tunables.{c,h}` — `analyze_new_indexes` (bool, default on) and
  `sc_analyze_threads` (int, default 2, readonly).
- `tests/sc_analyze.test` — new test.

## Locking & the write-vs-rootpages tension (investigated)

**Schema-lock timing — clear.** In `do_alter_table` the schema write lock is
acquired for setup only (`sc_alter_table.c:458`) and released at line 568,
*before* `convert_all_records`. At the proposed insertion point (after
conversion, before returning `SC_OK`) no schema lock is held. `do_alter_table`
is invoked as `pre()` with a NULL tran (`sc_logic.c:499`); the finalize schema
write lock is taken later inside `do_finalize` (line 540), so the analyze is not
nested inside the finalize lock. `sql_syntax_check` already performs
`get_curtran` + `sqlite3_open_serial` + `sqlite3_exec` at this stage (under the
*write* lock, even), so the mechanics are proven and self-deadlock is not a
concern.

**The real obstacle — stat writes need the path that clobbers rootpages.**
The stat-table INSERTs commit through the osql machinery:
`do_commitrollback` → `recom_commit` → `osql_sock_commit`
(`sqlinterfaces.c:2062`). That path is only reached by going *through*
`dispatch_sql_query`. But `dispatch_sql_query` → `prepare_engine` calls
`get_copy_rootpages_nolock` (`sqlinterfaces.c:4473`) whenever it opens/refreshes
the engine — which happens on our fresh analyze clnt's first query — replacing
`newdb`'s rootpages with the live schema. So:

- keep `newdb` visible → must avoid the `dispatch_sql_query` rootpage reload;
- write & commit the stats → need the `dispatch_sql_query` osql commit path.

`sql_syntax_check` only avoids this because it is read-only (`select 1`); it
never exercises the write/commit path, so it never needed to reconcile the two.

**Resolution — chosen: carry the rootpages on the clnt.** See
"The obstacle, and its resolution" above for the mechanism. `prepare_engine`
honors `clnt->custom_rootpages` when set, so writes continue to flow through the
normal osql commit path while `newdb` stays visible. Cost: a ~4-line branch in
the hot `prepare_engine` path.

A second option — **direct exec + manual osql lifecycle** (open SQLite directly
like `sql_syntax_check`, keeping `newdb` rootpages, and hand-drive
`osql_sock_start` … `recom_commit`/`osql_sock_commit` around `sqlite3_exec`) —
was considered and rejected. It keeps the hot dispatch path untouched, but
requires hand-managing the transaction lifecycle that `do_commitrollback`
normally provides, for no correctness benefit.

**Version-check interaction — investigated, largely safe.** `check_thd_gen`
(`sqlinterfaces.c:2801`) has three staleness triggers; only one can drop custom
rootpages, and its window is narrow:

- `analyze_gen` (line 2817): calls `reload_analyze`, which (a) early-returns
  when `analyze_running_flag` is set (line 2759) and (b) only calls
  `sqlite3AnalysisLoad` — it reloads stat *data* into the existing engine and
  does **not** reopen it or rebuild rootpages. Harmless. Our inline analyze must
  set `analyze_running_flag` (as the normal path does via
  `set_analyze_running`) — this also serializes against concurrent user
  analyzes.
- `views_gen` (line 2827): returns `SQLITE_SCHEMA_REMOTE`, which refreshes
  remote/fdb schema, not local rootpages. Not a concern here.
- `dbopen_gen` (line 2813): returns `SQLITE_SCHEMA` → `prepare_engine` recreate
  path → close + reopen + rootpage reload. With rootpages carried on the clnt
  this is *self-healing*: the reload after reopen goes through the same branch
  and re-reads `clnt->custom_rootpages`, so `newdb` stays visible. This is a
  further advantage of the clnt-carried approach over a bare suppress flag,
  which would have lost the custom rootpages on reopen.

## Other open questions / risks

- **Analyze cost in the SC critical path.** Running analyze inline lengthens the
  schema change. Bounded by sampling (existing threshold logic) and by
  analyzing only the indexes that were actually built. Whether stat4 collection
  should additionally be gated by table size is still open.
- **Stale rows under an index's previous sqlite name.** Altering an index
  changes its crc and therefore its sqlite name, so the row written under the
  old name is orphaned. This predates the feature (nothing has ever reaped
  per-index staleness -- `cleanup_stats` prunes by table), it is invisible to
  the planner, and a later full `ANALYZE` clears it via the `cdb2.<tbl>.sav`
  cycle. Left alone deliberately; a targeted cleanup would need to distinguish
  a changed key structure from a same-structure rebuild.
- **Coverage / percent — implemented.** Use the table's configured coverage via
  `get_saved_scale()` (`sqlanalyze.c:637`, reads the llmeta
  analyze-coverage value), falling back to
  `BDB_ATTR_DEFAULT_ANALYZE_PERCENT` — **20%** (`bdb/attr.h:422`) — when none is
  set. This matches what autoanalyze does (`autoanalyze.c:121`), so an operator
  who has tuned a table's coverage gets the same treatment here.
- **Resume / preempt — implemented.** Skip the inline analyze on schema-change
  resume. The stats are an optimization, not a correctness requirement, and
  resumed SCs already have the more complex state to reason about; a subsequent
  analyze or autoanalyze will fill them in.
- **Index-creation entry points — confirmed unified.** `CREATE INDEX`
  (`comdb2CreateIndex`, `comdb2build.c:6350`) sets `sc->kind =
  SC_ALTERTABLE_INDEX`. In the `do_schema_change_if` table (`sc_logic.c`), kinds
  24–29 (`SC_ALTERTABLE`, `SC_ALTERTABLE_PENDING`, `SC_REBUILDTABLE`,
  `SC_ALTERTABLE_INDEX`, `SC_DROPTABLE_INDEX`, `SC_REBUILDTABLE_INDEX`) all
  dispatch to `do_alter_table` / `finalize_alter_table`. So `CREATE INDEX`,
  `ALTER … ADD INDEX`, and `REBUILD INDEX` share one path, and the
  `ix_plan[i] == -1` detection covers all of them without branching on
  `sc->kind`:
  - `CREATE INDEX` / `ALTER ADD INDEX`: new index → `ix_plan == -1` → analyzed.
    A new index forces `plan_convert = 1` (`sc_schema.c:936`), so
    `convert_all_records` builds the index files in `newdb` before our insertion
    point — the data is present to analyze.
  - `REBUILD INDEX`: rebuilt index → `ix_plan == -1` → re-analyzed (desirable;
    stats may be stale).
  - `DROP INDEX`: no new index → matches nothing → analyze correctly skipped.

- **Altered indexes are covered.** `prepare_sc_plan` assumes rebuild
  (`ix_plan[ixn] = -1`, `sc_schema.c:904`) and only reuses an old index file
  when `cmp_index_int` (`tag.c:4350`) reports an exact ondisk match. Any
  semantic change to an index definition fails that comparison and forces a
  rebuild — added/removed columns (`nmembers`, line 4362), changed index
  attributes (DUP / RECNUM / DATACOPY / UNIQNULLS / PARTIALDATACOPY), a changed
  partial-index WHERE clause, or any per-field change (type, length, offset,
  ASC/DESC, column name). So e.g. adding a column to an existing index yields
  `ix_plan == -1` and is analyzed.

- **Coverage is complete — the three cases partition cleanly:**

  | Case | `ix_plan[i]` | Stats handling |
  |------|--------------|----------------|
  | Built or rebuilt (new index, or definition changed) | `-1` | **This design:** fresh analyze |
  | Reused, name changed (pure rename) | `>= 0` | Existing `check_for_idx_rename` → `add_idx_stats` copies stats to the new name |
  | Reused, name unchanged | `>= 0` | Nothing needed — existing stats remain valid |

  The middle row is exactly why `check_for_idx_rename` exists: when the index
  file is byte-identical and only the name changed, the old stats are still
  accurate and merely need re-keying. This design handles the complementary
  case where the file was actually rebuilt.

- **Datacopy-only changes re-analyze redundantly — accepted.** Adding
  DATACOPY/PARTIALDATACOPY to an index changes the attribute bits compared by
  `cmp_index_int` (`tag.c:4356-4358`), so the index is rebuilt with
  `ix_plan == -1` and this design re-analyzes it. That work is redundant:
  `analyzeOneTable` truncates the analyzed column count at the first DATACOPY
  column (`analyze.c:1317-1323`), so datacopy columns never contribute to
  statistics and the recomputed stat1 string is identical to the previous one.

  Decision: **leave it.** Correctness is unaffected (identical numbers are
  recomputed, never wrong ones); the marginal cost is modest because a datacopy
  change already forces a full index rebuild and `convert_all_records` table
  scan, with sampling bounding the analyze on large tables; and suppressing it
  would require a `cmp_index_int` variant that masks the datacopy attribute bits
  purely to detect this case.

  If SC latency later proves a concern, the better fix is not to skip but to
  *copy* the existing stats forward for datacopy-only changes (the
  `add_idx_stats` approach used for renames) — same correctness, near-zero cost.

## Testing

`tests/sc_analyze.test` covers, all without any explicit `ANALYZE`:

- **Statistics present on every node** immediately after the schema change
  returns — the property this design exists for. Iterates `comdb2_cluster` and
  checks each host directly, so it degrades to a single-host check when run
  standalone.

- **CREATE INDEX** populates `sqlite_stat1` for the new index.
- **ALTER ADD INDEX** does the same, and leaves the pre-existing index's
  statistics intact (the regression that per-index analyze fixes).
- **Altering an index definition** refreshes its statistics: the stat string
  grows from two values for `(b)` to three for `(b, a)`.
- **No `.NEW.` names** reach `sqlite_stat1` — guards the naming fix.
- **Datacopy rebuild preserves stats**: `ALTER TABLE ADD COLUMN` on a table with
  a datacopy index rebuilds it (`ix_plan == -1`) without changing its key
  structure; its statistics must survive. This is the case that rules out
  deleting statistics whenever we do not re-analyze.
- **DROP INDEX** leaves no statistics behind.
- **Tunable** `analyze_new_indexes` off restores previous behavior, and back on
  resumes collection.

Not yet covered, worth adding:

- Plan quality — assert a query actually picks the new index immediately
  post-SC, rather than only asserting the stat rows exist.
- Ordering *during* the window — the cluster test confirms statistics are on
  every node once the schema change returns, which is the user-visible
  guarantee, but it does not catch a replicant mid-window. Proving the strict
  ordering would need a test that queries a replicant between the stats
  transaction and the schema-change transaction landing.
- Rootpage isolation under load — run an index-adding SC against concurrent
  client queries and assert no client ever observes a truncated schema.
  Exercise the reconciliation path directly by forcing `clnt->pPool` to the
  default pool.
- Back-to-back schema changes — two in sequence must each analyze their own
  `newdb` (guards the `rootpages_owner` case on a reused pool thread).
- Concurrent analyze — start a user `ANALYZE`, then run an index-adding SC, and
  confirm the SC succeeds with a clear skip log rather than failing.
- Large-table path — confirm sampling engages above the threshold and SC latency
  stays bounded.

### Runs

Four-node cluster (`~/bin/cluster`): `sc_analyze` passes, including the
all-nodes assertion — statistics for a newly created index are present on all
four nodes as soon as the schema change returns. `analyze`, `analyze_sample`,
`autoanalyze`, and `sc_addfield` also pass clustered.

Single node: `sc_analyze` passes, as do `analyze_sample`, `autoanalyze`,
`sc_addfield`, `sc_datacopy`, `sc_desc_idx`. `analyze.test` fails at `t04_01`
standalone, but that subtest drives analyze headroom on a *remote* node over
ssh, so it cannot work without a cluster — it fails identically on a clean
baseline (verified by stashing the change and rebuilding) and passes clustered.

Clustered runs were intermittently flaky (~1 in 3). Root-caused to two test
timing assumptions, both fixed; 8 consecutive clustered runs pass since:

- **Read routing.** `cdb2sql default` may route to any node, so a query issued
  immediately after a schema change can land on one that has not applied the
  stats transaction yet. That is replication lag, not an ordering violation --
  such a node has not seen the index either. `assert_stats_present` now polls.
- **ANALYZE refused during a schema change.** `do_analyze` bails on
  `get_schema_change_in_progress`, and on a cluster the previous alter can
  still be settling, so the test's own baseline `ANALYZE` silently did nothing.
  It now retries via `retry_in_loop`.

Neither was a product defect. Note both are cluster-only: single-node runs
serialise tightly enough to hide them.

To make future clustered failures diagnosable, the skip and success paths now
also emit `ctrace`, which reaches the per-node `.trc.c` files. Ordinary
`logmsg` output does **not** survive to the captured cluster node logs -- not
even `LOGMSG_WARN`, and not on passing runs -- so `logmsg` alone is not usable
for diagnosis here. Sample from a node:

```
analyze_sc_new_indexes: skip t1 (no indexes were built)
analyze_new_indexes: skip (tunable=0 physrep=0)
analyze_new_indexes: committed stats for 1 new index(es) on t2
```

Two cluster-only issues surfaced that single-node testing could not:
`put tunable` is node-local while the schema change runs on the master (the
test now uses `sendtocluster`), and the per-node query needed the same LIKE
handling as the rest of the test. Neither was a product defect.
