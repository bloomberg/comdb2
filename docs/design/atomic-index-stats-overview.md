# Index statistics at schema-change time — overview

A short version of [atomic-index-stats.md](atomic-index-stats.md), which has
the full reasoning, code references, and rejected alternatives.

## The problem

When you create or alter an index, it starts with **no statistics**. The query
planner falls back to generic guesses until someone runs `ANALYZE` (manually or
via autoanalyze). In the meantime it can pick bad plans: ignoring the new index,
choosing a poor join order, or misjudging how selective a value is.

Today a schema change only *invalidates* cached plans. It never collects stats.

## The idea

Collect statistics for new indexes **as part of the schema change**, so the
index has good stats the moment it becomes visible — on every node.

## Why timing matters

The obvious approach — run `ANALYZE` right after the schema change commits —
leaves a gap on replicants:

```
master:     [schema change commits] ........ [stats commit]
replicant:  [schema change arrives] -- query here gets a bad plan -- [stats arrive]
```

Replication is ordered, so we flip it: **commit the stats first, then finalize
the schema change.** Every replicant receives the stats before it can see the
index.

```
master:     [stats commit] [schema change commits]
replicant:  [stats arrive] [schema change arrives]   <- no gap
```

## How it works

During an `ALTER`/`CREATE INDEX`, comdb2 builds the new table in a temporary
copy (`newdb`) and only swaps it in at finalize. We analyze that copy in between:

1. **Convert records** — existing step; builds the new index files in `newdb`.
2. **Find the indexes that were built.** The schema-change plan marks each index
   as either reused or rebuilt (`ix_plan[i] == -1`). Only rebuilt ones need
   stats.
3. **Analyze just those indexes** against `newdb`, on a dedicated `scanalyze`
   thread pool, and commit the stats in their own transaction.
4. **Finalize** — existing step; the new index goes live, with stats already in
   place.

### What it covers

All of these go through the same path, so no special cases are needed:

| DDL | Stats collected? |
|-----|------------------|
| `CREATE INDEX` | Yes |
| `ALTER TABLE ... ADD INDEX` | Yes |
| Changing an index's columns/flags | Yes |
| `REBUILD INDEX` | Yes |
| `DROP INDEX` | No (nothing to analyze) |
| Adding a column (datacopy index rebuilt) | Yes — redundant but harmless |

Indexes the schema change **didn't** rebuild keep their existing stats
untouched.

## The tricky parts

Making SQLite analyze a table that isn't live yet took a few pieces:

- **Showing SQLite the new schema.** Done by swapping the rootpages the analyze
  runs with — see [Rootpage swapping](#rootpage-swapping) below.
- **Pointing reads at the new files.** Table lookups normally go by name, which
  would find the *old* table. `custom_dbtable` redirects them to `newdb`.
- **Using the final index names.** In-flight indexes are named `.NEW.IXA`; stats
  are written under the name the index will have after commit (`IXA`), or the
  planner would never find them.
- **Flushing first.** The sampler reads index files straight from disk, so newly
  written pages must be flushed or it sees an empty index.
- **Not tripping existing guards.** Analyze normally aborts if a schema change is
  running. That guard is skipped for this path only, since here the schema
  change *is* the one analyzing.

### Rootpage swapping

**What rootpages are.** SQLite refers to every table and index by a number (its
"rootpage"). comdb2 keeps a per-thread lookup table that maps each of those
numbers to a real comdb2 table and index. Whatever is loaded in that table
decides both *which tables SQLite can see* and *which files it reads*.

Normally every SQL thread loads the live schema. That's the problem: `newdb`
isn't live yet, so SQLite can't see the new indexes and has nothing to analyze.

**The swap.** The analyze client carries its own small rootpage table
(`custom_rootpages`) — the **three-table view** — containing only:

- `newdb` — the new copy of the table, with the new index; this is what gets
  scanned
- `sqlite_stat1` and `sqlite_stat4` — where the stats get written

While the analyze runs, those three are the only tables SQLite believes exist.
The stat tables are required: SQLite finds them by name, and if they're missing
analyze runs happily but silently writes nothing. Nothing else is included
because nothing else is touched — a smaller view is cheaper to copy and has less
that can go stale.

**Precedent: `sql_syntax_check`.** This isn't a new trick. comdb2 already shows
SQLite a not-yet-live `newdb` during schema changes, in `sql_syntax_check`.

That function runs during `CREATE`/`ALTER TABLE`, before commit, when the new
schema has things SQLite has to parse — partial-index `WHERE` clauses, index
expressions, or `CHECK` constraints. Its job is to catch a schema that comdb2
accepts but SQLite can't, so the schema change fails up front instead of
leaving a table nobody can query. It:

1. builds a `sqlite_master` entry for `newdb`,
2. loads a **one-table** rootpage view containing just `newdb` onto its thread,
3. opens a SQLite connection directly on that thread, and
4. runs `select 1 from sqlite_master limit 1`, which forces SQLite to parse the
   schema — a parse error means the schema is bad.

We reuse steps 1 and 2 (with the stat tables added). We can't copy the rest,
because `sql_syntax_check` is **read-only** and runs everything on its own
thread. Analyze **writes** stats, and those writes can only commit through the
normal query path — which hands each query to a pool thread and reloads the
live rootpages there, wiping out a view set up on the calling thread. That's
what forces the next two pieces.

**Why it lives on the client, not the thread.** Queries don't run on the thread
that issues them; they're handed to a thread pool. So the custom view has to
travel with the client, and whichever pool thread picks up the query loads it.

**Swapping back.** A pool thread only reloads rootpages when its SQL engine is
opened or refreshed — not on every query. Left alone, a thread that ran our
analyze would keep our three-table view and hand it to the next client. So
before every query, each thread checks whether its rootpages match the client:

| Thread currently has | Client needs | Action |
|---|---|---|
| Live schema | Live schema | Nothing (the normal case — two cheap checks) |
| Live schema | Custom view | Load the client's view |
| Custom view | Live schema | Restore the live schema |
| Custom view (another client's) | Custom view | Load this client's view |

The thread also remembers which client its view belongs to, so repeated queries
from the same client don't recopy it, and a later schema change reusing the
thread doesn't inherit the previous one's `newdb`.

### Why a separate thread pool

The analyze runs on its own small pool (`scanalyze`, 2 threads) instead of the
normal SQL pool that serves client queries.

If it used the normal pool, our analyze would land on the same threads as
client queries, and each of those threads would briefly hold our three-table
view. The swap-back logic above would restore it before the next client query,
but:

- **Correctness would depend on that logic being perfect.** Any gap means a
  client query runs against a schema containing three tables. With a separate
  pool, our view never touches a client-facing thread in the first place; the
  swap-back becomes a safety net instead of the only line of defense.
- **Client queries would pay for the restore.** Reloading the live schema is a
  copy of every table and index entry, and it would happen inside some
  unrelated client's query.

The pool can stay small: analyze is globally one-at-a-time, so at most one
inline analyze is ever using it. 2 threads is just a little headroom. The
swap-back logic stays in place anyway: if the pool ever fails to be created, the
analyze falls back to the normal pool.

## Safety

- **Never fails a schema change.** Any error is logged and the schema change
  proceeds without fresh stats — i.e. today's behavior.
- **Only one analyze at a time** (existing global rule). If a user or
  autoanalyze is already running, we skip and log.
- **Skipped on schema-change resume.**
- **Tunable:** `analyze_new_indexes` (default **on**) turns it off. Pool size is
  `sc_analyze_threads` (default 2).
- Coverage follows the table's configured analyze coverage, default 20%.

## Known limitations

- **Old rows after altering an index.** Changing an index's columns changes its
  internal name, so the old stats row is left behind. The planner never uses it
  (it looks up by the new name), and the next full `ANALYZE` cleans it up. This
  predates the change.
- **`analyze backout`** after a schema change restores the pre-change stats,
  undoing this for that table until the next `ANALYZE`.
- **Concurrent schema changes:** if two finish at nearly the same time, only one
  gets inline stats (the other skips because analyze is single-threaded).

## Testing

- `tests/sc_analyze.test` covers each DDL case above, checks that stats are
  present **on every node** right after the schema change, and that the tunable
  works. Passes standalone and on a 4-node cluster.
- `yast` (imported SQLite planner tests) runs with the feature off: its expected
  outputs assume new indexes have no stats. Nearly all its diffs were the
  planner simply getting better estimates.
