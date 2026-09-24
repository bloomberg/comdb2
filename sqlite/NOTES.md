# NOTES

## Setup

* To get the current and latest versions of stock SQLite:

  ```bash
  git clone https://github.com/sqlite/sqlite.git sqlite-3.28.0
  rm -rf sqlite-3.51.2
  cp -R sqlite-3.28.0 sqlite-3.51.2
  (cd sqlite-3.28.0 && git checkout version-3.28.0)
  (cd sqlite-3.51.2 && git checkout version-3.51.2)
  ```

* To apply upstream changes to a file:

  ```bash
  export file=src/select.c
  diff3 -m $file sqlite-3.28.0/$file sqlite-3.51.2/$file > $file.merged

  # Claude suggests that "git merge-file" creates fewer spurious conflicts.
  git merge-file --stdout --quiet --diff3 $file sqlite-3.28.0/$file sqlite-3.51.2/$file > $file.merged
  ```

  Fix merge conflicts.

* Replace current file with merged:

  ```bash
  rm -f $file.patch $file.md && mv $file.merged $file
  ```

## Prompts

* Initial context:

  ```text
  I'm upgrading SQLite in comdb2 from 3.28.0 to 3.51.2. Relevant paths:
  `sqlite/sqlite-3.28.0/` and `sqlite/sqlite-3.51.2/` are clean upstream SQLite
  checkouts; current comdb2 code is under `sqlite/`; build/reachability clues
  include `sqlite/definitions.cmake`, `SQLITE_BUILDING_FOR_COMDB2`, git history,
  and references in the repo.

  When I ask questions, use those sources first: compare upstream versions,
  check comdb2 diffs/history, and distinguish compiled/reachable code from dead
  or excluded code. Do not assume comdb2 patches are obsolete or invent
  rationale; clearly separate evidence, inference, and uncertainty.
  ```

## Hazards

Things that will not announce themselves. A merge may well complete cleanly
without raising any of these.

- **`sqlite3VdbeSerialType()` / `sqlite3VdbeSerialPut()` must stay in sync with
  `OP_MakeRecord`.**

  Upstream in-lined both routines into `OP_MakeRecord` and deleted them, so the
  logic now lives in two places: our vendored copies in `db/sqlglue.c`, and the
  dispatch written directly into the opcode in `sqlite/src/vdbe.c`. They encode
  the same records and must agree.

  Change one and you must change the other. Neither the compiler nor a future
  merge will tell you: the opcode and the copies are in different files, and
  upstream edits to `OP_MakeRecord` will conflict only against the opcode.

## Cherry-picked patches

These are patches we cherry-picked that seem to have shifted around or been
updated in latest SQLite. During merge, preference was given to incoming
changes, so need to be wary whether these optimizations / bug-fixes still work.

I started tracking this when I got to more important files (`where.c`,
`select.c`), so it's not comprehensive.

* `0b4e056d5`:

  ```md
  Patch from Richard which addresses the unnecessary btree hits for OP_NoHope
  ```

* `2e54f2247`:

  ```md
  The OP_SeekScan opcode works, but using it requires disabling the IN-earlyout
  optimization because the OP_IfNoHope opcode might move the cursor.
  ```

* `807c5cb81`:

  ```md
  Cherrypick SQLite window function fix {173629165}
  ```

* `c536ee2a6`

  ```md
  Port sqlite 1f97086d
  ```

* `a837502df`

  ```md
  sqlite: Cherry-pick upstream 3.28 fixes
  
  * Remove two incorrect assert() statements from the logic used to derive
    column names and types from subqueries. This allows the SQL associated with
    CVE-2020-1387 (ticket [c8d3b9f0a750a529]) to be tested.
  
  * Fix a defect in the query-flattener optimization identified by ticket
    [8f157e8010b22af0]. This fix is associated with CVE-2020-15358.
  ```

* `da7576559c`:

  ```md
  Fix SQLite cost estimate
  
  This is a cherry-pick from SQLite (https://www.sqlite.org/src/info/c7b34930e27597e7).
  
  (DRQS 166741103)
  ```

* `cf92790ff`:

  ```md
  Cherrypick of SQLite 3.28 branch performance enhancement [263293f1e6db2603]:
  Minor tweaks to query planning weights so that when STAT4 is enabled and
  functioning, a full table scan is more likely to be selected if that seems
  like the fastest solution. Only do this when STAT4 info is available because
  an error has a large potential downside.
  ```

* `d1642188f9`:

  ```md
  {174271358} sqlite: Query planner interstage heuristic
  
  https://www.sqlite.org/src/info/74b247d958d74782
  Add a heuristic in between the two solver() passes of the query planner that
  tries to prevent a very slow query plan in cases where the output row count
  estimate is imprecise.
  
  https://www.sqlite.org/src/info/357d9513d2bd13c4
  Fix typos in comments.  Provided ".wheretrace" debugging output for the
  interstage heuristic module.  Do omit automatic index loops in the
  interstage heuristic.
  
  Port whereN.test to Comdb2
  ```

* `6afa709ac`:

  ```md
  Revisiting the IN-scan optimization to try to fix it .. (#1)

  From 68cf0ace3d160c8b3c12ee692c337f0d47e079d7 Mon Sep 17 00:00:00 2001
  From: drh <drh@noemail.net>
  Date: Mon, 28 Sep 2020 19:51:54 +0000
  Subject: [PATCH] Revisiting the IN-scan optimization to try to fix it for the
   corner case where the statistics deceive the query planner into using a scan
   when an indexed lookup would be better.  This check-in changes the code
   generation to do the IN-scan using a new OP_SeekScan opcode.  That new opcode
   is designed to abandon the scan and fall back to a seek if it doesn't find a
   match quickly enough.  For this work-in-progress check-in, OP_SeekScan is
   still a no-op and OP_SeekGE still ends up doing all the work.
  ```

* `52a70498d`:

  ```md
  Cherry-pick SQLite's update/delete optimization
    
  https://sqlite.org/src/info/0ecda433718f0bc9
  ```

* `5dc0b6b3b`:

  ```md
  Fix a problem where the loop for the RHS of a LEFT JOIN uses .. (#2)
  
  From 74ebaadcdd8b1058bbc80bf61334fe09da1c64b5 Mon Sep 17 00:00:00 2001
  From: dan <dan@noemail.net>
  Date: Sat, 4 Jan 2020 16:55:57 +0000
  Subject: [PATCH] Fix a problem where the loop for the RHS of a LEFT JOIN uses
   values from an IN() clause as the second or subsequent field of an index.
  ```

* `7d7d64e1e`:

  ```md
  At the end of the right-hand table loop of a LEFT JOIN that uses an IN (#3)
  
  commit 14c98a4f4016bb60679535e3d2d9fe6c49bfe04a (HEAD)
  Author: drh <drh@noemail.net>
  Date:   Mon Mar 16 03:07:53 2020 +0000
  
      At the end of the right-hand table loop of a LEFT JOIN that uses an IN
      operator in the ON clause, put the OP_IfNoHope operator after the
      OP_IfNotOpen operator, not before, to avoid a (harmless) uninitialized
      register reference.  Ticket [82b588d342d515d1]
  ```

## TODO

- **TODO**: Revisit the vendored `sqlite3VdbeSerialType()` /
  `sqlite3VdbeSerialPut()` in `db/sqlglue.c` and see whether they can be cut
  down to comdb2's own subset of serial types.

  `OP_MakeRecord` is the only serial-type producer in the SQLite library, and
  in a comdb2 build it routes through our copies, so types 1-5, 8 and 9 look
  unreachable for us -- the whole variable-width integer search. If that holds,
  both routines shrink a long way and stop tracking upstream's shape. Worth
  proving properly, with a `default:` that asserts, once there is a build to
  test against.

  Rename them to `comdb2SerialType()` / `comdb2SerialPut()` at the same time.
  They kept their SQLite names only to avoid touching the call sites in
  `db/sqlglue.c`, `db/fdb_bend.c`, `db/fdb_fend.c` and `db/sqlmaster.c` before
  there was a way to test the result.

- **TODO**: Confirm `typessql` still stays off for statements that call a scalar
  function (`tests/typessql.test`, the fix in `4c00fcf29`).

  `db/sqlinterfaces.c` skips `typessql_initialize()` when `Vdbe.hasScalarFunc`
  is set, and upstream deleted the block that used to set it. It now sits just
  before `sqlite3VdbeAddFunctionCall()` in the `TK_FUNCTION` case -- the same
  emit point, but only once `func.c` is merged.

## Observations and Decisions

### General

- We have cherry-picked specific extensions from SQLite rather than support all
  of them, presumably to reduce maintenance surface area.

### `alter.c`

- Disable SQLite's logic because we handle much of DDL ourselves.

### `analyze.c`

- **DECISION**: Don't let the stat4 sample tunables re-enable collection that
  the connection has disabled.

  `stat_init()` sets `mxSample` to 0 when the `SQLITE_Stat4` optimization bit is
  clear on the connection, and 0 means "collect stat1 only".
  `stat4_samples_multiplier` assigns over `mxSample` and `stat4_extra_samples`
  adds to it, so either one on its own would quietly bring sampling back. Both
  now run only when `mxSample` is already non-zero: the tunables control how
  many samples we take, not whether we take any.

- **DECISION**: Adopt upstream's 1.0-1.1 rounding rule in our fork of the
  `sqlite_stat1` loop.

  Each figure in a stat1 row is rows-per-distinct-prefix. Previously, if a
  figure was between 1.0 and 2.0, it would be rounded up to 2.0. This
  rounding-up turned a near-unique column into 2, and the planner would read 2
  as "twice as many rows as a unique index would give" — enough to make it
  prefer a different index. A column with 999 distinct values in 1000 rows is
  unique for planning purposes; 2 misprices it by 2×. Upstream's rule rounds
  back down to 1 when the true ratio is under 1.1. Our fork of the loop exists
  to use our own row count and to iterate `nCol-1` in place of `nKeyCol`, not to
  change the estimator, so the rule belongs on both sides.

- **OBSERVATION**: `StatAccum.nActualRow` and upstream's `nEst` are different
  quantities, not duplicates. For large tables comdb2's `ANALYZE` walks a
  sampler rather than the index itself (`bdb_summarize_table()`), so `OP_Count`
  returns the number of rows sampled while `analyze_get_nrecs()` returns the
  number the table really holds. `sqlite_stat1` reports the latter, and stat4's
  per-column counts are scaled by the ratio between the two. This is why
  `stat_init()` needs a fifth argument that upstream has no equivalent for.

- **OBSERVATION**: Upstream needs to pass both N and K because `N-K` varies by
  index shape. comdb2 doesn't, because only one of the three shapes can exist
  here.

  K is always `pIdx->nKeyCol`. N is not:

  | index shape | N | relation |
  |---|---|---|
  | index on a rowid table | `nColumn` — the key plus the rowid | N = K+1 |
  | secondary index on a WITHOUT ROWID table | `nColumn` — the key plus the P primary key columns | N = K+P |
  | the primary key index of a WITHOUT ROWID table | `nKeyCol` | N = K |

  The third is the index that *is* the table, so its `nColumn` is the full table
  width; `analyzeOneTable()` takes `nKeyCol` as N instead, to keep
  `sqlite_stat1` describing the primary key rather than every column. K is what
  tells `statGet()` how many figures a `sqlite_stat1` row carries, and `N-1`
  gives the wrong answer for the lower two shapes.

  comdb2 can only produce the first. `parse.y` puts the whole
  `table_option_set` production behind `%ifndef SQLITE_BUILDING_FOR_COMDB2`, so
  `WITHOUT ROWID` does not parse and `convertToWithoutRowidTable()` is
  unreachable. Every index we analyze carries the genid, so N = K+1 always.

- **DECISION**: Pass K rather than deriving it from N.

  `statInit()` takes K in `argv[1]` and stores it in `StatAccum.nKeyCol` as
  upstream does, and `statGet()`'s `sqlite_stat1` loop is bounded by
  `p->nKeyCol` rather than `p->nCol-1`.

  We pass our own key-column count as K, not `pIdx->nKeyCol`. `pIdx->nKeyCol`
  counts DATACOPY columns, which sit in the key's tail as payload and are not
  searchable. The local `nCol` in `analyzeOneTable()` is that count already
  truncated at the first DATACOPY column, so K is `nCol` and N is `nCol+1`.

  Deriving K as `N-1` also works here — per the observation above, only the
  rowid shape exists in comdb2, so `N = K+1` always. Passing it instead keeps
  upstream's `assert(nKeyCol<=nCol)` and `assert(nKeyCol>0)` live, which are
  the only check we have on the DATACOPY truncation, and leaves the stat1 loop
  textually identical to upstream's, so a future change to the estimator
  applies as a copy rather than a translation.

- **DECISION**: `stat_init()` takes comdb2's extra row count as a fifth
  argument, leaving upstream's four in their own positions.

  Upstream passes (N, K, C, L): index columns, key columns, `OP_Count` of the
  index, and `PRAGMA analysis_limit`. comdb2 adds A, `analyze_get_nrecs()`, in
  register `regStat+5`.

  `OP_Count` always asks for an exact count (P3 = 0) because
  `sqlite3BtreeRowCountEst()` is unimplemented for our btree.

- **DECISION**: Fold `analyze_empty_tables` into upstream's partial-index
  condition rather than keeping it as a separate patch.

  Stock SQLite writes nothing to `sqlite_stat1` for an empty index. To the
  planner an absent row means "unknown", not "empty", so it falls back to its
  built-in guess of a large table. The tunable makes an empty index still get a
  row, which our `statGet()` writes as `MAX(nRow, 1)` — the planner then costs
  the table as tiny instead of huge.

  3.51 added the same trick for partial indexes. One behavior change does come
  with 3.51: an empty *partial* index now gets a `sqlite_stat1` row even with
  the tunable off.

- **DECISION**: Keep our own second `OP_Rewind` for the stat4 skip; upstream's
  replacement cannot work here.

  Once the empty-index jump is rerouted to the `sqlite_stat1` write, something
  else has to skip the stat4 sample loop. Upstream re-derives that skip from the
  stat1 row itself: `OP_Cast regStat1` then `OP_IfNot regStat1`, which fires when
  the row count is 0. Our `statGet()` writes `MAX(nRow, 1)`, so that jump can
  never fire for us. `addrRewind` is a second `OP_Rewind`, opened and patched
  inside the same `if( analyze_empty_tables )` pair, and it is a distinct
  address from upstream's `addrGotoEnd`.

  Upstream's `OP_IfNot` is still emitted and still patched — a dead opcode, kept
  so its `p2` points somewhere sane rather than at address 0.

- **DECISION**: Split the stat4 sample loop into a single `#if/#else` instead of
  four interleaved guards.

  Upstream re-reads each sampled row out of the table: fetch the rowid, seek to
  it, load every index column, pack a record. We have no rowid — our sample is
  the packed index row the accumulator already holds — so we fetch it with
  `STAT_GET_ROW` and never touch the table cursor. The loop-exit test differs to
  match: upstream tests the rowid for NULL, we test `regEq`, since `statGet()`
  returns NULL for `STAT_GET_NEQ` once the samples run out.

  The two paths share only three `callStatGet()` calls. Duplicating those on our
  side buys an unbroken twelve-line copy of upstream's, which merges clean.

- **OBSERVATION**: `PRAGMA analysis_limit` is now fully wired for comdb2. The
  limit already reached `stat_init()`, and `statPush()` already returned the
  skip-ahead signal, but nothing read that signal; 3.51's call site does. The
  default is 0, which selects the plain `OP_Next` and leaves codegen unchanged.

### `CMakeLists.txt`

- Certain files are generated at build time (see `tool` directory and comments
  in `CMakeLists.txt`).
- Reorganized `CMakeLists.txt` to clarify which files are stock SQLite vs.
  Comdb2-related.

### `decimal.h` (decimal math)

- Looks like we use the `decNumber` library for decimal math
  - Written by someone at IBM
  - https://github.com/dnotq/decNumber
  - `src/decimal.h` typedefs `decQuad` to `sql_decimal_t`
- However SQLite now includes a decimal extension (`ext/misc/decimal.c`)
  - https://sqlite.org/floatingpoint.html#the_decimal_c_extension
  - Stores decimals as strings, supports arbitrary precision math
- It looks like the `decNumber` library is _way_ more comprehensive
  - Tries to be more space efficient
  - More focused on being performant
  - Wider variety of operations support
  - Fully implements the IEEE 754 decimal arithmetic spec
- https://tutti.prod.bloomberg.com/comdb2/programming/decimals
  - Looks like we specifically only support `decimal32`, `decimal64`, and
    `decimal128`, hence only typedef'ing `decQuad`.
  - The conceptual representation is `sign * significand * (10 ^ exponent)`,
    where each of decimal type can store a different number of digits for
    the significand and exponent.

### `expr.c`

- **DECISION**: Drop our `EP_Generic` patch in `sqlite3ExprAffinity()`; take
  upstream.

  Nothing sets `EP_Generic` in 3.51. SQLite removed the flag from `sqliteInt.h`
  along with the rewrite rule it served in the optimizer -- `x IN (y)` ->
  `x==y` (removed in `790b37a240`). The `sqlite3ExprSkipCollate()` call in the
  hunk is not lost: upstream inlined it into `sqlite3ExprAffinity()`
  (`a7d6db6ac0`) and has since widened it to also check `EP_Skip|EP_IfNullRow`.

- **DECISION**: In `sqlite3ExprCodeGetColumnOfTable()`, take upstream's
  placement of the `sqlite3ColumnDefault()` call (inside the `else`, no
  `if( iCol>=0 )`) but keep our `-1` register argument.

  `-1` is what suppresses `OP_RealAffinity`; `sqlite3ColumnDefault()`'s
  `iReg>=0` guard (`update.c`) is a comdb2 fork. The narrower placement costs
  nothing: for an `INTEGER PRIMARY KEY` the branch emits `OP_Rowid`, which
  ignores the P4 default that is all the call could still append.

- **DECISION**: In the `EP_FixedCol` branch of `sqlite3ExprCodeTarget()`, take
  upstream and widen `zAff[]` to cover every comdb2 affinity.

  That branch makes constant propagation safe across types:

  ```sql
  CREATE TABLE t1(a INT, b TEXT);
  INSERT INTO t1 VALUES(123,'0123');
  SELECT * FROM t1 WHERE a=123 AND b=a;     -- 1 row
  SELECT * FROM t1 WHERE a=123 AND b=123;   -- 0 rows
  ```

  `b=a` compares numerically but `b=123` compares as text, so the optimizer
  cannot simply substitute the value of `a`. It keeps the node `a` column, tags
  it `EP_FixedCol`, and codes the constant with `a`'s affinity -- and `zAff[]`
  is the affinity-to-`P4`-string lookup that applies it.

  Our only change here has ever been to give it a row per comdb2 data type, so
  the resolution is take-theirs with all of ours covered, `SQLITE_AFF_FLEXNUM`
  included (it arrived with `sqliteInt.h` after the table was last widened).

### `fwd_types.h`

- I guess we need to be able to refer to some SQLite structures in `db/` code.

### `global.c`

- Disable SQLite page cache because we use BerkeleyDB.

### `loadext.c`

- Some extension APIs are disabled because they're not consistent between
  SQLite and comdb2 semantics.

### `malloc.c`

- Implement thread-safe versions of `sqlite3DbMalloc` and `sqlite3DbRealloc`.
  These thread-safe variants only seem to be used for fdb, maybe because SQLite
  resources are shared for concurrent queries that reference the same foreign
  db?

### `md5.{h,c}`

- Adapted from SQLite's implementation in `src/test_md5.c`. Seems that SQLite
  does not expose this by default, maybe only when built with test flags.

### `mem1.c`

- Use `blobmem` (specialized blob memory allocator?) when allocating large
  amounts of memory. Maybe more optimized or has fewer problems with
  fragmentation, etc.

### `parse.y`

- The tokenizer (`tokenize.c`) converts a flat string of characters into a
  sequence of tokens, including: keywords (`SELECT`, `INSERT`), identifiers,
  `TK_SEMI`, `TK_SPACE`, etc. Other systems may refer to the tokenizer as a
  "lexer". The tokens are fed one at a time into the parser. `parse.y` specifies
  the grammar, i.e. what actions to take when a particular sequence of tokens is
  encountered. The parser maintains a stack of tokens. When the tokenizer feeds
  a new token into the parser, the parser **shifts** or pushes it onto a stack.
  If the top of the stack satisfies a grammar rule, the parser **reduces** or
  collapses those stack entries into the left-hand side of the grammar rule.

- How does the tokenizer classify ambiguous tokens? It "cheats" a little by
  keeping track of `lastTokenParsed` or calling `getToken(...)` to peek at the
  next token - this can be seen in functions like `analyzeOverKeyword`. The
  parser does NOT feed information back into the tokenizer.

- How does the parser handle ambiguous tokens? For example, it needs to be able
  to tell that in `SELECT begin FROM t`, `begin` is an identifier, not the
  `BEGIN` keyword. This is handled through `%fallback` lists in the grammar.
  When the parser sees a token from this list and the token cannot be used as a
  keyword in the current position of the statement being consumed, it attempts
  to parse as if the token is an identifier instead. The parser attempts to be
  **lenient** in this regard.

- **DECISION**: Keep the `if( pParse->pReprepare==0 )` guards for `explain`
  rules. These were introduced alongside the addition of
  `sqlite_stmt_explain(S, E)`, which allows the caller to change the explain
  mode or treat an `EXPLAIN ...` statement as a normal query.
  `sqlite_stmt_explain` internally reprepares the statement, so the guards are
  necessary to prevent the user-supplied state from being clobbered when
  re-parsing the query.

- **DECISION**: Keep the `SEMI` token in `explain` rules. On EOL, the tokenizer
  synthesizes `TK_SEMI`, so this does not break existing `EXPLAIN` statements
  that do not end in a semicolon. This just makes the `explain` rule consistent
  with other rules.

- **DECISION**: Disable `STRICT` mode for `CREATE TABLE`.

- **DECISION**: Accept `NULLS FIRST / LAST` syntax in a manner that is
  consistent with SQLite - allow it for `ORDER BY` whenever it appears in a
  query, disallow it for DDL with a call to `sqlite3HasExplicitNulls()`.
  `comdb2AddIndexInt()` is called from all paths that create indexes:
  `comdb2AddIndex()`, `comdb2AddPrimaryKey()`, and `comdb2CreateIndex()`.

- **DECISION**: Light the `isCreate` flag when writing to
  `u1.cr.constraintName`. It is a debug-only flag used to enforce the invariant
  that `u1.cr` members should only be accessed for `CREATE TABLE ...` and
  `ALTER TABLE ADD COLUMN ... CONSTRAINT x ...`. `u1.d` members are only active
  when a `RETURNING` clause is present in a query (i.e. never during DDL).
  SQLite always sets `isCreate` by calling `disableLookaside()` _before_ writing
  to `constraintName`. Do the same by calling `disableLookaside()` when reducing
  `ALTER TABLE`. `ASSERT_IS_CREATE` before writing to `constraintName`.

- **DECISION**: Migrate the comdb2 `DEFAULT` constraint rules from the 3.28-era
  `scanpt` grammar shape to upstream's `scantok` shape, even in the live
  `%ifdef SQLITE_BUILDING_FOR_COMDB2` branch. `comdb2AddDefaultValue` now receives
  the token-span pointers (e.g. `A.z, &A.z[A.n]`) instead of raw `scanpt` scan
  pointers. This is functionally equivalent for the single-token `term`
  productions, and keeping the same grammar shape as upstream reduces friction on
  future SQLite upgrades.

- **DECISION**: Keep `UPDATE ... FROM ...` disabled for comdb2. Upstream's shared
  UPDATE reduce action references the `from(F)` clause, but the comdb2 UPDATE
  production has no `from(F)` symbol. Wrap the FROM-handling block in
  `%ifndef SQLITE_BUILDING_FOR_COMDB2` (in *both* the `SQLITE_ENABLE_UPDATE_DELETE_LIMIT`
  and the `%else` branches) so it is excluded from the comdb2 build — otherwise
  lemon emits a reference to an undefined `F` and `parse.c` fails to compile.
  Leaving `UPDATE ... FROM` disabled until we work out what the execution layer
  needs to support it.

- **DECISION**: Bring the `%ifndef SQLITE_BUILDING_FOR_COMDB2` parity branches of
  `CREATE INDEX` and `DROP INDEX` back in line with pure upstream. They had
  accidentally picked up the comdb2 `dryrun` prefix; removed it so the parity
  (upstream-tracking) branch is verbatim 3.51.2.

- **OBSERVATION**: The `dryrun` prefix (a comdb2 nullable nonterminal) is inserted
  in-place into several *upstream* statement rules rather than added as separate
  `%ifdef`-guarded comdb2 rules: `DROP TABLE` (refactored to
  `drop_table ::= dryrun DROP TABLE ...`), `CREATE VIEW`, `DROP VIEW`,
  `CREATE TRIGGER`, `DROP TRIGGER`, and `create_vtab`. This is a known deviation
  from the "only add `%ifdef`-guarded lines to upstream" convention — these edits
  modify the upstream rule lines directly. Left as-is for now: converting each to
  a guarded fork would be a large, churny change for little benefit (`dryrun` is
  nullable, so the upstream syntax still parses).

- **DECISION**: Disable `RETURNING`, don't allow it to parse at all.

- **DECISION**: Allow CTE `AS [NOT] MATERIALIZED` to flow in - it's
  just an optimizer hint and this optimization may be performed
  as of today regardless.

### `random.c`

- We've made the random number generator thread-local instead of global to
  reduce contention amongst SQL threads.

### `rowset.c`

- We have modified SQLite's `RowSet` to use BerkeleyDB temp tables to better
  handle large update/delete/etc. queries. See Rivers's commit `c026b966e`.

### `shell.c.in`

- We have a patch to initialize comdb2's SQLite tunables in our build of the
  SQLite shell.
- Dropped this file entirely.

  ```
  commit 760b68a100908af2ab3355eb5ff186415707f40e
  Author: Shoumyo Chakravorti <schakravorti@bloomberg.net>
  Date:   Mon Aug 10 16:34:47 2026 +0000

      `shell.c`/`shell.c.in`: drop unbuilt `sqlite3` CLI

      These files were introduced in our source tree in previous SQLite upgrades but
      we currently don't build them. In the latest SQLite upgrade, these files have
      humongous diffs (+6,870 -2,294). Getting this to build is a low priority.
      Remove the files to avoid leaving things in an inconsistent/confusing state.

      Signed-off-by: Shoumyo Chakravorti <schakravorti@bloomberg.net>
  ```

### `sqlite_btree.{h,c}`

- We have completely replaced the B-Tree routines with our own to interact with
  BerkeleyDB instead. The functions are defined in `db/sqlglue.c`.

- **DECISION**: Add an extra `bias` argument to `sqlite3BtreeIndexMoveto()` in
  comdb2 builds, holding the VDBE opcode that issued the seek.

  Upstream split `sqlite3BtreeMovetoUnpacked()` into table and index variants;
  the table variant kept an int slot (`biasRight`) that comdb2 already
  overloads with the opcode, but the index variant didn't keep one. In comdb2's
  implementation of `sqlite3BtreeMovetoUnpacked()` for index cursors, the
  opcode selects the search direction, the duplicate-key resolution, the
  SELECTV key-range recording, and the `OP_IdxDelete` path that ships an index
  key to the master instead of seeking. We need to continue plumbing down the
  VDBE opcode to maintain these behaviors, hence the additional argument to
  `sqlite3BtreeIndexMoveto()`.

  Note - this doesn't add new information that wasn't being passed down before,
  it's reviving the slot that was used for this purpose and subsequently
  removed upstream.

### `sqlite_tunables.{h,c}`

- We have defined some of our own tunables for SQLite.

### `sqlite3.h` / `sqlite.h.in`

- Upstream *generates* `sqlite3.h` from `src/sqlite.h.in` via
  `tool/mksqlite3h.tcl`, and so do we. `src/sqlite.h.in` is the file we patch;
  `sqlite3.h` is built into `${PROJECT_BINARY_DIR}/sqlite` and is **not** in
  the source tree. We used to check in the generated header and patch it
  directly, which meant maintaining the same patch set twice -- and the two had
  drifted by thirteen changes.

- `tool/mksqlite3h.tcl` is verbatim upstream -- we carry no patch to it. Files
  are checked in solely so it runs unmodified: `manifest`, `manifest.uuid`,
  `manifest.tags` and `tool/mksourceid.c` for the source-id, datetime and
  branch/tags; and `ext/rtree/sqlite3rtree.h`, `ext/session/sqlite3session.h`
  and `ext/fts5/fts5.h`, which it inlines into the header even though we build
  none of those extensions. Since our sources are patched, `mksourceid` reports
  the source-id with an `alt1` suffix, which is fine. Nothing reads it
  functionally.

- Consumers outside `sqlite/` need the build directory on their include path
  *and* an ordering edge to the generator, since CMake does not track generated
  files across directories. Both come from linking the `sqlite3_header`
  INTERFACE library. It deliberately does not pull in `sqlite`, which depends
  on `bdb`, which includes `<sqlite3.h>` -- that would be a cycle. Anything
  depending on `sqlite` inherits the ordering already; some that do not are
  `bdb`, `schemachange` and the five plugins (every plugin includes
  `bbinc/comdb2_plugin.h`, hence the link in `cmake/plugin.cmake`).

- `tests/tools` and `tools/pmux` use the *system* SQLite, not ours.

#### Upgrade procedure for `sqlite3.h`

1. Cut our delta against the new upstream template:

   ```sh
   diff -u sqlite/sqlite-<new>/src/sqlite.h.in sqlite/src/sqlite.h.in
   ```

2. Overwrite `sqlite/src/sqlite.h.in` with the new upstream copy and re-apply
   that delta. Copy the rest across unedited:

   ```sh
   cd sqlite

   cp sqlite-<new>/VERSION \
      sqlite-<new>/manifest \
      sqlite-<new>/manifest.uuid \
      sqlite-<new>/manifest.tags \
      .

   cp sqlite-<new>/tool/mksourceid.c \
      sqlite-<new>/tool/mksqlite3h.tcl \
      tool/

   for h in ext/rtree/sqlite3rtree.h ext/session/sqlite3session.h ext/fts5/fts5.h;
     do cp sqlite-<new>/$h $h;
   done
   ```

3. Check every re-applied hunk is inside a `SQLITE_BUILDING_FOR_COMDB2` guard,
   and drop any comdb2 backport upstream has since mainlined.

Verify by building `generate_sqlite3_h` and diffing the header against the
previous release's; then `cc -fsyntax-only -Wall` it both with and without
`-DSQLITE_BUILDING_FOR_COMDB2`.

### `sqliteInt.h`

- We define additional affinity types for new datatypes like datetime,
  interval, etc.
- We had a cool Lingzhi patch to make `SQLITE_KEEPNULL` work in spite of our
  additional affinity types. Latest SQLite doesn't have `SQLITE_KEEPNULL` at
  all, removed the patch. **This means we have one less
  `#ifdef SQLITE_BUILDING_FOR_COMDB2` block now** (127 vs. 128).
- `SF_ASTIncluded`. We have added a `selFlag` related to parallel SQL execution
  that now conflicts with SQLite's own `SF_PushDown`. My only concern would be
  if changing the flag would break like fdb queries or something, but I don't
  think the AST itself is serialized, so maybe the actual flag value doesn't
  matter? In that case it should be safe to just pick a different flag value...

### `status.c`

- We do not use SQLite's page caching, so disable assertions that check whether
  page cache mutex is held, and never try to acquire page cache mutex.

### `trigger.c`

- Our `sqlite_master` has an additional `csc2` column, needs to be set to NULL
  for triggers. Also, `MASTER_NAME` (`"sqlite_master"`) has been renamed to
  `LEGACY_SCHEMA_TABLE`.

### `vdbe.c`

- **TODO**: Make `OP_MakeRecord` call `sqlite3VdbeSerialType()` and
  `sqlite3VdbeSerialPut()` instead of carrying its own copy of the dispatch.

  Upstream in-lined both routines into the opcode, so taking its version
  verbatim means comdb2's serial types live in two places. Restoring the calls
  puts them back in one, and retires the sync hazard recorded under "Hazards".

### `vdbeaux.c`

- **DECISION**: Move `sqlite3VdbeSerialType()` and `sqlite3VdbeSerialPut()` out
  of `vdbeaux.c` into `db/sqlglue.c`, and leave `vdbeaux.c` reading as stock.

  Upstream in-lined both into `OP_MakeRecord`: `SerialType` survives only as an
  `#if 0` block kept for reference, and `SerialPut` was deleted outright.
  Neither has a caller left in the SQLite library. Comdb2 still packs records
  outside the VDBE -- `db/sqlglue.c`, `db/fdb_bend.c`, `db/fdb_fend.c` and
  `db/sqlmaster.c` -- so we own copies rather than keep patching a file upstream
  has finished with. `vdbeaux.c` takes upstream's text for both: the `#if 0`
  block is stock 3.51, and the `SerialPut` conflict resolves take-theirs. The
  prototypes stay in `vdbeInt.h`, guarded, so every existing caller keeps seeing
  them.

  `sqlite3VdbeSerialTypeLen()`, `sqlite3VdbeSerialGet()` and
  `sqlite3SmallTypeSizes[]` stay put and stay patched -- upstream still uses all
  three. The comdb2 `#include <arpa/inet.h>` / `<flibc.h>` and the
  `END_INLINE_SERIALGET` marker also stay, despite sitting on the take-theirs
  side of the conflict: `SerialGet()` needs the byte-order helpers, and dropping
  the marker would unbalance `tool/mkvdbeauxinlines.tcl`.

  The copies went over as-is, guards and all, rather than being rewritten for a
  comdb2-only file. There is no build to test against yet; see the TODO about
  simplifying them.

- **DECISION**: Serialize `MEM_IntReal` as a real rather than an integer.

  Resolved while the function was still in `vdbeaux.c`; the resolution travelled
  with it to `db/sqlglue.c`. Our fork of the integer arm exists to fix the width
  at 8 bytes, so that `sqlite3VdbeSerialPut()` can write the whole `i64` at once
  instead of SQLite's variable-width encoding. That settles width, not type.
  Upstream decides int-versus-real by width -- it keeps the real once the value
  costs 8 bytes either way -- and for comdb2 that is every value, so
  `MEM_IntReal` gets serial type 7. The value has to move from `pMem->u.i` to
  `pMem->u.r` first, since `sqlite3VdbeSerialPut()` reads `u.r` for type 7.

### `vdbeInt.h`

- Same bit used for `MEM_Comdb2` and `MEM_Term`. Safe because they are mutually
  exclusive. Former used to pass through data converted from client format into
  our row format, skipping SQLite format. Latter says whether string is
  null-terminated, but only relevant when data is in SQLite format (so would
  never be used/inspected if data is in our row format).
