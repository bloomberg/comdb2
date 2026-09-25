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

Problems that won't show up as a merge conflict or a compiler error. A merge
can finish cleanly and still break one of these.

- **Keep `sqlite3VdbeSerialType()` / `sqlite3VdbeSerialPut()` in sync with
  `OP_MakeRecord`.**

  Upstream deleted these two functions and copied their logic straight into
  `OP_MakeRecord`. We still need the functions, so we keep our own copies in
  `db/sqlglue.c`. The same record-encoding logic now exists in two places:
  `db/sqlglue.c` and `OP_MakeRecord` in `sqlite/src/vdbe.c`. Both must produce
  identical records.

  If you change one, change the other. Nothing will warn you. They're in
  different files, so the compiler can't catch a mismatch, and an upstream
  change to `OP_MakeRecord` will only conflict in `vdbe.c`.

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

- **TODO**: Simplify our copies of `sqlite3VdbeSerialType()` /
  `sqlite3VdbeSerialPut()` in `db/sqlglue.c`.

  In a comdb2 build, `OP_MakeRecord` is the only code in SQLite that picks
  serial types, and it goes through our copies. Our copies always store
  integers as 8 bytes, so serial types 1-5, 8 and 9 (the shorter integer
  encodings) should never come up. If that's true, most of the code in both
  functions can go, and they no longer need to match upstream's layout.
  Confirm it once the tree builds, e.g. with a `default:` case that asserts.

  Rename them to `comdb2SerialType()` / `comdb2SerialPut()` at the same time.
  They only kept the SQLite names so we didn't have to change the callers in
  `db/sqlglue.c`, `db/fdb_bend.c`, `db/fdb_fend.c` and `db/sqlmaster.c` before
  we could test anything.

- **TODO**: Check that `typessql` is still skipped for statements that call a
  scalar function (see `tests/typessql.test` and the fix in `4c00fcf29`).

  `db/sqlinterfaces.c` skips `typessql_initialize()` when `Vdbe.hasScalarFunc`
  is set. Upstream deleted the code block that used to set it, so we moved our
  call to just before `sqlite3VdbeAddFunctionCall()` in the `TK_FUNCTION` case
  of `expr.c`. That's where the flag was set before, but it can't be tested
  until `func.c` is merged and the tree builds.

- **TODO**: Check whether the `sqlite3MemCompare()` `MEM_Small` fix changes the
  expected output of any test.

  Comparing a `float` column with a string or blob that isn't a number used to
  return an arbitrary result. It now sorts the way upstream does (numbers
  before text and blobs). Any test whose expected output recorded the old
  result will now fail. We can't run tests until the tree builds.

## Observations and Decisions

### General

- We have cherry-picked specific extensions from SQLite rather than support all
  of them, presumably to reduce maintenance surface area.

### `alter.c`

- Disable SQLite's logic because we handle much of DDL ourselves.

### `analyze.c`

- **DECISION**: The stat4 sample tunables must not turn sampling back on when
  the connection has turned it off.

  If the `SQLITE_Stat4` optimization is disabled on the connection,
  `stat_init()` sets `mxSample` to 0, which means "collect stat1 only".
  `stat4_samples_multiplier` overwrites `mxSample` and `stat4_extra_samples`
  adds to it, so either tunable could make `mxSample` non-zero again and
  re-enable sampling. We now apply both only when `mxSample` is already
  non-zero. The tunables change the number of samples, but they don't decide
  whether sampling happens.

- **DECISION**: Use upstream's rounding rule for stat1 figures in our copy of
  the `sqlite_stat1` loop.

  Each number in a stat1 row is the average number of rows per distinct key
  prefix. We used to round any value between 1.0 and 2.0 up to 2.0. That made
  a nearly unique column look like it had 2 rows per value, and the planner
  would treat it as twice as expensive as a unique index, which could make it
  choose a different index. For example, a column with 999 distinct values in
  1000 rows is effectively unique, but we reported it as 2. Upstream rounds
  down to 1 when the real value is under 1.1, and we now do the same.

  Our copy of the loop only exists so we can use our own row count and loop
  over `nCol-1` instead of `nKeyCol`. It was never meant to change the
  estimate, so it should round the same way upstream does.

- **OBSERVATION**: `StatAccum.nActualRow` and upstream's `nEst` hold different
  values; one is not a duplicate of the other. For a sampled index, `nEst` is
  the number of rows sampled and `nActualRow` is the number of rows in the
  whole index. See the entry below on `stat_init()`'s fifth argument.

- **OBSERVATION**: Upstream passes both N (the number of index columns) and K
  (the number of key columns) because the gap between them depends on the kind
  of index. comdb2 only has one kind of index, so the gap is always 1.

  K is always `pIdx->nKeyCol`. N depends on the index:

  | index shape | N | relation |
  |---|---|---|
  | index on a rowid table | `nColumn` — the key plus the rowid | N = K+1 |
  | secondary index on a WITHOUT ROWID table | `nColumn` — the key plus the P primary key columns | N = K+P |
  | the primary key index of a WITHOUT ROWID table | `nKeyCol` | N = K |

  In the third case the index is the table itself, so its `nColumn` counts
  every column in the table. `analyzeOneTable()` uses `nKeyCol` as N instead,
  so that `sqlite_stat1` describes only the primary key columns. `statGet()`
  uses K to decide how many numbers go in a `sqlite_stat1` row, and `N-1`
  would give the wrong count in the second and third cases.

  comdb2 can only create the first kind. `parse.y` wraps the whole
  `table_option_set` rule in `%ifndef SQLITE_BUILDING_FOR_COMDB2`, so
  `WITHOUT ROWID` doesn't parse and `convertToWithoutRowidTable()` is never
  called. Every index we analyze ends with the genid, so N = K+1 always.

- **DECISION**: Pass K to `stat_init()` explicitly instead of computing it as
  N-1.

  As in upstream, `statInit()` reads K from `argv[1]` and stores it in
  `StatAccum.nKeyCol`, and the `sqlite_stat1` loop in `statGet()` runs up to
  `p->nKeyCol` instead of `p->nCol-1`.

  The K we pass is our own count of key columns, not `pIdx->nKeyCol`.
  `pIdx->nKeyCol` includes DATACOPY columns, which are stored at the end of
  the key as extra data and can't be searched on. The local variable `nCol` in
  `analyzeOneTable()` already stops counting at the first DATACOPY column, so
  we pass `nCol` as K and `nCol+1` as N.

  Computing K as N-1 would also give the right answer, since N = K+1 always
  holds in comdb2 (see above). We pass it anyway for two reasons. It keeps
  upstream's `assert(nKeyCol<=nCol)` and `assert(nKeyCol>0)` working, and
  those asserts are the only check that we cut off the DATACOPY columns
  correctly. It also keeps our stat1 loop identical to upstream's, so future
  upstream changes to it can be copied over directly.

- **DECISION**: Pass comdb2's real row count to `stat_init()` as a fifth
  argument, after upstream's four, so upstream's arguments keep their
  positions.

  Upstream passes four arguments: N (index columns), K (key columns), C (the
  `OP_Count` of the index), and L (`PRAGMA analysis_limit`). For a large
  table, comdb2's `ANALYZE` doesn't read the whole index.
  `bdb_summarize_table()` picks a random subset of its pages, and `OP_Count`
  only counts the rows on those pages. So we also pass A, the number of rows
  in the whole index from `analyze_get_nrecs()`, in register `regStat+5`. A is
  used as the total row count in the `sqlite_stat1` row, and stat4's counts
  are multiplied by A/C to scale them up to the whole table. If the index
  wasn't sampled, `analyze_get_nrecs()` returns -1 and C is used as-is.

  `OP_Count` always asks for an exact count (P3 = 0), because
  `sqlite3BtreeRowCountEst()` isn't implemented for our btree.

- **DECISION**: Merge the `analyze_empty_tables` tunable into upstream's
  partial-index check instead of keeping it as a separate patch.

  Stock SQLite writes no `sqlite_stat1` row for an empty index. The planner
  treats a missing row as "unknown" rather than "empty", and assumes the table
  is large. With `analyze_empty_tables` on, an empty index still gets a row.
  Our `statGet()` writes its row count as `MAX(nRow, 1)`, so the planner
  treats the table as tiny instead of huge.

  3.51 does the same thing for partial indexes, so we added our tunable to
  that check. This changes one behavior: an empty *partial* index now gets a
  `sqlite_stat1` row even when the tunable is off.

- **DECISION**: Keep our own second `OP_Rewind` to skip the stat4 sample loop
  for an empty index. Upstream's replacement doesn't work for us.

  An empty index now jumps to the code that writes the `sqlite_stat1` row (see
  the previous entry), so something else has to skip the stat4 sample loop.
  Upstream does this by checking the row count in the stat1 row: `OP_Cast
  regStat1` followed by `OP_IfNot regStat1`, which jumps when the count is 0.
  Our `statGet()` writes `MAX(nRow, 1)`, so the count is never 0 and the jump
  never happens. Instead we emit a second `OP_Rewind` (`addrRewind`), created
  and patched inside the same `if( analyze_empty_tables )` blocks. It is a
  separate address from upstream's `addrGotoEnd`.

  Upstream's `OP_IfNot` is still emitted and patched even though it never
  jumps for us. Patching it keeps its `p2` pointing at a sensible address
  instead of address 0.

- **DECISION**: Write the stat4 sample loop as one `#if/#else`, with a
  separate upstream version and comdb2 version, instead of four `#if` blocks
  mixed into upstream's code.

  Upstream reads each sampled row back from the table: it gets the rowid,
  seeks to it, loads every index column, and builds a record. We don't have a
  rowid. Our sample is the packed index row that the accumulator already
  holds, so we fetch it with `STAT_GET_ROW` and never use the table cursor.
  The loop also ends differently. Upstream stops when the rowid is NULL. We
  stop when `regEq` is NULL, because `statGet()` returns NULL for
  `STAT_GET_NEQ` once there are no more samples.

  The two versions only share three `callStatGet()` calls. Repeating those
  three calls in our version keeps the upstream version as one unbroken
  twelve-line copy, so future upstream changes to it should merge without
  conflicts.

- **OBSERVATION**: `PRAGMA analysis_limit` now works fully in comdb2. The limit
  was already passed to `stat_init()`, and `statPush()` already returned a
  signal to skip ahead, but nothing used that signal. The 3.51 code that calls
  `statPush()` does. The default limit is 0, which emits a plain `OP_Next`, so
  the generated code only changes if the pragma is set.

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

- **DECISION**: Drop our `EP_Generic` patch in `sqlite3ExprAffinity()` and use
  upstream's version.

  SQLite added `EP_Generic` in 2014 (`fbb24d1092`) for an optimization that
  rewrote `x IN (y)` as `x==y`. The flag told `sqlite3ExprAffinity()` to
  ignore the affinity of `y`. comdb2 found this broke affinity and commented
  out the check (it was already commented out, marked "breaks affinity", in
  the 2017 initial import). In 2019 SQLite reached the same conclusion: it
  removed the optimization because it "causes difficult affinity problems"
  (`790b37a240`), and later removed the flag entirely. Upstream's code now
  does what our patch was for, so taking it loses nothing.

  The 3.26 upgrade (`c335397f1`) accidentally undid our patch. It turned the
  commented-out line into an `#if defined(SQLITE_BUILDING_FOR_COMDB2)` block,
  which turned the check back on for comdb2 builds only. From 2018 until this
  upgrade, comdb2 behaved like stock SQLite here.

  The `sqlite3ExprSkipCollate()` call in our version isn't lost either.
  Upstream moved the same logic into `sqlite3ExprAffinity()` itself
  (`a7d6db6ac0`), and it now also checks `EP_Skip|EP_IfNullRow`.

- **DECISION**: In `sqlite3ExprCodeGetColumnOfTable()`, keep passing `-1` to
  `sqlite3ColumnDefault()` instead of `regOut`, so that no `OP_RealAffinity`
  is emitted after reading a REAL or float column.

  To save space, stock SQLite may store a REAL value that is a whole number
  (e.g. `3.0`) as an integer. After reading a REAL column, it emits
  `OP_RealAffinity` to turn such a value back into a real. comdb2 never stores
  reals as integers: `db/sqlglue.c` converts each on-disk `SERVER_BREAL` field
  straight into a `MEM_Real`. So the opcode never does anything for us, and we
  stopped emitting it in 2013 (DRQS 40468319).

  The patch has two halves, and both must stay. `expr.c` passes `-1`, and
  `sqlite3ColumnDefault()` in `update.c` only emits `OP_RealAffinity` when
  `iReg>=0`. Without the `update.c` check, SQLite would emit the opcode on
  register -1. A 2013 merge dropped that check and debug asserts caught it in
  2014 (DRQS 48268498). The other caller, `pragma.c`, passes a real register
  and still gets the opcode, which is harmless.

  We take upstream's placement of the call (inside the `else`, without the
  `if( iCol>=0 )`). This loses nothing. The case it no longer covers is an
  `INTEGER PRIMARY KEY` column, which is read with `OP_Rowid`, and `OP_Rowid`
  ignores the default value the call would attach.

- **DECISION**: In the `EP_FixedCol` branch of `sqlite3ExprCodeTarget()`, use
  upstream's version and extend `zAff[]` to include every comdb2 affinity.

  This branch keeps constant propagation correct when column types differ:

  ```sql
  CREATE TABLE t1(a INT, b TEXT);
  INSERT INTO t1 VALUES(123,'0123');
  SELECT * FROM t1 WHERE a=123 AND b=a;     -- 1 row
  SELECT * FROM t1 WHERE a=123 AND b=123;   -- 0 rows
  ```

  `b=a` compares the values as numbers, but `b=123` compares them as text, so
  the optimizer can't just replace `a` with 123. Instead it keeps the
  reference to column `a`, marks it `EP_FixedCol`, and codes the constant
  using `a`'s affinity. `zAff[]` is the table that maps each affinity to the
  `P4` string that applies it.

  Our only change to this code has always been adding a row to `zAff[]` for
  each comdb2 data type. So we took upstream's version and checked that all of
  our rows are there, including `SQLITE_AFF_FLEXNUM`, which arrived with the
  new `sqliteInt.h` after the table was last extended.

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

- **DECISION**: In comdb2 builds, add an extra `bias` argument to
  `sqlite3BtreeIndexMoveto()` that holds the VDBE opcode doing the seek.

  In 3.28, one function, `sqlite3BtreeMovetoUnpacked()`, handled seeks on both
  tables and indexes. comdb2 reused its int argument `biasRight` to pass down
  the opcode that issued the seek. Upstream later split the function into a
  table version and an index version. The table version kept `biasRight`, but
  the index version has no such argument.

  comdb2's index seek code needs the opcode. It uses it to choose the search
  direction, decide how duplicate keys are handled, record key ranges for
  SELECTV, and, for `OP_IdxDelete`, send the index key to the master instead
  of seeking. So we added the argument back to `sqlite3BtreeIndexMoveto()`.

  This doesn't pass down anything new. It restores an argument we already
  used for this purpose, which upstream removed.

### `sqlite_tunables.{h,c}`

- We have defined some of our own tunables for SQLite.

### `sqlite3.h` / `sqlite.h.in`

- Upstream doesn't keep `sqlite3.h` in its source tree. It generates it from
  `src/sqlite.h.in` using `tool/mksqlite3h.tcl`, and we now do the same. We
  patch `src/sqlite.h.in`. The generated `sqlite3.h` is written to
  `${PROJECT_BINARY_DIR}/sqlite` and is **not** checked in. We used to check
  in the generated header and patch it directly, which meant keeping the same
  patches in two files, and the two had drifted apart by thirteen changes.

- We don't patch `tool/mksqlite3h.tcl`; it's a copy of upstream's. Some other
  files are checked in only so that the script can run unmodified:

  - `manifest`, `manifest.uuid`, `manifest.tags` and `tool/mksourceid.c`,
    which it uses for the source ID, date and branch/tags.
  - `ext/rtree/sqlite3rtree.h`, `ext/session/sqlite3session.h` and
    `ext/fts5/fts5.h`, which it copies into the header even though we don't
    build those extensions.

  Because our sources are patched, `mksourceid` adds an `alt1` suffix to the
  source ID. That's fine, because nothing depends on the value.

- Code outside `sqlite/` that includes `sqlite3.h` needs two things: the build
  directory on its include path, and a build dependency on the generator so
  the header exists before it compiles. CMake doesn't track generated files
  across directories, so neither happens automatically. Linking the
  `sqlite3_header` INTERFACE library provides both.

  `sqlite3_header` doesn't link `sqlite`, because that would create a cycle:
  `sqlite` depends on `bdb`, and `bdb` includes `<sqlite3.h>`. Anything that
  already depends on `sqlite` gets the build ordering automatically. Targets
  that don't, such as `bdb`, `schemachange` and the five plugins, link
  `sqlite3_header` directly. (Every plugin includes `bbinc/comdb2_plugin.h`,
  so for plugins the link is in `cmake/plugin.cmake`.)

- `tests/tools` and `tools/pmux` use the *system* SQLite, not ours.

#### Upgrade procedure for `sqlite3.h`

1. Save our changes as a diff against the old upstream file:

   ```sh
   diff -u sqlite/sqlite-<old>/src/sqlite.h.in sqlite/src/sqlite.h.in
   ```

2. Replace `sqlite/src/sqlite.h.in` with the new upstream copy and re-apply
   our changes. Copy the other files over unchanged:

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

3. Check that every re-applied change is inside a `SQLITE_BUILDING_FOR_COMDB2`
   guard. Drop any comdb2 backport of a feature that upstream now includes.

To verify, build the `generate_sqlite3_h` target and diff the generated header
against the previous release's. Then run `cc -fsyntax-only -Wall` on it, once
with and once without `-DSQLITE_BUILDING_FOR_COMDB2`.

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

- **TODO**: Change `OP_MakeRecord` to call `sqlite3VdbeSerialType()` and
  `sqlite3VdbeSerialPut()` instead of containing its own copy of their logic.

  Upstream inlined both functions into the opcode. If we take its version
  as-is, comdb2's serial-type logic would need to exist in two places. Calling
  the functions again would leave it in one place and remove the hazard
  described under "Hazards".

### `vdbeaux.c`

- **DECISION**: Move `sqlite3VdbeSerialType()` and `sqlite3VdbeSerialPut()`
  out of `vdbeaux.c` into `db/sqlglue.c`, and leave `vdbeaux.c` the same as
  upstream.

  Upstream inlined both functions into `OP_MakeRecord`. `SerialType` remains
  only inside an `#if 0` block kept for reference, and `SerialPut` was
  deleted. Nothing in the SQLite library calls either one anymore. comdb2
  still builds records outside the VDBE, in `db/sqlglue.c`, `db/fdb_bend.c`,
  `db/fdb_fend.c` and `db/sqlmaster.c`. Rather than keep patching code
  upstream no longer uses, we keep our own copies. `vdbeaux.c` uses upstream's
  text for both: the `#if 0` block is unchanged from 3.51, and we took
  upstream's side of the `SerialPut` conflict. The prototypes stay in
  `vdbeInt.h`, inside a comdb2 guard, so existing callers still see them.

  `sqlite3VdbeSerialTypeLen()`, `sqlite3VdbeSerialGet()` and
  `sqlite3SmallTypeSizes[]` stay in `vdbeaux.c` with our patches, because
  upstream still uses all three. A few comdb2 lines also stay, even though we
  otherwise took upstream's side of that conflict. The
  `#include <arpa/inet.h>` / `<flibc.h>` lines stay because `SerialGet()`
  needs their byte-order functions. The `END_INLINE_SERIALGET` marker stays
  because `tool/mkvdbeauxinlines.tcl` expects every start marker to have a
  matching end marker.

  The copies were moved as-is, including their `#if` guards, rather than
  rewritten as comdb2-only code, because there's no build to test against
  yet. See the TODO about simplifying them.

- **DECISION**: Store `MEM_IntReal` values as reals, not integers.

  This was decided while the function was still in `vdbeaux.c`, and the
  change moved with it to `db/sqlglue.c`.

  A `MEM_IntReal` value is a real held as an integer in `u.i`. Our version of
  the integer case in `sqlite3VdbeSerialType()` exists so that integers are
  always 8 bytes wide. That lets `sqlite3VdbeSerialPut()` write the whole
  `i64` at once instead of using SQLite's variable-width encoding. But that
  only decides how wide the value is, not whether it is stored as an integer
  or a real. Upstream decides between integer and real based on width: if the
  value takes 8 bytes either way, it keeps it as a real. For comdb2, every
  integer takes 8 bytes, so `MEM_IntReal` always gets serial type 7 (a real).
  The value has to be copied from `pMem->u.i` to `pMem->u.r` first, because
  `sqlite3VdbeSerialPut()` reads `u.r` for type 7.

- **DECISION**: Restore upstream's `__HP_cc` sign-extension workaround in
  `sqlite3VdbeSerialGet()`.

  `45366deb2` removed it as part of a general cleanup of AIX/HP-UX code, not
  to fix anything in comdb2. We only run on Linux and Solaris, so the code
  never runs either way. We put it back to stay as close to upstream as
  possible.

- **DECISION**: In `sqlite3MemCompare()`, use the `MEM_Small` comparison only
  when both values are numbers, and read `u.i` for `MEM_IntReal` as well as
  `MEM_Int`.

  `MEM_Small` means "compare at 4-byte float precision". It is what makes
  `floatcol = 0.1` match: the stored float converts to a slightly different
  double than the literal `0.1`, so a full-precision comparison would fail.

  `vdbe.c` sets the flag on *both* sides of a comparison with a SMALL-affinity
  column, before affinity has converted the values to their final types. So
  this code can be given a value that isn't a number at all. It used to read
  `u.r` anyway, and for a text or blob value that field holds leftover data,
  so `floatcol < 'abc'` returned an arbitrary answer. Now, if either side
  isn't a number, we skip this code and fall through to the code below, which
  already sorts numbers before text and blobs. Strings that look like numbers
  aren't affected, because `applyNumericAffinity()` has already converted them
  to `MEM_Real` or `MEM_Int`.

  A `MEM_IntReal` value is stored in `u.i`, so the code has to check for it
  alongside `MEM_Int`, or it reads the wrong field. No such value can reach
  this code until `vdbe.c` is merged, because all the code that sets that flag
  is in `vdbe.c`.

### `vdbeInt.h`

- Same bit used for `MEM_Comdb2` and `MEM_Term`. Safe because they are mutually
  exclusive. Former used to pass through data converted from client format into
  our row format, skipping SQLite format. Latter says whether string is
  null-terminated, but only relevant when data is in SQLite format (so would
  never be used/inspected if data is in our row format).

### `vdbemem.c`

- **DECISION**: In `sqlite3VdbeMemCast()`, use upstream's code for casts to
  TEXT, and remove comdb2's version.

  After converting a value to text, upstream clears the flags for the value's
  old types, so that only `MEM_Str` is left. comdb2 replaced that line with an
  `if` statement that uses `&` where `&&` was meant.
  `(pMem->flags & MEM_Str)` is either 0 or 2, and `!pMem->db->mallocFailed` is
  either 0 or 1, so the condition is always 0 and the body never runs. The
  compiler removes it entirely. The body would have been wrong anyway, because
  `~(MEM_AffMask|MEM_Zero)` sets nearly every flag bit.

  So comdb2 has never cleared these flags. `applyAffinity()` already clears
  the integer and real flags, so the difference only shows in
  `CAST(<blob> AS TEXT)`: the result kept `MEM_Blob` and `MEM_Zero` alongside
  `MEM_Str`. With upstream's code the result is a plain text value, the same
  as in stock SQLite.

- **DECISION**: Remove the `sqlite_record()` SQL function, as upstream did.

  Upstream added `sqlite_record()` only to read the old `sqlite_stat3` table,
  and deleted it when `sqlite_stat3` support was removed. comdb2 still
  registered it, so it appears in `comdb2_completion()`, but nothing in comdb2
  calls it. A search of BBGitHub found no application code that uses it
  either.

  `tests/auth.test/t09.expected` lists `sqlite_record()` among the completion
  candidates, and that line needs to be removed once the tree builds.
