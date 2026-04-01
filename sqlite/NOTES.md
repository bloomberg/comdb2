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
  I am working on upgrading SQLite in this project from version 3.28 to 3.51.
  `sqlite/` contains our copy of SQLite with patches for comdb2. Inside there, I
  have also cloned `sqlite-3.28.0` and `sqlite-3.51.2`, the vanilla upstream
  releases, for your reference.

  In `<file>`, around line <#>, we have a change to ... How should this change
  be ported given that the surrounding code has been modified in SQLite 3.51.2?
  ```

## Cherry-picked patches

These are patches we cherry-picked that seem to have shifted around in latest
SQLite. During merge, preference was given to incoming changes, so need to be
wary whether these optimizations / bug-fixes still work.

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

## Friday, Mar 27th, 2026

- `vdbeInt.h`: same bit used for `MEM_Comdb2` and `MEM_Term`. Safe because
  they are mutually exclusive. Former used to pass through data converted from
  client format into our row format, skipping SQLite format. Latter says
  whether string is null-terminated, but only relevant when data is in SQLite
  format (so would never be used/inspected if data is in our row format).

## Wednesday, Mar 25th, 2026

- `rowset.c`: we have modified SQLite's `RowSet` to use BerkeleyDB temp tables
  to better handle large update/delete/etc. queries. See Rivers's commit
  `c026b966e`.

## Tuesday, Mar 24th, 2026

- `trigger.c`: our `sqlite_master` has an additional `csc2` column, needs to
  be set to NULL for triggers. Also, `MASTER_NAME` (`"sqlite_master"`) has
  been renamed to `LEGACY_SCHEMA_TABLE`.

## Monday, Mar 23rd, 2026

- `random.c`: we've made the random number generator thread-local instead of
  global to reduce contention amongst SQL threads.
- `alter.c`: disable SQLite's logic because we handle much of DDL ourselves.
- `loadext.c`: some extension APIs are disabled because they're not consistent
  between SQLite and comdb2 semantics.
- `mem1.c`: use `blobmem` (specialized blob memory allocator?) when allocating
  large amounts of memory. Maybe more optimized or has fewer problems with
  fragmentation, etc.
- `status.c`: we do not use SQLite's page caching, so disable assertions that
  check whether page cache mutex is held, and never try to acquire page cache
  mutex.
- `malloc.c`: implement thread-safe versions of `sqlite3DbMalloc` and
  `sqlite3DbRealloc`. These thread-safe variants only seem to be used for fdb,
  maybe because SQLite resources are shared for concurrent queries that
  reference the same foreign db?

## Friday, Mar 20th, 2026

- `shell.c.in`: we have a patch to initialize comdb2's SQLite tunables in our
  build of the SQLite shell.
- `global.c`: disable SQLite page cache because we use BerkeleyDB.

## Wednesday, Feb 11th, 2026

- `sqlite_tunables.{h,c}`: we have defined some of our own tunables for SQLite.
- `sqlite_btree.{h,c}`: we have completely replaced the B-Tree routines with our
  own to interact with BerkeleyDB instead. The functions are defined in
  `db/sqlglue.c`.
- `md5.{h,c}`: Adapted from SQLite's implementation in `src/test_md5.c`. Seems
  that SQLite does not expose this by default, maybe only when built with test
  flags.
- `fwd_types.h`: I guess we need to be able to refer to some SQLite structures in
  `db/` code.

## Tuesday, Feb 10th, 2026

- `sqliteInt.h`
  - We define additional affinity types for new datatypes like datetime, interval,
    etc.
  - We had a cool Lingzhi patch to make `SQLITE_KEEPNULL` work in spite of our
    additional affinity types. Latest SQLite doesn't have `SQLITE_KEEPNULL` at
    all, removed the patch. **This means we have one less
    `#ifdef SQLITE_BUILDING_FOR_COMDB2` block now** (127 vs. 128).
  - `struct Index` - `nAlloc` vs. `mxSample`. Seems like we have rewritten some
    parts of `analyze.c`, feels like it makes sense to keep both.
  - `SF_ASTIncluded`. We have added a `selFlag` related to parallel SQL execution
    that now conflicts with SQLite's own `SF_PushDown`. My only concern would be
    if changing the flag would break like fdb queries or something, but I don't
    think the AST itself is serialized, so maybe the actual flag value doesn't
    matter? In that case it should be safe to just pick a different flag value...

## Monday, Feb 9th, 2026

- Decimal
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

## Thursday, Feb 5th, 2026

- Certain files are generated at build time (see `tool` directory and comments
  in `CMakeLists.txt`).
- We have cherry-picked specific extensions from SQLite rather than support all
  of them, presumably to reduce maintenance surface area.
- Reorganized `CMakeLists.txt` to clarify which files are stock SQLite vs.
  Comdb2-related.
