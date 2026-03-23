# NOTES

## Setup

* To get the current and latest versions of stock SQLite:

  ```bash
  git clone https://github.com/sqlite/sqlite.git sqlite-3.28.0
  cp -R sqlite-3.28.0 sqlite-3.51.2
  (cd sqlite-3.28.0 && git checkout version-3.28.0)
  (cd sqlite-3.51.2 && git checkout version-3.51.2)
  ```

* To apply upstream changes to a file:

  ```bash
  diff3 -m $file sqlite-3.28.0/$file sqlite-3.51.2/$file > $file.merged
  ```

  Fix merge conflicts.

* Replace current file with merged:

  ```bash
  rm -f $file.patch $file.md && mv $file.merged $file
  ```

## Monday, Mar 23rd, 2026

- `random.c`: we've made the random number generator thread-local instead of
  global to reduce contention amongst SQL threads.
- `alter.c`: disable SQLite's logic because we handle much of DDL ourselves.
- `loadext.c`: some extension APIs are disabled because they're not consistent
  between SQLite and comdb2 semantics.

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
