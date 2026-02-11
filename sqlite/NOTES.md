# NOTES

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
