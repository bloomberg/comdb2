# TODO

## Questions / Decisions

### Friday, Mar 20th, 2026

- What is dry-run mode?
  - See `ddl_dryrun.test`. `dryrun $ddl` tells you the schema change
    plan without making any changes.

### Wednesday, Feb 11th, 2026

- [ ] Might need to add some declarations/definitions from `btree.h` to
  `sqlite_btree.h` so things continue to compile.
- [x] Check if we might need to update the MD5 implementation in `md5.{h,c}`
  from `src/test_md5.c`.
  - Confirmed that implementation has not changed.
- [x] `WHERETRACE` macros moved from `whereInt.h` to `sqliteInt.h`, moved
  our patch to enable wheretrace in comdb2 builds.

### Tuesday, Feb 10th, 2026

- [ ] Need to review affinity definitions / mask in `sqliteInt.h`
  - I think `SQLITE_AFF_MASK` is correct...
- [ ] In `struct Index`, our `nAlloc` and SQLite's `mxSample` sound oddly similar,
  is one redundant with the other? See usage of `nAlloc` in `analyze.c`
- [ ] Safe to change value of `SF_ASTIncluded`?

### Monday, Feb 9th, 2026

- [ ] Decimal: Keep using ours?
  - My vote: yes. `decNumber` is more space-efficient, performance-oriented, and
    supports a wider variety of operations, implementing the full IEEE 754
    decimal arithmetic specification. It is also tied to the storage format now
    since we store these types natively (not converting to text like SQLite).
- [x] `src/hash.h`: We added a declaration for `sqlite3HashFindN`, which was for
  finding a key with a known number of bytes. The definition has since been
  removed from `src/hash.c` in commit `07ce98afc` as part of the SQLite 3.27.2
  upgrade, but seems like the declaration lingered. Removed the declaration and
  pulled in latest changes.

## Done

- [x] 4   ./src/default_consumer.h.in.patch
  - Comdb2-specific, leave as-is.
- [x] 10  ./src/comdb2Int.h.patch
  - Comdb2-specific, leave as-is.
- [x] 10  ./src/decimal.h.patch
- [x] 26  ./src/decimal.c.patch
  - Comdb2-specific, leave as-is. SQLite has a decimal extension now but
    choosing not to pull it in, we'll stick with our implementation.
- [x] 14  ./src/hash.h.patch
- [x]     ./src/hash.c
- [x] 14  ./src/os.h.patch
- [x]     ./src/os.c
  - Kept our patch, added comment on temp file naming convention.
- [x] 1003 ./src/sqliteInt.h.patch
  - But with questions...
- [x] 15  ./src/sqlite_tunables.h.patch
- [x] 82  ./src/sqlite_tunables.c.patch
  - Nothing to do, new files.
- [x]     ./src/btree.h
- [x]     ./src/btreeInt.h
- [x]     ./src/btree.c
  - Copy over from SQLite.
- [x] 402 ./src/sqlite_btree.h.patch
  - Comdb2-specific, leave as-is.
- [x] 27  ./src/md5.h.patch
- [x] 260 ./src/md5.c.patch
  - Comdb2-specific, leave as-is.
- [x] 58  ./src/comdb2vdbe.h.patch
- [x] 169 ./src/comdb2build.h.patch
- [x] 394  ./src/comdb2vdbe.c.patch
- [x] 469  ./src/comdb2lua.c.patch
- [x] 8063 ./src/comdb2build.c.patch
  - Leaving as-is for now, will revisit to see how we hook into SQLite.
- [x] 37  ./src/fwd_types.h.patch
  - Comdb2-specific, leave as-is.
- [x] 39  ./src/sqliteLimit.h.patch
- [x] 65  ./src/whereInt.h.patch
- [x] 12  ./src/shell.c.in.patch
- [x] 13  ./src/vtab.c.patch
- [x] 14  ./src/global.c.patch
- [x] 14  ./src/random.c.patch
- [x] 14  ./src/test3.c.patch
- [x] 14  ./src/vdbeblob.c.patch
- [x] 16  ./src/pragma.c.patch
- [x] 18  ./src/threads.c.patch
- [x] 27  ./tool/lemon.c.patch
- [x] 32  ./src/alter.c.patch
- [x] 33  ./src/treeview.c.patch
- [x] 34  ./src/loadext.c.patch
- [x] 35  ./src/window.c.patch
- [x] 43  ./src/printf.c.patch
- [ ] 47  ./src/mem1.c.patch
- [ ] 57  ./src/whereexpr.c.patch
- [ ] 58  ./src/status.c.patch
- [ ] 59  ./src/callback.c.patch
- [ ] 61  ./src/malloc.c.patch
- [ ] 62  ./src/upsert.c.patch

## Headers

- [ ] 152 ./src/vdbe.h.patch
- [ ] 252 ./src/sqlite.h.in.patch
- [ ] 368 ./src/vdbeInt.h.patch

## Source Files

- [ ] 100  ./src/trigger.c.patch
- [ ] 106  ./src/test_vfs.c.patch
- [ ] 136  ./src/os_unix.c.patch
- [ ] 140  ./src/delete.c.patch
- [ ] 143  ./src/update.c.patch
- [ ] 164  ./src/rowset.c.patch
- [ ] 205  ./src/vdbesort.c.patch
- [ ] 221  ./src/util.c.patch
- [ ] 221  ./tool/mkkeywordhash.c.patch
- [ ] 258  ./src/resolve.c.patch
- [ ] 334  ./src/attach.c.patch
- [ ] 359  ./src/prepare.c.patch
- [ ] 417  ./src/dttz.c.patch
- [ ] 427  ./src/tokenize.c.patch
- [ ] 436  ./src/insert.c.patch
- [ ] 451  ./src/main.c.patch
- [ ] 480  ./src/wherecode.c.patch
- [ ] 593  ./src/select.c.patch
- [ ] 612  ./src/vdbeapi.c.patch
- [ ] 718  ./src/where.c.patch
- [ ] 1152 ./src/analyze.c.patch
- [ ] 1362 ./src/func.c.patch
- [ ] 1378 ./src/expr.c.patch
- [ ] 1551 ./src/build.c.patch
- [ ] 1719 ./src/vdbeaux.c.patch
- [ ] 1805 ./src/vdbemem.c.patch
- [ ] 2332 ./src/vdbe.c.patch

## Miscellaneous

- [ ] 1671 ./parse.y.patch
