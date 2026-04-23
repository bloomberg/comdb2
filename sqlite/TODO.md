# TODO

## Questions / Decisions

### Thursday, April 6th, 2026

- [ ] `src/analyze.c`: `analyzeOneTable()` calls `statInit()` via generated
  VDBE. The order of arguments has changed, go back to `statInit()` and see
  if we need to modify it or the caller.

### Wednesday, April 2nd, 2026

- [x] `src/where.c`: had to relocate patch to track dropped tables, the "omit
  no-op join" optimization was factored into its own function.
- [ ] `src/analyze.c`: `nActualRow` seems redundant with `nEst` in `StatAccum`
  now?

### Wednesday, April 1st, 2026

- [ ] `src/where.c`: (not important) do we still need `gbl_sqlite_stat4_scan`?
  Seems like we have changes that are now mainlined, is this flag unnecessary...
- [x] `src/insert.c`: patches to ensure data is converted from SQLite
  format to comdb2 format at insert time were moved from
  `sqlite3CompleteInsertion()` to `sqlite3GenerateConstraintChecks()`. 
- We have both `gbl_sqlite_sortermult` and `gbl_sqlite_sorterpenalty`,
  what's the difference? 
- [ ] Change default value of `gbl_sqlite_sorterpenalty` to 3 to match
  new default in SQLite?

### Wednesday, Mar 25th, 2026

- [x] `src/vdbe.c`: we needed to expand prepare mask to accomodate our
  custom flags. Some flags needed to be shifted over in `sqlite.h.in`
  to avoid conflict with `SQLITE_PREPARE_DONT_LOG`. So the prepare mask
  also needed to be extended.
- [x] `src/util.c`: dropping `sqlite3GetVarint32` patch because that branch
  can't possibly be hit anymore.
- [x] `tool/mkkeywordhash.c`: what should we set as priorities for custom
  keywords???

### Tuesday, Mar 24th, 2026

- [x] `src/test_vfs.c`: looks like we wanted to disable an assertion in
  `testvfs_obj_del` (`ckfree((char *)p->apScript)`). This assertion
  was removed entirely in newer SQLite, so removing the assertion
  and our patch-out in our copy as well.
- [ ] `src/os_unix.c`: some logmsg are NOT right (logging incorrect
  SQLite error).
- [x] `delete.c`: we had cherry picked an optimization, preserved it
  (commit `52a70498d`).
- [ ] `update.c`: line 908, we have a change to make `UPDATE` generate
  verify error if row cannot be found, how should this work for the new
  case of `UPDATE FROM` in the branch above?

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
- [x] Safe to change value of `SF_ASTIncluded`?
  - Yes, only used at prepare time for planning parallel execution.

### Monday, Feb 9th, 2026

- [x] Decimal: Keep using ours?
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

- [x] 4   ./src/default_consumer.h.in.patch (keep)
- [x] 10  ./src/comdb2Int.h.patch (keep)
- [x] 10  ./src/decimal.h.patch (keep)
- [x] 26  ./src/decimal.c.patch (keep)
- [x] 14  ./src/hash.h.patch (pr)
- [x]     ./src/hash.c (pr)
- [x] 14  ./src/os.h.patch
- [x]     ./src/os.c
- [x] 1003 ./src/sqliteInt.h.patch
- [x] 15  ./src/sqlite_tunables.h.patch
- [x] 82  ./src/sqlite_tunables.c.patch
- [x]     ./src/btree.h
- [x]     ./src/btreeInt.h
- [x]     ./src/btree.c
- [x] 402 ./src/sqlite_btree.h.patch
- [x] 27  ./src/md5.h.patch
- [x] 260 ./src/md5.c.patch
- [x] 58  ./src/comdb2vdbe.h.patch
- [x] 169 ./src/comdb2build.h.patch
- [x] 394  ./src/comdb2vdbe.c.patch
- [x] 469  ./src/comdb2lua.c.patch
- [x] 8063 ./src/comdb2build.c.patch
- [x] 37  ./src/fwd_types.h.patch
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
- [x] 47  ./src/mem1.c.patch
- [x] 57  ./src/whereexpr.c.patch
- [x] 58  ./src/status.c.patch
- [x] 59  ./src/callback.c.patch
- [x] 61  ./src/malloc.c.patch
- [x] 62  ./src/upsert.c.patch
- [x] 100  ./src/trigger.c.patch
- [x] 106  ./src/test_vfs.c.patch
- [x] 136  ./src/os_unix.c.patch
- [x] 140  ./src/delete.c.patch
- [x] 143  ./src/update.c.patch
- [x] 152  ./src/vdbe.h.patch
- [x] 164  ./src/rowset.c.patch
- [x] 205  ./src/vdbesort.c.patch
- [x] 221  ./src/util.c.patch
- [x] 221  ./tool/mkkeywordhash.c.patch
- [x] 252  ./src/sqlite.h.in.patch
- [x] 258  ./src/resolve.c.patch
- [x] 368  ./src/vdbeInt.h.patch
- [x] 334  ./src/attach.c.patch
- [x] 359  ./src/prepare.c.patch
- [x] 417  ./src/dttz.c.patch
- [x] 427  ./src/tokenize.c.patch
- [x] 436  ./src/insert.c.patch
- [x] 451  ./src/main.c.patch
- [x] 480  ./src/wherecode.c.patch
- [x] 593  ./src/select.c.patch
- [x] 612  ./src/vdbeapi.c.patch
- [x] 718  ./src/where.c.patch
- [ ] 1152 ./src/analyze.c.patch
- [ ] 1362 ./src/func.c.patch
- [ ] 1378 ./src/expr.c.patch
- [ ] 1551 ./src/build.c.patch
- [ ] 1671 ./parse.y.patch
- [ ] 1719 ./src/vdbeaux.c.patch
- [ ] 1805 ./src/vdbemem.c.patch
- [ ] 2332 ./src/vdbe.c.patch
