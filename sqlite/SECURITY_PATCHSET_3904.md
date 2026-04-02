# Issue #3904 SQLite Security Backport Matrix

Scope: targeted `src/*` backports from upstream SQLite commits referenced in
https://github.com/bloomberg/comdb2/issues/3904.

## Status Summary

| Order | Upstream Commit | URL | Local Targets | Status | Verification Notes |
|---|---|---|---|---|---|
| 1 | `1e9c47be1e81e94a67f788c98fd70e8bf70e3746` | https://github.com/sqlite/sqlite/commit/1e9c47be1e81e94a67f788c98fd70e8bf70e3746 | `sqlite/src/build.c`, `sqlite/src/parse.y`, `sqlite/src/prepare.c` | `already_present` | Behavior is already in tree: `sqlite/src/build.c:2500`, `sqlite/src/parse.y:276`, `sqlite/src/prepare.c:52`. |
| 2 | `522ebfa7cee96fb325a22ea3a2464a63485886a8` | https://github.com/sqlite/sqlite/commit/522ebfa7cee96fb325a22ea3a2464a63485886a8 | `sqlite/src/resolve.c` | `not_applicable` | This tree does not define generated-column symbols (`TF_HasGenerated`, `COLFLAG_GENERATED`); generated-column SQL is rejected (`tests/sqlite_bugs.test/t01.expected`). Guarded references are at `sqlite/src/resolve.c:593` and `sqlite/src/resolve.c:644`. |
| 3 | `e59c562b3f6894f84c715772c4b116d7b5c01348` | https://github.com/sqlite/sqlite/commit/e59c562b3f6894f84c715772c4b116d7b5c01348 | `sqlite/src/select.c` | `applied` | DISTINCT-to-GROUP rewrite now excludes window queries via `p->pWin==0` at `sqlite/src/select.c:6219`. |
| 4 | `38096961c7cd109110ac21d3ed7dad7e0cb0ae06` | https://github.com/sqlite/sqlite/commit/38096961c7cd109110ac21d3ed7dad7e0cb0ae06 | `sqlite/src/alter.c`, `sqlite/src/build.c`, `sqlite/src/sqliteInt.h` | `applied_manual` | Added `SF_View` (`sqlite/src/sqliteInt.h:3052`), mark views in create path (`sqlite/src/build.c:2815`), and prune/unset in rename walkers (`sqlite/src/alter.c:817`, `sqlite/src/alter.c:1397`, `sqlite/src/alter.c:1482`). |
| 5 | `ebd70eedd5d6e6a890a670b5ee874a5eae86b4dd` | https://github.com/sqlite/sqlite/commit/ebd70eedd5d6e6a890a670b5ee874a5eae86b4dd | `sqlite/src/pragma.c` | `applied` | Guard added so `OPFLAG_TYPEOFARG` is only set if last opcode is `OP_Column`: `sqlite/src/pragma.c:1580`. |
| 6 | `926f796e8feec15f3836aa0a060ed906f8ae04d3` | https://github.com/sqlite/sqlite/commit/926f796e8feec15f3836aa0a060ed906f8ae04d3 | `sqlite/src/resolve.c` | `not_applicable` | Same rationale as commit 2: generated-column support is absent in this baseline, so upstream generated-column behavior is not reachable. Fallback `colUsed` logic remains active at `sqlite/src/resolve.c:659`. |
| 7 | `396afe6f6aa90a31303c183e11b2b2d4b7956b35` | https://github.com/sqlite/sqlite/commit/396afe6f6aa90a31303c183e11b2b2d4b7956b35 | `sqlite/src/select.c` | `applied_manual` | LEFT JOIN flattening guard now blocks DISTINCT parents: `sqlite/src/select.c:3855`. |
| 8 | `8428b3b437569338a9d1e10c4cd8154acbe33089` | https://github.com/sqlite/sqlite/commit/8428b3b437569338a9d1e10c4cd8154acbe33089 | `sqlite/src/select.c` | `applied` | Early abort on prior parse errors in `multiSelect()` at `sqlite/src/select.c:2878`. |
| 9 | `a6c1a71cde082e09750465d5675699062922e387` | https://github.com/sqlite/sqlite/commit/a6c1a71cde082e09750465d5675699062922e387 | `sqlite/src/select.c` | `applied` | `selectExpander()` now checks `pParse->nErr` before join processing at `sqlite/src/select.c:5055`. |
| 10 | `0934d640456bb168a8888ae388643c5160afe501` | https://github.com/sqlite/sqlite/commit/0934d640456bb168a8888ae388643c5160afe501 | `sqlite/src/expr.c` | `applied` | Defensive aggregate bounds checks in expression code at `sqlite/src/expr.c:3882`. |

## Notes

- Patches were applied to `sqlite/src/*` only. Upstream `manifest*` metadata
  changes were intentionally excluded.
- Two commits required semantic/manual adaptation due local divergence:
  `380969...` and `396afe...`.
- Generated-column specific fixes (`522eb...`, `926f...`) are not applicable on
  this baseline because generated-column support is not compiled in.
