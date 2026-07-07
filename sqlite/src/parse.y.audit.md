# Audit Log — `sqlite/src/parse.y` upgrade 3.28.0 → 3.51.2

Status: **Steps 1–4 COMPLETE. All questions resolved. Awaiting maintainer checkpoint, then Step 5 (merge plan below) → Step 6 (implement) → Step 7/8 (verify).**

---

## ▶ RESUME HERE — implementation checklist (Step 5 merge plan)

Inputs on disk: `sqlite/src/parse.y` (comdb2 current, 3.28-based), `sqlite/sqlite-3.28.0/src/parse.y`, `sqlite/sqlite-3.51.2/src/parse.y`, `sqlite/src/parse.y.merged` (25-conflict diff3 starting point — **do not trust; verify each hunk**). Do **not** read/reuse `parse.y.md` / `parse.y.patch` (prior attempt, off-limits).

Overarching rule: for every **forked** rule, put full 3.51.2 text in the `%ifndef SQLITE_BUILDING_FOR_COMDB2` parity branch and keep the comdb2 variant in `%ifdef`. For **unforked** core rules, take 3.51.2 verbatim. All new upstream *features* on forked rules stay parity-only.

Per-region actions (see matching E# for detail):
1. E1/E2 preamble: port `%include{}` header wrap, `%realloc parserStackRealloc`/`%free sqlite3_free`, `%syntax_error`→parserSyntaxError, `%stack_overflow`→sqlite3OomFault.
2. E3/E4/E5 C-include: keep comdb2 `comdb2Int.h`+`TRAN_ERROR`+`setExpert`; add upstream `ASSERT_IS_CREATE`, `parserSyntaxError`, rewritten `disableLookaside`, `updateDeleteLimitError` (dead), `parserStackRealloc`, `attachWithToSelect`, rewritten `parserDoubleLinkSelect`.
3. E6 explain: fork `ecmd` — comdb2 `explain cmdx.` (ifdef) / upstream `explain cmdx SEMI. {NEVER-REDUCE}` (ifndef); add `if(pParse->pReprepare==0)` to explain=1/2 and to comdb2 `EXPLAIN DISTRIBUTION` (=3).
4. E7 transactions: keep comdb2 TRAN_ERROR stubs; no upstream delta.
5. E8/E13 CREATE TABLE: keep all comdb2 rules (CSC2, partitioned/merge, LIKE); update parity `create_table_args`→`table_option_set`; comdb2 keeps its own minimal `table_options` (no STRICT).
6. E9 columnname: comdb2 `comdb2AddColumn(&A,&Y)` live; parity `sqlite3AddColumn(A,Y)`.
7. E10/E11/E30 tokens: keep comdb2's relocated synthesized-token block; apply upstream reorder + add `ERROR` into it; keep comdb2 `TO_*` block + `#if TK_SPAN>255` guard; add core `term ::= QNUMBER`; fallback list: add `NULLS FIRST LAST` (live), `WITHIN` (guard off), `GENERATED ALWAYS` under `%ifndef SQLITE_OMIT_GENERATED_COLUMNS` (dropped), **do not** add `MATERIALIZED` to comdb2 live; adopt final `%token SPACE COMMENT ILLEGAL.`. Re-check TK_SPAN≤255.
8. E12 scantok: port. E15 DEFAULT: keep comdb2 scanpt+comdb2AddDefaultValue live; parity→scantok forms.
9. E14 generated cols: parity-only (comdb2 ccons fork already excludes them). Also add `-DSQLITE_OMIT_GENERATED_COLUMNS` to `definitions.cmake` (separate file).
10. E16/E19 ccons/tcons: keep comdb2 comdb2Add*/comdb2CreateForeignKey rules; parity gets CHECK-span + ASSERT_IS_CREATE/u1.cr forms. **Q5/Option A:** comdb2 forked constraint rules write `pParse->u1.cr.constraintName`; comdb2 ALTER entry must set `isCreate=1` (companion, comdb2build.c).
11. E17/E18 autoinc/defer_subclause: preserve comdb2 ifndef-guarding (FK omitted).
12. E20 nulls: port (flows into comdb2, incl. DDL sortlist — flagged N1). E21 indexed_opt: keep comdb2 empty-only live; parity gets `indexed_by`.
13. E22 on_using/seltablist: port upstream verbatim (unforked); verify nothing references old `on_opt`/`using_opt`.
14. E23/E24/E25/E26 DELETE/UPDATE/INSERT/upsert: keep comdb2 forms (maybe_with; UPDATE no-orconf/no-FROM; INSERT non-CTE split); RETURNING/FROM parity-only; port shared `upsert` recursion + `sqlite3UpsertNew(…,N)`.
15. E27 rename: keep comdb2 `#if !defined(SQLITE_BUILDING_FOR_COMDB2)` guards around new upstream rename logic; verify each guard placement in auto-merge.
16. E28 CTE: preserve comdb2 `maybe_with`; adopt `wqitem/wqas` rewrite parity-only (no MATERIALIZED live); dedupe the two wqlist regions.
17. E29 comdb2 extensions block: preserve verbatim; ALTER parity gets `%ifndef SQLITE_OMIT_VIRTUALTABLE` wrap + `sqlite3AlterDropColumn`.

**E-VERIFY before finishing** (3.51.2 signatures of shared callees in *live* comdb2 rules): `sqlite3SrcListAppend`, `sqlite3SelectNew`(9-arg), `sqlite3ExprAddCollateToken`, `sqlite3ExprListAppend`, `tokenExpr`, `sqlite3ExprListSetSortOrder`(3-arg), `sqlite3Analyze`, `sqlite3AlterRenameTable`.

**Companion out-of-file changes** (flag in final report; may be handled by sibling upgrades): `definitions.cmake` +`-DSQLITE_OMIT_GENERATED_COLUMNS`; comdb2 ALTER sets `isCreate=1`; `comdb2build.c:6449‑6475` & `build.c:1579/1937` → `u1.cr.constraintName`.

**Step 7:** re-check each E# implemented as decided. **Step 8:** diff final file vs 3.51.2 upstream; every remaining difference must map to an E# comdb2 mod.

---

(Original header) Status: Steps 1–4 complete; audit below.

## Ground facts (reachability)

From `sqlite/definitions.cmake` and `sqlite/CMakeLists.txt` (`COMMAND lemon ${SQLITE_FLAGS} parse.y` — all `-D` flags reach lemon, so lemon evaluates `%ifdef/%if` with them):

- `SQLITE_BUILDING_FOR_COMDB2` — **always defined**. Therefore:
  - `%ifdef SQLITE_BUILDING_FOR_COMDB2` rules = **live**.
  - `%ifndef SQLITE_BUILDING_FOR_COMDB2` rules = **dead upstream-parity scaffolding** (kept so the file tracks upstream; never compiled into comdb2).
- `SQLITE_ENABLE_UPDATE_DELETE_LIMIT` — defined ⇒ the `%if SQLITE_ENABLE_UPDATE_DELETE_LIMIT || SQLITE_UDL_CAPABLE_PARSER` branch is live; the `#ifndef SQLITE_ENABLE_UPDATE_DELETE_LIMIT { updateDeleteLimitError } #endif` blocks are dead (feature IS enabled).
- Defined OMITs relevant here: `SQLITE_OMIT_FOREIGN_KEY`, `SQLITE_OMIT_VACUUM`, `SQLITE_OMIT_AUTOINCREMENT`.
- **Not** defined (features live): windowfunc, CTE, subquery, trigger, view, explain, altertable, virtualtable, analyze.
- `SQLITE_ENABLE_ORDERED_SET_AGGREGATES` — **not** defined ⇒ `WITHIN GROUP` ordered-set aggregate rules are compiled out.
- `SQLITE_OMIT_GENERATED_COLUMNS` — **to be defined** per maintainer instruction (2026-07-02). See E14.

## Merge philosophy (derived)

comdb2 forked only the DDL / statement-level rules it cares about (transactions, CREATE/ALTER/DROP TABLE/INDEX/VIEW/TRIGGER/PROCEDURE, GET/PUT/GRANT/REVOKE, etc.) plus the RENAME-token removal. It did **not** fork the core `expr` / `select` / `sortlist` / `values` grammar. Consequences:

1. Upstream changes to **unforked** core rules flow into comdb2 automatically (Class A) — e.g. NULLS FIRST/LAST, IS [NOT] DISTINCT FROM, `->`/`->>` (PTR), aggregate `ORDER BY`, multi-row VALUES, FILTER/window restructuring. These require their new callees / tokens from the *other* upgraded files (build.c, select.c, expr.c, tokenize.c, sqliteInt.h) — out of scope for this file, in scope for the upgrade as a whole.
2. Upstream changes to **forked** rules must be split: put full 3.51.2 text in the `%ifndef SQLITE_BUILDING_FOR_COMDB2` parity branch, preserve the comdb2 variant in the `%ifdef` branch.
3. New upstream **features that would require comdb2-engine work** are omitted initially (maintainer instruction; see E14) rather than wired into the live comdb2 grammar.

---

## Audit entries

Legend — Class: A pure-upstream / B pure-comdb2 / C overlapping / D undetermined. Conf: H/M/L.

### E1 — File preamble: `%include{...}` header wrapper, `%realloc`/`%free` directives
- Location: top of file (3.51 lines 1–40).
- Class **A**. Conf **H**.
- Upstream: header comment wrapped in `%include{}`; date reformat; new `%realloc parserStackRealloc` / `%free sqlite3_free` directives — the parser stack is now heap-growable (SQLite 3.38-era). comdb2 did not touch the preamble.
- Action: port upstream verbatim. `parserStackRealloc` static fn is added in the C `%include` block (see E5).

### E2 — `%syntax_error` / `%stack_overflow`
- Location: 3.51 lines 43–55.
- Class **A**. Conf **H**.
- Upstream: `%syntax_error` now calls `parserSyntaxError(pParse,&TOKEN)`; `%stack_overflow` now calls `sqlite3OomFault(pParse->db)` (heap stack ⇒ overflow is OOM). comdb2 untouched.
- Action: port upstream.

### E3 — C `%include` block: comdb2 headers + upstream `ASSERT_IS_CREATE`
- Location: after `#include "sqliteInt.h"` (merged ~156–181).
- Class **C** (both add to same block, non-overlapping content). Conf **H**.
- comdb2 adds `#include "comdb2Int.h"` + `TRAN_ERROR` macro (B) and `setExpert()` (B).
- Upstream adds `#define ASSERT_IS_CREATE assert(pParse->isCreate)` (A).
- Action: keep all comdb2 additions; add upstream `ASSERT_IS_CREATE`.

### E4 — `parserSyntaxError`, `disableLookaside` rewrite, `updateDeleteLimitError`
- Location: C `%include` (3.51 lines 116–163).
- Class **A**. Conf **M** (depends on upgraded sqliteInt.h).
- Upstream: new `parserSyntaxError`; `disableLookaside` now `memset(&pParse->u1.cr,0,...)`, `DisableLookaside`, `pParse->isCreate=1` under SQLITE_DEBUG; new `updateDeleteLimitError` (guarded `#if !defined(SQLITE_ENABLE_UPDATE_DELETE_LIMIT) && defined(SQLITE_UDL_CAPABLE_PARSER)` ⇒ **compiled out** for comdb2 since UDL enabled). comdb2 did not modify these bodies.
- Action: port upstream. Relies on `Parse.u1.cr`, `Parse.isCreate`, `DisableLookaside` from upgraded sqliteInt.h (other file). `updateDeleteLimitError` is dead for comdb2 but kept for parity.

### E5 — `parserStackRealloc` + `attachWithToSelect` + `parserDoubleLinkSelect` rewrite
- Location: C `%include` before the CTE `select` rules (3.51 ~lines 539–605).
- Class **A**. Conf **H**.
- Upstream: `parserDoubleLinkSelect` rewritten (ORDER BY/LIMIT-before-compound error, SF_Values); factored `attachWithToSelect`; new `parserStackRealloc`. comdb2 did not fork these.
- Action: port upstream verbatim.

### E6 — `explain` rules  — **RESOLVED**
- Location: merged 190–206.
- Class **C**. Conf **H** (post-investigation).
- comdb2 (B): added `explain ::= EXPLAIN DISTRIBUTION. {pParse->explain=3;}`.
- Upstream (A): `ecmd ::= explain cmdx.` → `ecmd ::= explain cmdx SEMI. {NEVER-REDUCE}`; added `if(pParse->pReprepare==0)` guard to `explain=1/2`.
- **Investigation (pReprepare):** `pParse->pReprepare` (the VM being reprepared) exists in 3.28.0, 3.51.2 and current comdb2; comdb2 uses the reprepare path (`sqlite3Reprepare`, vdbeapi.c:1233,2451). During reprepare, `prepare.c` restores the explain flag from the original statement via `sqlite3_stmt_isexplain(pReprepare)` (3.51.2 prepare.c:702; already present in comdb2 prepare.c:880). The guard exists so the grammar does not clobber that restored value. **It is relevant and safe for comdb2.**
- **Decision (maintainer, 2026-07-02):** (1) Do **NOT** adopt `ecmd ::= explain cmdx SEMI` — comdb2 apps rely on EXPLAIN not requiring a trailing semicolon. Keep `ecmd ::= explain cmdx.` as an intentional comdb2 fork (`%ifdef SQLITE_BUILDING_FOR_COMDB2`), with upstream's `ecmd ::= explain cmdx SEMI. {NEVER-REDUCE}` preserved in the `%ifndef` parity branch. (2) Adopt the `if(pParse->pReprepare==0)` guard on `explain=1/2`, and add the same guard to `EXPLAIN DISTRIBUTION` (`explain=3`).

### E7 — Transactions: BEGIN/COMMIT/END/ROLLBACK/SAVEPOINT/RELEASE
- Location: merged 210–258.
- Class **B**. Conf **H**.
- comdb2 replaces all transaction statements with `sqlite3ErrorMsg(pParse, TRAN_ERROR, ...)` (transactions are not issued through the SQL engine in comdb2). Upstream unchanged in this region (still `sqlite3BeginTransaction`/`sqlite3EndTransaction`/`sqlite3Savepoint`).
- Action: preserve comdb2 error-stubs (live); upstream forms remain in `%ifndef`/`#else` parity branches. No upstream delta to fold.

### E8 — CREATE TABLE: `create_table`, CSC2, `create_table_args`, `createkw`  ⟶ **Q2**
- Location: merged 260–358.
- Class **C**. Conf **M**.
- comdb2 (B): `dryrun` prefix; `comdb2CreateTableStart`; `comdb2_create_table_csc2` (`... NOSQL`); `create_table_args ::= LP columnlist conslist_opt RP comdb2opt table_options partitioned merge` → `comdb2CreateTableEnd`; `partitioned`/`merge`/`partition_options` subgrammar; `create_table_args ::= LIKE_KW nm dbnm` → `comdb2CreateTableLikeEnd`. comdb2 has **no** `AS select` create-table.
- Upstream (A): `create_table_args` uses `table_option_set(F)` (renamed from `table_options`, STRICT support, see E13); `createkw` body reformatted (cosmetic).
- Proposed: preserve all comdb2 rules unchanged. Update the `%ifndef` parity `create_table_args` to `table_option_set`. **Q2**: comdb2's rule still uses the `table_options(F)` nonterminal name — keep comdb2 on its own `table_options` (STRICT not supported) vs. renaming; confirm comdb2 does not want STRICT tables.

### E9 — `columnname` action signature
- Location: merged 397–444.
- Class **C**. Conf **H**.
- comdb2 (B): `comdb2AddColumn(pParse,&A,&Y)` (by pointer — comdb2 fn signature fixed).
- Upstream (A): `sqlite3AddColumn(pParse,A,Y)` (changed to by-value).
- Action: keep comdb2 `&A,&Y` (live); parity branch gets upstream by-value form.

### E10 — Relocated synthesized-token `%token` block + `#if TK_SPAN>255`  ⟶ **Q6**
- Location: merged 407–439 (comdb2 moved it here) vs. upstream end-of-file block (see E30).
- Class **C**. Conf **L**.
- comdb2 (B): moved the `TRUEFALSE…SPAN` synthesized-token block **and** the `#if TK_SPAN>255` guard to *before* the early `%token`/`%fallback` declarations — to keep TK_SPAN ≤ 255 given ~120 extra comdb2 keyword/`TO_*` tokens (token-numbering management).
- Upstream (A): kept the block at EOF, **reordered** it (COLUMN/AGG_* first; UPLUS before UMINUS), added `ERROR` token, and added `term ::= QNUMBER` + `QNUMBER` token (see E30).
- Proposed: apply the upstream reordering + `ERROR` token to the **relocated** comdb2 copy; resolve where `QNUMBER`/`ERROR`/`COMMENT` land and re-verify TK_SPAN ≤ 255 for the comdb2 token set. **Q6** — this is comdb2 token-space architecture; do not guess.

### E11 — `%fallback` keyword list additions  ⟶ **Q6**
- Location: merged 469–540.
- Class **C**. Conf **M**.
- comdb2 (B): added `SEQUENCE` to base list; a `%ifdef SQLITE_OMIT_WINDOWFUNC RANGE %endif`; and a large comdb2 keyword block (ADD, AGGREGATE, … ZLIB).
- Upstream (A): added `NULLS FIRST LAST`; `%ifdef SQLITE_ENABLE_ORDERED_SET_AGGREGATES WITHIN`; `%ifndef SQLITE_OMIT_GENERATED_COLUMNS GENERATED ALWAYS`; `MATERIALIZED`.
- Proposed: merge upstream keywords into the shared list — `NULLS FIRST LAST` (live; used by E20), `WITHIN` (guard stays off), `GENERATED ALWAYS` under `%ifndef SQLITE_OMIT_GENERATED_COLUMNS` (⇒ dropped, E14), `MATERIALIZED` (**Q4**). Preserve all comdb2 keywords. Watch fallback-token count vs. TK_SPAN (**Q6**).

### E12 — `scantok` nonterminal
- Location: merged 606–609.
- Class **A**. Conf **H**. Upstream added `scantok` (token-with-length, for DEFAULT offset capture). comdb2 untouched. Action: port.

### E13 — `table_options` → `table_option_set`/`table_option` (STRICT)  ⟶ **Q2**
- Location: merged 360–396.
- Class **C**. Conf **M**.
- Upstream (A): replaced `table_options` with `table_option_set`(u32)/`table_option` adding STRICT.
- comdb2 (B): keeps `%type table_options {int}`, only the empty production live (`WITHOUT ROWID` is in the `%ifndef` parity branch); comdb2 tables are never WITHOUT ROWID / STRICT.
- Proposed: keep comdb2's minimal `table_options` for the live path; put full upstream `table_option_set` in the parity branch. Tied to **Q2**.

### E14 — Generated columns (`ccons ::= GENERATED ALWAYS AS …`) — **RESOLVED: omit**
- Location: merged 728–743; fallback E11.
- Class **C** → resolved. Conf **H**.
- Upstream (A): added `ccons ::= GENERATED ALWAYS AS generated` / `AS generated` / `generated ::= …` calling `sqlite3AddGenerated` (a no-op macro under `SQLITE_OMIT_GENERATED_COLUMNS`, build.c:1104).
- comdb2 forked all of `ccons`; generated rules were never added.
- **Decision (maintainer, 2026-07-02):** define `SQLITE_OMIT_GENERATED_COLUMNS` initially (full support needs extensive comdb2 work).
- Action: (a) parse.y — place upstream generated rules in the `%ifndef SQLITE_BUILDING_FOR_COMDB2` parity branch (unreachable in comdb2 ⇒ `GENERATED …` remains a syntax error, preserving current behavior); gate `GENERATED ALWAYS` fallback tokens with `%ifndef SQLITE_OMIT_GENERATED_COLUMNS` per upstream. (b) `definitions.cmake` (separate change) — add `-DSQLITE_OMIT_GENERATED_COLUMNS`.

### E15 — `ccons`: DEFAULT rules (scanpt→scantok)
- Location: merged 616–684.
- Class **C**. Conf **M**.
- comdb2 (B): all DEFAULT variants call `comdb2AddDefaultValue` with `scanpt` offsets (comdb2 fn signature fixed); includes extra `#if defined(SQLITE_BUILDING_FOR_COMDB2)` splits inside the MINUS/id rules.
- Upstream (A): DEFAULT PLUS/MINUS reordered to `scantok(Z) term(X)`, offsets via `&Z.z[Z.n]`; `sqlite3AddDefaultValue(pParse,X,A.z,&A.z[A.n])`.
- Proposed: keep comdb2 `scanpt`+`comdb2AddDefaultValue` productions live; update the `%ifndef` parity forms to the upstream scantok shapes. comdb2 DEFAULT productions intentionally stay on the 3.28 grammar shape (matches `comdb2AddDefaultValue`). Parity forms are dead in the comdb2 lemon run ⇒ no grammar conflict.

### E16 — `ccons`: NULL / NOT NULL / PRIMARY KEY / UNIQUE / INDEX / REFERENCES / CHECK / OPTION DBPAD
- Location: merged 689–736.
- Class **B** (comdb2 fork) with **C** overlap on CHECK.
- comdb2 (B): `comdb2AddAutoIncrement/AddNull/AddNotNull/AddPrimaryKey/AddIndex/CreateForeignKey`; `INDEX` (dupkey) constraint; `OPTION DBPAD`. `AUTOINCR` handled as a ccons (not the `autoinc` nonterminal).
- Upstream (A): `ccons ::= CHECK LP(A) expr RP(B).` now passes span `A.z,B.z` to `sqlite3AddCheckConstraint`; REFERENCES uses `eidlist_opt`.
- Note: `SQLITE_OMIT_FOREIGN_KEY` defined ⇒ upstream `defer_subclause` group + FK checking omitted; comdb2 owns FK via `comdb2CreateForeignKey`/`comdb2DeferForeignKey`.
- Action: preserve comdb2 rules; update `%ifndef` parity forms (CHECK span args, REFERENCES) to 3.51.2.

### E17 — `autoinc`
- Location: merged 745–750. Class **B**. Conf **H**.
- comdb2: `autoinc ::= AUTOINCR` in the `%ifndef` branch only (comdb2 handles AUTOINCR as a ccons, E16). No upstream delta. Preserve.

### E18 — `defer_subclause` / `init_deferred_pred_opt`
- Location: merged 771–779. Class **B**. Conf **H**.
- comdb2 wraps the sqlite defer subgrammar in `%ifndef SQLITE_BUILDING_FOR_COMDB2` (FK omitted / comdb2 owns FK). No upstream delta. Preserve.

### E19 — `tcons` (table constraints)
- Location: merged 787–850.
- Class **B** fork + **C** overlap on CHECK / `ASSERT_IS_CREATE`.
- comdb2 (B): `nm_opt`, `with_opt`, `with_opt2`, `with_inc`; `comdb2AddPrimaryKey`; `comdb2AddIndex` UNIQUE/INDEX (+ INCLUDE datacopy variants, `where_opt` partial-index); `comdb2CreateForeignKey`+`comdb2DeferForeignKey`; `comdb2AddCheckConstraint(E,BW,AW)` span.
- Upstream (A): `tcons ::= CONSTRAINT nm. {ASSERT_IS_CREATE; pParse->u1.cr.constraintName=X;}`; `CHECK LP(A) expr RP(B) onconf` span.
- Action: preserve comdb2 tcons; update `%ifndef` parity (`ASSERT_IS_CREATE`, `u1.cr.constraintName`, CHECK span) to 3.51.2. NOTE the shared `tconscomma` line auto-merged to `ASSERT_IS_CREATE; pParse->u1.cr.constraintName.n=0` — verify comdb2's forked `tcons`/`constraint_opt`/`alter_comma` still use `pParse->constraintName` (3.28 field) while the shared `tconscomma` now uses `u1.cr.constraintName`. **Possible inconsistency** (comdb2 code sets `pParse->constraintName`, upstream shared line sets `pParse->u1.cr.constraintName`). Flag Class C, Conf M — **Q5**.

### E20 — ORDER BY `nulls` (NULLS FIRST/LAST)
- Location: merged 1325–1343.
- Class **A** (core `sortlist`, unforked). Conf **H**.
- Upstream added `nulls` nonterminal; `sqlite3ExprListSetSortOrder(A,Z,X)` 3-arg. Flows into comdb2. Requires upgraded `sqlite3ExprListSetSortOrder` (other file) + NULLS/FIRST/LAST tokens (E11). Action: port.

### E21 — `indexed_opt` / `indexed_by` / `using_opt`
- Location: merged 1287–1311.
- Class **C**. Conf **H**.
- comdb2 (B): removed `INDEXED BY` / `NOT INDEXED` from the live path (`indexed_opt` empty only; comdb2 does not support INDEXED-BY hints — they sit in the `%ifndef` branch).
- Upstream (A): refactored into `indexed_opt ::= indexed_by` + `indexed_by ::= INDEXED BY nm | NOT INDEXED`.
- Action: keep comdb2's live empty-only `indexed_opt`; put upstream `indexed_by` refactor in parity branch.

### E22 — `on_opt` → `on_using` (JOIN … USING) + `seltablist` rewrite
- Location: 3.51 lines 852–872; comdb2 file still on 3.28 `on_opt`/`using_opt`.
- Class **C** (shared/unforked rules). Conf **M**.
- Upstream (A): replaced `on_opt`(Expr*)+`using_opt` with unified `on_using`(OnOrUsing); rewrote all `seltablist` productions; `sqlite3SrcListAppendFromTerm` new signature (one fewer arg); `SrcItem`/`u4.pSubq`/`fg.isSubquery`/`fg.isNestedFrom` nested-from handling.
- comdb2 did not fork `seltablist`/`on_opt`/`using_opt`. Action: port upstream verbatim (live). Depends on select.c/sqliteInt.h upgrade. **Verify** no comdb2 rule references `on_opt`/`using_opt` after the change.

### E23 — DELETE statement  ⟶ **Q3**
- Location: merged 1375–1411.
- Class **C**. Conf **M**.
- comdb2 (B): `maybe_with` instead of `with` (E28); no RETURNING.
- Upstream (A): `%if SQLITE_ENABLE_UPDATE_DELETE_LIMIT || SQLITE_UDL_CAPABLE_PARSER`; `where_opt_ret` (RETURNING); `updateDeleteLimitError` (dead).
- Proposed: keep comdb2 `maybe_with`. **Q3**: adopt `where_opt_ret` (RETURNING) for comdb2 DELETE? Needs `sqlite3AddReturning` (new; absent from comdb2 tree) + comdb2 execution support. Else keep `where_opt`.

### E24 — `where_opt_ret` / RETURNING nonterminals  ⟶ **Q3**
- Location: merged 1413–1425. Class **A** (new). Conf **M**.
- Defines `where_opt_ret` with RETURNING → `sqlite3AddReturning`. Only reachable if wired into DELETE/UPDATE/INSERT (Q3). Keep definitions at least in parity branch.

### E25 — UPDATE statement  ⟶ **Q3**
- Location: merged 1429–1524.
- Class **C**. Conf **L** (highest-uncertainty region).
- comdb2 (B): drops `orconf(R)` (`cmd ::= maybe_with UPDATE xfullname …`, `sqlite3Update(…,0,…)` — comdb2 UPDATE has no OR-conflict clause); `maybe_with`; no FROM; no RETURNING.
- Upstream (A): added `from(F)` (UPDATE…FROM) with subquery nesting via `sqlite3SrcListAppendList`; `where_opt_ret` (RETURNING); `updateDeleteLimitError` (dead).
- Proposed default (safe): keep comdb2 UPDATE as-is; full 3.51.2 in parity branch. **Q3**: does comdb2 want UPDATE…FROM and/or RETURNING?

### E26 — INSERT statement + upsert RETURNING  ⟶ **Q3**
- Location: comdb2 INSERT block (~641–671) + upsert (3.51 1049–1084).
- Class **C**. Conf **M**.
- comdb2 (B): added non-`with` INSERT productions (unconditional) plus `with` productions under `%ifndef SQLITE_OMIT_CTE` — consequence of `with` becoming non-nullable (E28).
- Upstream (A): `returning` nonterminal; `upsert(N)` recursion; `upsert ::= RETURNING …`; `DEFAULT VALUES returning`; `sqlite3UpsertNew(…,N)` extra arg.
- Proposed: preserve comdb2 INSERT restructuring; port shared `upsert` changes (new `sqlite3UpsertNew` sig from upgraded upsert.c). **Q3**: RETURNING reachability via comdb2 INSERT.

### E27 — RENAME-token removal (`expr nm.nm`, `nm.nm.nm`, `fullname`, CREATE INDEX)
- Location: merged 1660–1694; fullname ~1000; CREATE INDEX ~1751.
- Class **C**. Conf **M**.
- comdb2 (B): consistently `#if !defined(SQLITE_BUILDING_FOR_COMDB2)` guards out every `sqlite3RenameTokenMap`/`IN_RENAME_OBJECT` block (comdb2 uses its own schema-change engine, not SQLite ALTER…RENAME token remapping).
- Upstream (A): reworked rename mapping — `tokenExpr` in `nm.nm`; `sqlite3RenameTokenRemap` in `nm.nm.nm`; `tokenExpr` in selcollist `*`.
- Proposed: keep the comdb2 guards around the **new upstream** rename logic (guarded block dead for comdb2 ⇒ behavior preserved; parity build gets new logic). Auto-merge already did this for `nm.nm.nm` — **verify each** guard placement. Confirm comdb2 use of `tokenExpr` (static fn, sig unchanged) in `nm.nm` is fine.

### E28 — `with` → `maybe_with` + CTE `wqlist`→`wqitem/wqas` (MATERIALIZED)  ⟶ **Q4**
- Location: comdb2 CTE-extensions area (~2175) and shared CTE block near EOF.
- Class **C**. Conf **M**.
- comdb2 (B): introduced `maybe_with` (`maybe_with ::= .` / `maybe_with ::= with`), removed nullable `with ::= .`, so DELETE/UPDATE/INSERT use `maybe_with`. There are **two** `wqlist`/`with` regions in the comdb2 file — must dedupe carefully.
- Upstream (A): rewrote `wqlist` into `wqitem`/`wqas`/`withnm` with `AS [NOT] MATERIALIZED` (`M10d_*`), `sqlite3CteNew`, `sqlite3WithAdd` (2-arg), `pParse->bHasWith`.
- Proposed: preserve comdb2 `maybe_with`; adopt upstream `wqitem/wqas` rewrite in the unforked `wqlist` rules. **Q4**: MATERIALIZED / NOT MATERIALIZED CTE hints for comdb2 (adds `MATERIALIZED` keyword, E11)? Needs `sqlite3CteNew`/`M10d_*`/`bHasWith`.

### E29 — COMDB2 SYNTAX EXTENSIONS (large block) + SELECTV + COLLATE DATACOPY + EXPLAIN DISTRIBUTION + ANALYZE variants + ALTER TABLE actions + GET/PUT/GRANT/REVOKE/TRUNCATE/REBUILD/PARTITION/PROCEDURE/LUA
- Location: SELECTV ~856; COLLATE DATACOPY ~1380; ALTER 2438–2576; extensions ~2189–2805.
- Class **B**. Conf **H**.
- Entirely comdb2-owned; no upstream counterpart. Preserve verbatim.
- Upstream ALTER-region deltas that live only in the `%ifndef` parity branch: ALTER TABLE wrapped in `%ifndef SQLITE_OMIT_VIRTUALTABLE`; added `ALTER TABLE … DROP COLUMN` + `sqlite3AlterDropColumn`. comdb2 has its own `alter_table_drop_column`.
- See E-VERIFY for shared callee signature checks.

### E30 — End-of-file token blocks: `ERROR`, `QNUMBER`/`term ::= QNUMBER`, `TO_*`, `SPACE COMMENT ILLEGAL`  ⟶ **Q6**
- Location: merged 3383–3473.
- Class **C**. Conf **L**.
- comdb2 (B): at EOF keeps only `%token TO_TEXT … TO_DECIMAL` (comdb2 cast operators); main synthesized block moved up (E10).
- Upstream (A): EOF synthesized block reordered + `ERROR`; new `term ::= QNUMBER` + `QNUMBER` token; final `%token SPACE COMMENT ILLEGAL.` (adds `COMMENT`).
- Proposed: `%token SPACE COMMENT ILLEGAL.` auto-merged to upstream (adopt — tokenizer now emits TK_COMMENT). `term ::= QNUMBER` is a core rule ⇒ port live (needs `QNUMBER` token + `sqlite3DequoteNumber`). Placement of `QNUMBER`/`ERROR` relative to comdb2's relocated block + `TO_*` and the TK_SPAN≤255 invariant ⇒ **Q6**.

---

## E-VERIFY — 3.51.2 signature checks for comdb2-LIVE shared callees
Confirm 3.51.2 signatures match comdb2 call sites (else preserved comdb2 branches break):
- `sqlite3SrcListAppend(pParse,0,&X,0)` — fullname / CREATE INDEX.
- `sqlite3SelectNew(...9 args...)` — SELECTV.
- `sqlite3ExprAddCollateToken(pParse,A,&t,1)` — COLLATE DATACOPY.
- `sqlite3ExprListAppend`, `tokenExpr` — pdl.
- `sqlite3ExprListSetSortOrder` (now 3-arg) — comdb2 `#if` helper block (~merged 1751).
- `sqlite3Analyze`, `sqlite3AlterRenameTable` — comdb2 ANALYZE / RENAME.

---

## Decisions (maintainer, 2026-07-02)
- **Q1 (EXPLAIN)** — RESOLVED, see E6: keep no-SEMI comdb2 fork; adopt pReprepare guard incl. DISTRIBUTION.
- **Q2 (STRICT / table_option_set)** — RESOLVED: **omit / parity-only.** comdb2 keeps its minimal `table_options`; upstream `table_option_set`+STRICT stays in the `%ifndef` parity branch.
- **Q3 (RETURNING, UPDATE…FROM)** — RESOLVED: **omit all / parity-only.** comdb2 DELETE keeps `where_opt` (not `where_opt_ret`); comdb2 UPDATE keeps no-FROM/no-orconf form; INSERT/upsert RETURNING not wired live. Full 3.51.2 forms live in `%ifndef` parity branches. `sqlite3AddReturning` never reached by comdb2.
- **Q4 (MATERIALIZED CTE)** — RESOLVED: **omit / parity-only.** Adopt upstream `wqitem/wqas` rewrite only in parity branch; comdb2 CTE keeps 3.28-shape (no MATERIALIZED). Preserve comdb2 `maybe_with`. `MATERIALIZED` keyword not added to the comdb2 live fallback list.

## Still-open

### Q5 — `constraintName` moved into the `isCreate`-gated `u1.cr` union  — **RESOLVED (Option A, proven safe)**
**Decision (maintainer, 2026-07-02):** Option A — comdb2 uses `pParse->u1.cr.constraintName`, sets `pParse->isCreate` during ALTER, and `comdb2build.c` migrates to `u1.cr`.
**Safety proof:** `Parse.u1` has exactly two members — `cr` (isCreate-gated: addrCrTab/regRowid/regRoot/constraintName) and `d` (only `Returning *pReturning`). The sole alternative to `cr` is `d.pReturning`, which is used only by RETURNING (trigger.c:74,1085) — and RETURNING is disabled for comdb2 (Q3). Therefore in the comdb2 build `u1` is only ever `u1.cr`; setting `isCreate` in ALTER and writing `u1.cr.constraintName` cannot clobber a live field. (Other `u1.` matches in the tree are `sqlite3.u1.isInterrupted` and `SrcItem.u1` — different unions.)
**Companion changes (other files):** comdb2 ALTER entry (`comdb2AlterTableStart` / `alter_table` rule) must set `pParse->isCreate = 1`; `comdb2build.c:6449‑6475` + `build.c:1579/1937` → `u1.cr.constraintName`. parse.y forked constraint rules (`tcons CONSTRAINT`, `constraint_opt`, `alter_comma`) write `pParse->u1.cr.constraintName` and the shared `tconscomma` auto-merge (`ASSERT_IS_CREATE; u1.cr.constraintName.n=0`) is then correct.

### Q5 (historical framing)
Investigation:
- Upgraded `sqliteInt.h` (wip tree) has `constraintName` **only** inside `u1.cr` (line 4146), gated on `pParse->isCreate`; **no flat field / alias remains**.
- Upstream sets `isCreate` only on the CREATE path (`createkw`→`disableLookaside`, under SQLITE_DEBUG the `ASSERT_IS_CREATE` asserts it). constraintName is upstream-touched only during CREATE TABLE.
- **comdb2 touches `constraintName` during ALTER TABLE** too: `constraint_opt`, `alter_comma`, `tconsfk`/`tconscheck` (ADD CONSTRAINT) — and comdb2's C consumers (`comdb2build.c:6449‑6475`, plus `build.c:1579/1937`) still read the **flat** `pParse->constraintName`.
- comdb2 C code does **not** set `pParse->isCreate` anywhere (grep: only unrelated os-layer `isCreate`). So during comdb2 ALTER, the `u1.cr` slot is not "valid" per the new invariant.
Conflict: upstream's memory optimization (union `u1.cr`, valid only when `isCreate`) collides with comdb2 using `constraintName` in non-CREATE (ALTER) contexts. Writing `u1.cr.constraintName` in comdb2 ALTER rules would violate the invariant / trip `ASSERT_IS_CREATE` and risk aliasing other `u1` fields. This spans parse.y + sqliteInt.h + comdb2build.c.
Options: (A) comdb2 sets `isCreate` during ALTER + migrate `comdb2build.c` to `u1.cr` (needs proof no other `u1` field is used in comdb2 ALTER); (B) keep a flat `constraintName` field for the comdb2 build (diverge from the union optimization) — simplest/safest, but a sqliteInt.h change; (C) other. **parse.y's forked constraint rules should write whichever field this decision picks.** Default pending answer: preserve current comdb2 behavior (flat `pParse->constraintName`) ⇒ implies option (B) in sqliteInt.h.

### Q6 — Token-block relocation + TK_SPAN ≤ 255 (build-verifiable; my plan)
Plan (no maintainer input needed): preserve comdb2's relocation of the synthesized-token block; apply upstream's reorder + insert `ERROR` into that relocated block; keep the comdb2 `TO_*` block at EOF; add core `term ::= QNUMBER` (+ implicit `QNUMBER` token) near the window/end section as upstream; adopt final `%token SPACE COMMENT ILLEGAL.`. The `#if TK_SPAN>255` guard (also relocated) validates at build. comdb2-live net-new tokens vs 3.28: `NULLS FIRST LAST`, `PTR`, `ERROR`, `QNUMBER`, `COMMENT` (~6). If the guard trips, revisit block placement. Will confirm the count once the merged grammar is assembled.

## Core unforked syntax additions — per-feature review (for team, per maintainer request)

Direction (2026-07-02): let these flow into comdb2, but document each so the team can later decide whether to disable. For each: **surface** (where the syntax may appear), **comdb2 work / relation to existing features**, **nature** (pure sugar-internal vs. touches comdb2).

### N1 — `NULLS FIRST` / `NULLS LAST` (ORDER BY sort terms)
- Grammar: new `nulls` nonterminal appended to both `sortlist` productions; `sqlite3ExprListSetSortOrder(list, sortorder, nulls)` (now 3-arg).
- **Surface (broad):** `sortlist` is used by — SELECT/compound `ORDER BY`; window `ORDER BY` (over_clause); **`CREATE INDEX` column list**; **`tcons` PRIMARY KEY / UNIQUE / INDEX** column lists; `upsert ON CONFLICT (sortlist)`; comdb2 `ALTER … ADD INDEX`. So `NULLS FIRST/LAST` becomes grammatically accepted in comdb2 **DDL** (index/PK definitions), not just query ORDER BY.
- **comdb2 work:** For query `ORDER BY`, ordering is done by SQLite's sorter (internal, no comdb2 work). For **index/PK DDL**, the sortlist is consumed by `comdb2AddIndex` / `comdb2AddPrimaryKey` — comdb2's schema engine does not today model per-column NULLS ordering; the clause would be parsed and either ignored or misinterpreted. **Needs review / likely should be rejected in DDL.**
- **Nature:** sugar+internal for queries; **NOT pure sugar for DDL** — flag.

### N2 — `IS [NOT] DISTINCT FROM`
- Grammar: `expr IS NOT DISTINCT FROM expr` → `TK_IS`; `expr IS DISTINCT FROM expr` → `TK_ISNOT` (same codegen as existing `IS` / `IS NOT`).
- **Surface:** any expression context (WHERE, ON, HAVING, SELECT list, CHECK, partial-index WHERE, …).
- **comdb2 work:** none — pure alias of existing `IS`/`IS NOT` operators, fully internal.
- **Nature:** pure syntactic sugar, internal.

### N3 — `->` and `->>` JSON operators (PTR token)
- Grammar: `expr PTR expr` → `sqlite3ExprFunction` with function name `"->"` / `"->>"`; precedence `%left CONCAT PTR.`. Needs `PTR` from the upgraded tokenizer.
- **Surface:** any expression context.
- **Relation to comdb2 JSON:** `SQLITE_ENABLE_JSON1` is defined; 3.51.2 `json.c` registers `"->"`/`"->>"` (json.c:3980–3981) as functions (abbreviated-path json_extract variants: `->` returns a JSON subcomponent, `->>` returns an SQL text/number). comdb2 already exposes `json_extract()` from json1, so the operators are the same subsystem. **Confirm comdb2 uses stock json1** (not a custom json_extract that omits the operators) — if stock, no comdb2 work; the operators arrive with the json.c upgrade.
- **Nature:** sugar over json1 functions; internal to the JSON extension. Flag: verify JSON provenance.

### N4 — Aggregate `ORDER BY`: `aggfunc(args ORDER BY sortlist)`
- Grammar: new `expr ::= idj LP distinct exprlist ORDER BY sortlist RP` (+ window variant); `sqlite3ExprAddFunctionOrderBy`.
- **Surface:** aggregate function calls in SELECT (and window form). Common use: `group_concat(x ORDER BY y)`.
- **comdb2 work:** ordering of aggregate input rows is performed internally by SQLite's aggregate machinery (VDBE), before values reach the aggregate step — works for SQLite-native aggregates with no comdb2 change. **Flag:** verify interaction with comdb2 user-defined / Lua aggregates (`comdb2CreateAggFunc`).
- **Nature:** internal aggregate-evaluation behavior; effectively transparent to comdb2 except custom aggregates.

### N5 — `FILTER (WHERE …)` on aggregates (without `OVER`)
- Grammar: `filter_over` restructuring lets a `filter_clause` attach to a plain aggregate (3.28 allowed FILTER only with `OVER`). Requires windowfunc (enabled).
- **Surface:** aggregate function calls, e.g. `count(*) FILTER (WHERE x>0)`.
- **comdb2 work:** filter applied during aggregate accumulation, internal to SQLite. **Flag:** same custom/Lua-aggregate caveat as N4.
- **Nature:** internal; transparent except custom aggregates.

### N6 — `RAISE(action, <expr>)` (expression message)
- Grammar: `expr ::= RAISE LP raisetype COMMA expr RP` (was `nm` in 3.28) → `sqlite3PExpr(TK_RAISE, Z, 0)`.
- **Surface:** `RAISE()` inside `CREATE TRIGGER` bodies (comdb2 uses SQLite triggers via `createkw trigger_decl … BEGIN … END`).
- **comdb2 work:** none beyond trigger codegen (internal). Widens the message argument from a name to any expression.
- **Nature:** minor syntax widening in triggers; internal.

### N7 — Digit separators in numeric literals (`QNUMBER`)
- Grammar: new `term ::= QNUMBER` → `tokenExpr` + `sqlite3DequoteNumber`. Tokenizer emits `TK_QNUMBER` for numeric literals containing `_` (`SQLITE_DIGIT_SEPARATOR`), e.g. `1_000_000`, `0xFF_FF`, `1_000.5`. `sqlite3DequoteNumber` strips `_` and yields INTEGER/FLOAT (util.c:332).
- **Surface:** any numeric literal in any expression.
- **comdb2 work:** none — purely tokenizer/parse-time normalization. Depends on the tokenize.c upgrade.
- **Nature:** pure syntactic sugar, internal.

### Internal-only changes flowing in (no new user syntax)
- **Multi-row VALUES** rebuilt via `mvalues`/`sqlite3MultiValues` — same `VALUES (…),(…)` syntax; internal memory/structure change. (Depends on select.c.)
- `expr AND expr` now `sqlite3ExprAnd` (constant-fold/short-circuit); `IN (…)` simplifications; unary `+`/`-` folding; `sqlite3ExprSetErrorOffset` on `*`/INTEGER (error-position reporting); `case_operand ::= expr` cleanup — all internal, no syntax change.

**Whole-upgrade dependency note:** every "flows-in" feature needs its callee/token from sibling upgraded files (tokenize.c → PTR/QNUMBER/COMMENT; expr.c → sqlite3ExprAnd/AddFunctionOrderBy/ExprSetErrorOffset; select.c → sqlite3MultiValues/on_using; build.c/util.c → sqlite3ExprListSetSortOrder 3-arg/sqlite3DequoteNumber; json.c → `->`/`->>`). These are out of scope for parse.y but required for the parser to link.
