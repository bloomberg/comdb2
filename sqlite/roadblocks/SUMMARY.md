# SQLite Upgrade Roadblocks Summary

## Overview

This document summarizes all anticipated roadblocks for upgrading Comdb2's SQLite from **3.28.0** to **3.51.2**.

**Total releases to traverse:** 55+ releases (3.29.0 through 3.51.2)

## Critical Roadblocks (Must Address)

These changes require significant attention and will likely cause merge conflicts with Comdb2 modifications.

### 1. ALTER TABLE DROP COLUMN (3.35.0)

**Risk Level:** Critical

**Files Affected:**
- `build.c` - DDL handling (heavily modified by Comdb2)
- `parse.y` - Grammar (heavily modified by Comdb2)
- `sqliteInt.h` - Table structure

**Comdb2 Impact:**
- Comdb2 routes DDL to its own schema engine
- Must integrate or bypass this new DDL feature

**Action:** Review ALTER TABLE implementation and update Comdb2 DDL routing.

---

### 2. Generalized UPSERT - Multiple ON CONFLICT (3.35.0)

**Risk Level:** Critical

**Files Affected:**
- `insert.c` - UPSERT handling (heavily modified by Comdb2)
- `parse.y` - Grammar
- `upsert.c` - UPSERT internals

**Comdb2 Impact:**
- Comdb2 has `comdb2SetUpsertIdx()`, `need_index_checks_for_upsert()`
- Multiple ON CONFLICT requires updated logic

**Action:** Update UPSERT handling in insert.c for multiple ON CONFLICT clauses.

---

### 3. RIGHT and FULL OUTER JOIN (3.39.0)

**Risk Level:** Critical

**Files Affected:**
- `select.c` - JOIN handling (heavily modified by Comdb2)
- `parse.y` - Grammar
- `where*.c` - Query planning

**Comdb2 Impact:**
- Comdb2 has ~70 modification blocks in select.c
- New JOIN types require careful integration

**Action:** Carefully merge RIGHT/FULL OUTER JOIN support into Comdb2's select.c.

---

### 4. RETURNING Clause (3.35.0)

**Risk Level:** High

**Files Affected:**
- `parse.y` - Grammar
- `insert.c`, `update.c`, `delete.c` - DML statements

**Comdb2 Impact:**
- Affects INSERT/UPDATE/DELETE which have Comdb2 modifications
- May conflict with Comdb2's DDL routing

**Action:** Integrate RETURNING clause support.

---

## High-Risk Roadblocks

### 5. Generated Columns (3.31.0)

**Files Affected:** `build.c`, `parse.y`, `sqliteInt.h`

**Impact:** Table structure changes, new DDL syntax.

**Action:** Review Table structure extensions, update DDL handling.

---

### 6. STRICT Tables (3.37.0)

**Files Affected:** `build.c`, `parse.y`

**Impact:** New table mode with strict type checking. May conflict with Comdb2's type system.

**Action:** Decide if Comdb2 should support or disable STRICT tables.

---

### 7. JSON Rewrite with JSONB (3.45.0)

**Files Affected:** JSON function implementations

**Impact:** Complete JSON internals rewrite. If Comdb2 uses JSON functions, needs testing.

**Action:** Test JSON compatibility if used.

---

### 8. OP_SeekScan Opcode (3.34.0)

**Files Affected:** `vdbe.c`, `wherecode.c`

**Impact:** Comdb2 has `gbl_seekscan_maxsteps` tunable for this opcode.

**Action:** Ensure tunable logic still applies to new implementation.

---

### 9. Bloom Filters in Query Planner (3.38.0)

**Files Affected:** `wherecode.c`, `where.c`

**Impact:** New optimization in WHERE clause handling (heavily modified by Comdb2).

**Action:** Review Bloom filter integration with Comdb2's cursor hints.

---

### 10. sqlite3_filename Typedef (3.40.0)

**Files Affected:** Multiple files with filename parameters

**Impact:** Interface changes for file handling.

**Action:** Update Comdb2 code that passes filenames to SQLite functions.

---

## Medium-Risk Roadblocks

| Version | Change | Files Affected | Notes |
|---------|--------|----------------|-------|
| 3.30.0 | ORDER BY/LIMIT on DELETE/UPDATE | parse.y, update.c, delete.c | Grammar changes |
| 3.35.0 | Math functions | func.c | Check for name conflicts |
| 3.38.0 | JSON -> and ->> operators | tokenize.c | Tokenizer changes |
| 3.38.0 | Date function changes | func.c | Comdb2 has datetime modifications |
| 3.40.0 | Expression index extraction | wherecode.c | Comdb2 has expression index support |
| 3.43.0 | Aggregate function ORDER BY | parse.y, select.c | Grammar and handling changes |
| 3.45.0 | SQLITE_RESULT_SUBTYPE | vdbeapi.c | UDF API change |
| 3.50.0 | sqlite3_setlk_timeout() | New API | Consider for Comdb2 lock handling |

## Low-Risk Roadblocks

| Version | Change | Notes |
|---------|--------|-------|
| 3.29.0 | DQS configuration | Double-quote string handling |
| 3.29.0 | Query planner AND/OR | May affect query plans |
| 3.30.0 | FILTER clause | New aggregate syntax |
| 3.32.0 | Approximate ANALYZE | ANALYZE sampling changes |
| 3.36.0 | EXPLAIN format | Output format changes |
| 3.40.0 | PRNG RC4→Chacha20 | Randomness algorithm |
| 3.44.0 | concat() function | Check for conflicts |
| 3.49.0 | ENABLE_COMMENTS | SQL parsing option |

## Summary by Comdb2 Component

### parse.y (Most Affected)
- 3.30.0: ORDER BY/LIMIT on DELETE/UPDATE
- 3.31.0: Generated columns
- 3.35.0: ALTER TABLE DROP COLUMN, UPSERT, RETURNING
- 3.37.0: STRICT tables
- 3.39.0: RIGHT/FULL OUTER JOIN
- 3.43.0: Aggregate ORDER BY

### insert.c
- 3.35.0: Generalized UPSERT, RETURNING

### select.c
- 3.39.0: RIGHT/FULL OUTER JOIN
- 3.43.0: Aggregate changes

### build.c
- 3.31.0: Generated columns
- 3.35.0: ALTER TABLE DROP COLUMN
- 3.37.0: STRICT tables

### vdbe.c / wherecode.c
- 3.34.0: OP_SeekScan
- 3.38.0: Bloom filters
- 3.40.0: Expression index extraction

### func.c
- 3.35.0: Math functions
- 3.38.0: Date functions
- 3.44.0: concat(), string_agg()

### analyze.c
- 3.32.0: Approximate ANALYZE
- 3.45.0: Index quality handling

## Recommended Upgrade Path

### Phase 1: Foundation (3.29.0 - 3.34.1)
- Lower risk changes
- OP_SeekScan introduction
- Establish baseline compatibility

### Phase 2: Major Features (3.35.0 - 3.37.2)
- **Most complex phase**
- ALTER TABLE DROP COLUMN
- Generalized UPSERT
- RETURNING clause
- STRICT tables

### Phase 3: Joins and JSON (3.38.0 - 3.42.0)
- RIGHT/FULL OUTER JOIN
- Bloom filters
- JSON improvements

### Phase 4: Stabilization (3.43.0 - 3.51.2)
- Mostly incremental changes
- Testing and refinement

## Testing Requirements

1. **DDL Tests**
   - All CREATE/ALTER/DROP variants
   - DRYRUN mode
   - Schema versioning

2. **DML Tests**
   - UPSERT with multiple ON CONFLICT
   - RETURNING clause
   - INSERT/UPDATE/DELETE with all options

3. **Query Tests**
   - All JOIN types including RIGHT/FULL OUTER
   - Complex WHERE clauses
   - Aggregates with ORDER BY

4. **Type Tests**
   - datetime, interval, decimal
   - Type conversions
   - STRICT table compatibility

5. **Distributed Tests**
   - Remote table access (FDB)
   - Schema change propagation
   - Transaction handling

## Conclusion

The upgrade from SQLite 3.28.0 to 3.51.2 spans approximately 6 years of SQLite development with major features added. The most challenging aspects will be:

1. **Grammar changes** in parse.y (many new SQL features)
2. **UPSERT and DML changes** in insert.c
3. **JOIN handling** in select.c
4. **Query optimization** changes in where*.c

A phased approach with thorough testing after each major version milestone is recommended.
