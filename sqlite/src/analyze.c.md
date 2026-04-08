# Comdb2 Patches to `src/analyze.c`

Summary of all changes made to SQLite's `src/analyze.c` under `SQLITE_BUILDING_FOR_COMDB2`.

---

## 1. Packed Row Instead of Rowid (Sample Storage)

The biggest structural change. Comdb2 doesn't use SQLite rowids. Instead of storing a rowid reference in each `Stat4Sample`, comdb2 stores a **packed row** (`u8 packedRow[1024]` + `u32 nPackedRow`). This ripples through:
- `Stat4Sample` struct: rowid union/`nRowid` replaced with `packedRow`/`nPackedRow`
- `sampleClear()`: zeroes the packed row instead of freeing a rowid blob
- `sampleSetPackedRow()` replaces `sampleSetRowid()` / `sampleSetRowidInt64()`
- `sampleCopy()`: copies packed row instead of rowid
- `stat_push()`: stores packed row from `argv[2]` blob instead of rowid int/blob
- `stat_get()`: new `STAT_GET_ROW` (value 4) replaces `STAT_GET_ROWID`; returns the packed row blob directly instead of seeking back to the table by rowid
- VDBE codegen in `analyzeOneTable()`: uses `OP_MakeRecord` on index columns to create the sample row, instead of seeking the table cursor by rowid

## 2. Dynamic Sample Count (Adaptive Sampling)

Instead of a fixed 24 samples, comdb2 scales the number of stat4 samples based on table size via `numRowsToNumSamplesEst()` (24 for <10K rows up to 512 for >100M rows). Two tunables control this:
- `stat4_samples_multiplier` — multiplied against the base estimate
- `stat4_extra_samples` — added on top

## 3. Actual Row Count Tracking

A new field `Stat4Accum.nActualRow` captures the "actual" row count (from `analyze_get_nrecs()`), separate from the VDBE-counted `nRow`. This is used to:
- Report more accurate row counts in stat1 output
- Scale stat4 `anEq`/`anLt`/`anDLt` values when actual vs. sampled counts diverge

## 4. `nKeyCol` Removed / Column Count Adjusted

Comdb2 removes `Stat4Accum.nKeyCol` entirely. The column count (`nCol`) is set to `nKeyCol + 1` (including an extra rowid column), so assertions change from `nCol>0` to `nCol>1`. The stat1 loop iterates over `nCol-1` instead of `nKeyCol`.

## 5. DATACOPY-Aware Column Counting

When setting up analysis for an index, comdb2 scans collation names and stops at the first `"DATACOPY"` column, effectively excluding datacopy columns from statistics collection.

## 6. Stat Table Opening (Non-Expert Mode)

When `db->isExpert==0`, comdb2 opens stat tables by looking them up with `sqlite3FindTable` and using `OP_OpenWrite` directly (by `tnum`), rather than creating/dropping them. It uses `sqlite3NestedParse("DELETE FROM ...")` instead of `OP_Clear` to clear stat table rows. A `skip4` thread-local tracks whether `sqlite_stat4` exists.

## 7. Completely Rewritten `loadStat4()`

The upstream `loadStat4()` is replaced. The comdb2 version:
- Queries `sqlite_stat4` directly, filtering out `cdb2.*.sav` rows
- Lazily creates `pIdx->pKeyInfo` via `sqlite3KeyInfoOfIndex()`
- Uses a dynamically growing `aSample[]` array (realloc-based) with separate mallocs for each sample's `anEq`/`anLt`/`anDLt`
- Inserts samples in **sorted order** using binary search + insertion sort (upstream stores them sequentially)
- Uses `sqlite3FindTableCheckOnly()` instead of `sqlite3FindTable()`
- Includes `#include <vdbecompare.c>` to get `sqlite3VdbeRecordCompare` at file scope

## 8. Skip-Scan Control

After decoding stat1 data, comdb2 sets `pIndex->noSkipScan` based on:
- `is_comdb2_index_disableskipscan()` for local tables (iDb==0)
- Always disabled for foreign tables (iDb > 1)

## 9. Analyze Empty Tables Support

A tunable `analyze_empty_tables` changes control flow so that stat1/stat4 entries are generated even for empty indexes (changes where `addrRewind` jump targets are placed).

## 10. Sample Overflow Handling

When `iMin == -1` (all samples are periodic and none can be evicted), comdb2 triples `nPSample` and demotes every 3rd sample, then retries — handling cases where the row count has grown significantly since `statInit()`.

## 11. `sqlite3AnalysisLoad()` Changes

- Uses `sqlite3FindTableByAnalysisLoad()` and `sqlite3FindTableCheckOnly()` instead of `sqlite3FindTable()`
- Filters out `cdb2.*.sav` entries from stat1 queries
- Protects against wiping stats for already-attached tables during dynamic attach
- Manages `pKeyInfo` reference counting after loading stat4
- Removes the `else` branch that handles NULL index names (stale stat1 entries) to avoid bad estimates
- Removes `loadAnalysis()` call and `OP_Expire` from the `ANALYZE` command path

## 12. Minor / Miscellaneous

- `#include <logmsg.h>` for comdb2 logging
- `SQLITE_INDEX_SAMPLES` defined as 10
- `PACKEDROWSIZE` defined as 1024
- `sqlite3SchemaMutexHeld` assertion kept (upstream removed it)
- `deleteIndexSamples()` zeros out `nAlloc`/`nSample`/`aSample` and frees per-sample arrays individually
- `initAvgEq()` early-returns if `aSample==0`; uses `nSampleCol` directly instead of `nSampleCol-1`
