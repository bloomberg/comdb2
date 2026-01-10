# rowset.c Modifications

## Summary

Major modifications to use **BerkeleyDB temp tables** as an alternative backend for RowSet storage, providing better scalability for large row sets.

## Modifications

### 1. BerkeleyDB Integration Headers (Lines 66-73)
```c
#include <bdb_api.h>
#include <comdb2.h>
#include <logmsg.h>

int gbl_sqlite_use_temptable_for_rowset = 1;
extern struct dbenv *thedb;
```

Adds includes for BerkeleyDB API and a tunable to enable/disable the temp table backend.

### 2. Extended RowSet Structure (Lines 117-122)
```c
struct RowSet {
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  bdb_state_type *bdb_state;     /* points to thedb->bdb_env */
  struct temp_table *tmptbl;     /* temp table */
  struct temp_cursor *tmpcur;    /* temp cursor */
#endif
  // ... original fields ...
};
```

New fields to hold the BerkeleyDB temp table and cursor.

### 3. Temp Table Initialization (Lines 145-172)
Modified `sqlite3RowSetInit()` to optionally create a BerkeleyDB temp array instead of using the in-memory chunk-based storage.

### 4. Temp Table Cleanup (Lines 191-197)
Modified `sqlite3RowSetClear()` to close the temp cursor and table when using BerkeleyDB backend.

### 5. Temp Table Insert (Lines 267-278)
Modified `sqlite3RowSetInsert()` to insert into BerkeleyDB temp table using `bdb_temp_table_insert()`.

### 6. Temp Table Iteration (Lines 470-502)
Modified `sqlite3RowSetNext()` to iterate using `bdb_temp_table_first()` and `bdb_temp_table_next()`.

### 7. Temp Table Lookup (Lines 548-564)
Modified `sqlite3RowSetTest()` to use `bdb_temp_table_find_exact()` for membership testing.

## Why These Changes

SQLite's default RowSet implementation uses in-memory chunked storage with a tree structure. While fast for small sets, this has issues for large row sets:

1. **Memory consumption**: Large row sets can consume significant memory
2. **Scalability**: The in-memory tree structure may not scale well for millions of rows

By using BerkeleyDB temp tables:
1. **Disk spillover**: Large row sets can spill to disk automatically
2. **Better memory management**: BerkeleyDB manages memory more efficiently for large datasets
3. **Consistent with comdb2 architecture**: Uses the same temp table infrastructure as the rest of comdb2

The tunable `gbl_sqlite_use_temptable_for_rowset` allows disabling this for debugging or when small row sets are expected.
