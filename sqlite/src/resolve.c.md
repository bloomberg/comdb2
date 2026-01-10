# resolve.c Modifications

## Summary

Modifications to name resolution for **comdb2 special columns**, **strict double-quote handling**, **blob index tracking**, **window function fixes**, and **disabling rename object tracking**.

## Modifications

### 1. External Declarations (Lines 18-21)
```c
extern int gbl_strict_dbl_quotes;
int sqlite3IsComdb2Rowid(Table *pTab, const char *);
int sqlite3IsComdb2RowTimestamp(Table *pTab, const char *);
int is_comdb2_index_blob(const char *dbname, int icol);
```

### 2. Comdb2 Special Column Resolution (Lines 410-448)
```c
else if( cnt==0 && cntTab==1 && pMatch && sqlite3IsComdb2Rowid(pMatch->pTab, zCol) ){
   cnt = 1;
   pExpr->iColumn = -2;  // Magic value for comdb2_rowid
   pExpr->affinity = SQLITE_AFF_TEXT;
}else if( cnt==0 && cntTab==1 && pMatch && sqlite3IsComdb2RowTimestamp(pMatch->pTab, zCol) ){
   cnt = 1;
   pExpr->iColumn = -3;  // Magic value for row timestamp
   pExpr->affinity = SQLITE_AFF_TEXT;
}
```

Resolves special pseudo-columns:
- **comdb2_rowid** (iColumn = -2): Returns the rrn+genid (comdb2's row identifier)
- **comdb2_rowtimestamp** (iColumn = -3): Returns the row's timestamp

### 3. Blob Index Tracking (Lines 452-459)
```c
if( (pNC->ncFlags & (NC_PartIdx|NC_IdxExpr))!=0
 && pExpr->y.pTab
 && pExpr->iColumn>=0
){
  is_comdb2_index_blob(pExpr->y.pTab->zName, pExpr->iColumn);
}
```

When processing partial indexes or expression indexes, tracks if blob columns are used.

### 4. Strict Double-Quote Mode (Lines 518-525)
```c
if( gbl_strict_dbl_quotes ){
  sqlite3ErrorMsg(pParse, "double-quoted string literal: \"%w\"", zCol);
  return WRC_Abort;
}
```

When `gbl_strict_dbl_quotes` is enabled, double-quoted strings that don't match column names produce an error instead of being treated as string literals. This enforces SQL standard behavior.

### 5. Disable IN_RENAME_OBJECT Code (Multiple locations)
All `IN_RENAME_OBJECT` code blocks are wrapped in `#if !defined(SQLITE_BUILDING_FOR_COMDB2)` to disable SQLite's rename token tracking.

### 6. Function Not Found in Index Expressions (Lines 897-901)
```c
}else if( no_such_func && (pNC->ncFlags & (NC_IdxExpr|NC_PartIdx)) ){
  sqlite3ErrorMsg(pParse, "no such function: %.*s", nId, zId);
  pNC->nErr++;
}
```

Reports errors when unknown functions are used in index expressions.

### 7. Window Function Improvements (Lines 920-940)
Refactored window function handling:
- Adds `ppThis` pointer for doubly-linked list management
- Improves window removal from select's window list
- Skips `sqlite3WindowUpdate` during rename operations
- New public functions: `sqlite3WindowRemoveExprFromSelect()`, `sqlite3WindowRemoveExprListFromSelect()`

### 8. Updated sqlite3ExprIsInteger Calls (Lines 1103, 1167, 1411)
Changed from `sqlite3ExprIsInteger(pE, &i)` to `sqlite3ExprIsInteger(pE, &i, 0)` - adds a third parameter.

## Why These Changes

1. **Special Columns**: Comdb2 has pseudo-columns for accessing internal row metadata that don't exist in physical tables
2. **Strict Double Quotes**: Helps catch SQL portability issues where developers accidentally use double quotes for strings
3. **Blob Index Tracking**: Needed to validate that blob columns are properly handled in indexes
4. **Disable Rename Tracking**: Comdb2 has its own rename handling mechanism
5. **Window Function Fixes**: These appear to be bug fixes for window function management that may have been backported from a newer SQLite version
