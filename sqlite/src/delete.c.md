# delete.c Modifications

## Summary

Modifications to DELETE statement processing for comdb2's architecture, including **LIMIT clause control**, **AST tracking**, **deferred seek handling**, and **index management**.

## Modifications

### 1. External Declarations (Lines 17-20)
```c
extern int gbl_update_delete_limit;
int has_comdb2_index_for_sqlite(Table *pTab);
```

References to comdb2's global configuration and index checking functions.

### 2. LIMIT Clause Enforcement (Lines 166-177)
```c
if (pLimit && gbl_update_delete_limit == 0) {
  sqlite3ErrorMsg(pParse,
                  "LIMIT on %s without lrl option: update_delete_limit",
                  zStmtType);
  goto limit_where_cleanup;
}
```

DELETE with LIMIT is disabled by default unless explicitly enabled via the `update_delete_limit` configuration option. This prevents potentially dangerous operations.

### 3. Early VDBE Acquisition (Lines 329-334)
```c
v = sqlite3GetVdbe(pParse);
if( v==0 ){
  goto delete_from_cleanup;
}
```

Gets the VDBE earlier in the processing to ensure it's available for AST tracking.

### 4. AST (Abstract Syntax Tree) Tracking (Lines 358-362)
```c
ast_t *ast = ast_init(pParse, __func__);
if( ast ) ast_push(ast, AST_TYPE_DELETE, v, (iDb>1) ? pTab : NULL);
```

Adds DELETE operations to comdb2's AST tracking system for query analysis and debugging.

### 5. Remove WHERE_SEEK_TABLE Flag (Line 432)
```c
// Removed WHERE_SEEK_TABLE from:
u16 wcf = WHERE_ONEPASS_DESIRED|WHERE_DUPLICATES_OK;
```

The `WHERE_SEEK_TABLE` flag is removed, likely because comdb2's cursor handling works differently.

### 6. Deferred Seek Handling (Lines 478-480)
```c
if( sqlite3WhereUsesDeferredSeek(pWInfo) ){
  sqlite3VdbeAddOp1(v, OP_FinishSeek, iTabCur);
}
```

Adds explicit `OP_FinishSeek` instruction when using deferred seeks.

### 7. Modified ONEPASS Handling (Lines 503-548)
Restructured the ONEPASS vs multi-pass logic with different label creation timing.

### 8. Extended sqlite3OpenTableAndIndices Call (Lines 563-569)
```c
sqlite3OpenTableAndIndices(pParse, pTab, OP_OpenWrite, OPFLAG_FORDELETE,
                           iTabCur, aToOpen, &iDataCur, &iIdxCur, OE_None,
                           0);
```

Additional parameters for comdb2's conflict handling.

### 9. P5 Flag for NotFound (Lines 583-585)
```c
sqlite3VdbeChangeP5(v, 1);
```

Adds a flag to the `OP_NotFound` instruction.

### 10. Index Check in sqlite3GenerateRowIndexDelete (Lines 885-887)
```c
if ( !has_comdb2_index_for_sqlite(pTab) ) return;
```

Skip index deletion code generation if the table doesn't have SQLite-visible indexes.

## Why These Changes

1. **Safety Controls**: The `update_delete_limit` option prevents accidental deletion of large amounts of data
2. **Query Tracking**: AST tracking enables query analysis and debugging features
3. **Storage Integration**: Deferred seek and index handling integrate with BerkeleyDB's cursor model
4. **Index Management**: Comdb2 manages indexes differently, requiring checks before generating index deletion code
