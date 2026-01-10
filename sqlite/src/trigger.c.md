# trigger.c Modifications

## Summary

Modifications for **DRYRUN mode blocking**, **CSC2 schema integration**, **rename object disabling**, and **VDBE table transfer**.

## Modifications

### 1. DRYRUN Mode Blocking in BeginTrigger (Lines 106-110)
```c
if(comdb2IsDryrun(pParse)){
    sqlite3ErrorMsg(pParse, "DRYRUN not supported for this operation");
    goto trigger_cleanup;
}
```

Prevents trigger creation in DRYRUN mode.

### 2. Disable IN_RENAME_OBJECT in BeginTrigger (Lines 255-267)
```c
#if !defined(SQLITE_BUILDING_FOR_COMDB2)
if( IN_RENAME_OBJECT ){
  sqlite3RenameTokenRemap(pParse, pTrigger->table, pTableName->a[0].zName);
  pTrigger->pWhen = pWhen;
  pWhen = 0;
}else{
#endif
  pTrigger->pWhen = sqlite3ExprDup(db, pWhen, EXPRDUP_REDUCE);
#if !defined(SQLITE_BUILDING_FOR_COMDB2)
}
#endif
```

Always uses the `else` branch (expression duplication) rather than rename object tracking.

### 3. DRYRUN Mode Blocking in FinishTrigger (Lines 296-301)
```c
if(comdb2IsDryrun(pParse)){
    sqlite3ErrorMsg(pParse, "DRYRUN not supported for this operation");
    goto triggerfinish_cleanup;
}
```

Prevents trigger completion in DRYRUN mode.

### 4. CSC2 Column in Trigger Schema (Lines 340-346)
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
"INSERT INTO %Q.%s VALUES('trigger',%Q,%Q,0,'CREATE TRIGGER %q', NULL)",
#else
"INSERT INTO %Q.%s VALUES('trigger',%Q,%Q,0,'CREATE TRIGGER %q')",
#endif
```

Adds NULL for the CSC2 column when inserting trigger metadata.

### 5. Disable IN_RENAME_OBJECT in TriggerStep (Lines 438-442)
```c
#if !defined(SQLITE_BUILDING_FOR_COMDB2)
if( IN_RENAME_OBJECT ){
  sqlite3RenameTokenMap(pParse, pTriggerStep->zTarget, pName);
}
#endif
```

Disables rename token mapping for trigger steps.

### 6. DRYRUN Mode Blocking in DropTrigger (Lines 580-584)
```c
if(comdb2IsDryrun(pParse)){
    sqlite3ErrorMsg(pParse, "DRYRUN not supported for this operation");
    goto drop_trigger_cleanup;
}
```

Prevents trigger dropping in DRYRUN mode.

### 7. Remove ALWAYS() Assertion (Lines 674-679)
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
if( pTrigger ){
#else
if( ALWAYS(pTrigger) ){
#endif
```

Removes the `ALWAYS()` macro which asserts the condition is always true. In comdb2, the trigger might legitimately be NULL.

### 8. VDBE Table Transfer (Lines 990-993)
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
sqlite3VdbeTransferTables(pParse->pVdbe, pSubParse->pVdbe);
#endif
```

Transfers table references from sub-parse VDBE to main VDBE when compiling trigger programs.

## Why These Changes

1. **DRYRUN Mode**: Triggers involve complex metadata changes that can't be safely simulated. DRYRUN is comdb2's validation-only mode for testing DDL statements.

2. **CSC2 Column**: Maintains schema table structure consistency - all sqlite_master entries have the same column count.

3. **Disable Rename Object**: Comdb2 has its own rename handling and doesn't need SQLite's token tracking.

4. **Remove ALWAYS()**: In distributed scenarios, trigger lookup might fail legitimately.

5. **VDBE Table Transfer**: Ensures trigger programs have access to the same table information as the main query, important for proper execution in comdb2's environment.
