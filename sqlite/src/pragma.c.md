# pragma.c Modifications

## Summary

A single modification to add **extra parameters** to the `sqlite3OpenTableAndIndices` function call.

## Modifications

### 1. Extended sqlite3OpenTableAndIndices Call (Lines 1549-1556)
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
sqlite3OpenTableAndIndices(pParse, pTab, OP_OpenRead, 0,
                           1, 0, &iDataCur, &iIdxCur, OE_None, 0);
#else
sqlite3OpenTableAndIndices(pParse, pTab, OP_OpenRead, 0,
                            1, 0, &iDataCur, &iIdxCur);
#endif
```

The comdb2 version passes two additional parameters:
- `OE_None` - Conflict resolution action (presumably for ON CONFLICT handling)
- `0` - Additional flags

## Why This Change

The `sqlite3OpenTableAndIndices` function has been extended in comdb2 to handle conflict resolution and additional cursor options. This is used throughout the codebase (in delete.c, insert.c, etc.) and needs to be consistently called with the extra parameters even in pragma processing code.

This specific instance is in the PRAGMA integrity_check or quick_check code that validates table/index consistency.
