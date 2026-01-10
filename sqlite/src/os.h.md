# os.h Modifications

## Summary

A single modification to change the **temporary file prefix** for comdb2.

## Modifications

### 1. Change Temp File Prefix (Lines 65-69)
```c
#ifndef SQLITE_TEMP_FILE_PREFIX
#if defined(SQLITE_BUILDING_FOR_COMDB2)
# define SQLITE_TEMP_FILE_PREFIX "sqlsort_"
#else
# define SQLITE_TEMP_FILE_PREFIX "etilqs_"
#endif
#endif
```

SQLite normally uses `etilqs_` ("sqlite" spelled backwards) as the prefix for temporary files. Comdb2 changes this to `sqlsort_`.

## Why This Change

The `sqlsort_` prefix makes it easier to identify temporary files created by comdb2's SQL sorting operations. This is useful for:
1. **Debugging**: Quickly identifying which temp files are from SQL operations
2. **Monitoring**: Tracking temporary file usage specifically from the SQL layer
3. **Cleanup**: Easily finding and cleaning up SQL-related temp files
