# status.c Modifications

## Summary

Modifications to **disable page cache mutex assertions** and change mutex acquisition for status reporting.

## Modifications

### 1. Disable Pcache Mutex Assertions (Lines 73, 95, 107, 127)
```c
#if !defined(SQLITE_BUILDING_FOR_COMDB2)
  assert( sqlite3_mutex_held(statMutex[op] ? sqlite3Pcache1Mutex()
                                           : sqlite3MallocMutex()) );
#endif
```

Multiple assertions checking that the correct mutex is held are disabled for comdb2.

### 2. Always Use Malloc Mutex for Status (Lines 155-159)
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  pMutex = sqlite3MallocMutex();
#else
  pMutex = statMutex[op] ? sqlite3Pcache1Mutex() : sqlite3MallocMutex();
#endif
```

When getting status, comdb2 always uses the malloc mutex instead of potentially using the page cache mutex.

## Why These Changes

1. **No Page Cache**: Comdb2 doesn't use SQLite's page cache (it uses BerkeleyDB's), so there is no page cache mutex. The assertions would fail.

2. **Simplified Mutex**: Since there's no page cache, all status operations can just use the malloc mutex, simplifying the code.

These changes prevent assertion failures and runtime errors that would occur because comdb2 has disabled SQLite's page cache system.
