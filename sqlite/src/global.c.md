# global.c Modifications

## Summary

A single modification to the global configuration to **disable the default page cache initialization size** for comdb2.

## Modifications

### 1. Disable Default Page Cache Size (Lines 224-228)
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
   0,                         /* nPage */
#else
   SQLITE_DEFAULT_PCACHE_INITSZ, /* nPage */
#endif
```

The `nPage` field in the global SQLite configuration is set to 0 instead of `SQLITE_DEFAULT_PCACHE_INITSZ`.

## Why This Change

Comdb2 does not use SQLite's page cache system. Since comdb2 uses BerkeleyDB as its storage layer, BerkeleyDB handles all page caching. Setting the SQLite page cache to 0 prevents unnecessary memory allocation for a cache that will never be used.
