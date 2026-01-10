# hash.h Modifications

## Summary

A new hash lookup function declaration added for **query normalization** support.

## Modifications

### 1. Add sqlite3HashFindN Declaration (Lines 71-75)
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
#ifdef SQLITE_ENABLE_NORMALIZE
void *sqlite3HashFindN(const Hash*, const char *pKey, int nKey);
#endif
#endif
```

This declares a new function `sqlite3HashFindN` that looks up a hash table entry using a key with an explicit length, rather than a null-terminated string.

## Why This Change

Query normalization (`SQLITE_ENABLE_NORMALIZE`) is used for query fingerprinting - converting queries into a canonical form for statistics, caching, and analysis. The `sqlite3HashFindN` function allows looking up normalized query strings that may not be null-terminated, which is more efficient when working with substrings or pre-computed lengths.

This is useful for comdb2's query tracking and performance monitoring features.
