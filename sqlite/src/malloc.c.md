# malloc.c Modifications

## Summary

New **mutex-protected memory allocation functions** added for comdb2's multi-threaded environment.

## Modifications

### 1. sqlite3DbMallocWithMutex (Lines 593-612)
```c
void *sqlite3DbMallocWithMutex(sqlite3 *db, u64 n, int bZero);
```

A thread-safe version of `sqlite3DbMalloc` that:
- Acquires the database mutex before allocation
- Releases the mutex after allocation
- Optionally zeroes the allocated memory if `bZero` is set
- Falls back to global allocator if no database connection

### 2. sqlite3DbReallocWithMutex (Lines 614-638)
```c
void *sqlite3DbReallocWithMutex(sqlite3 *db, void *p, u64 n, int bZero);
```

A thread-safe version of `sqlite3DbRealloc` that:
- Acquires the database mutex before reallocation
- Releases the mutex after reallocation
- Optionally zeroes any newly allocated memory if `bZero` is set
- Falls back to global allocator if no database connection

## Why These Changes

Standard SQLite is designed for `SQLITE_THREADSAFE=0` or with explicit mutex management. Comdb2 is a highly concurrent database that may have multiple threads accessing the same database connection from different contexts (e.g., schema changes, parallel query execution).

These mutex-protected allocation functions ensure memory operations are thread-safe even when called from contexts where the caller hasn't already acquired the database mutex. This prevents race conditions and memory corruption in comdb2's multi-threaded architecture.
