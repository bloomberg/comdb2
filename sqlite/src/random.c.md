# random.c Modifications

## Summary

Modified the random number generator state to be **thread-local** instead of globally shared.

## Modifications

### 1. Thread-Local PRNG State (Lines 24-28)
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
static __thread SQLITE_WSD struct sqlite3PrngType {
#else
static SQLITE_WSD struct sqlite3PrngType {
#endif
```

The `sqlite3PrngType` structure (the PRNG state) is declared with `__thread` storage class, making it thread-local.

## Why This Change

Standard SQLite uses a single global PRNG state protected by mutexes. This creates contention in highly concurrent environments.

By making the PRNG state thread-local, each thread gets its own independent random number generator. This:
1. **Eliminates contention**: No mutex needed for random number generation
2. **Improves performance**: Random number generation is a common operation (for query optimization, temp file names, etc.)
3. **Maintains thread safety**: Each thread's PRNG is completely isolated

The trade-off is slightly more memory usage (one PRNG state per thread), but this is minimal (~260 bytes per thread) and worthwhile for a highly concurrent database like comdb2.
