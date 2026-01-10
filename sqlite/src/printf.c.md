# printf.c Modifications

## Summary

Modifications to string buffer growth logic and **disabling debug print functions**.

## Modifications

### 1. Buffer Growth Edge Case (Lines 900-918)
```c
} else {
  /* It is now impossible to grow the buffer further exponentially.
  ** However, we could still have some room left to grow during this last
  ** leg of expansion. We do that by setting new size to maximum allowed
  ** size. One more thing to notice is that in the previous iteration, the
  ** sqlite memory allocator could have allocated more that the requested
  ** size (due to roundup) and the current buffer size (p->nAlloc) could
  ** have already exceeded the maximum allowed limit (p->mxAlloc). Let us
  ** bail out if we are already past the limit.
  **/
  if( p->nAlloc >= p->mxAlloc ){
    sqlite3_str_reset(p);
    setStrAccumError(p, SQLITE_TOOBIG);
    return 0;
  }
  szNew = p->mxAlloc;
}
```

Fixes an edge case where:
1. The buffer cannot double in size anymore (would overflow)
2. But there's still room below `mxAlloc` for a smaller growth
3. Additionally handles cases where memory allocator roundup has already exceeded the limit

Without this fix, the buffer would fail to grow even when there's valid room, or could allocate beyond the limit.

### 2. Disable Debug Print Functions (Lines 1244-1289)
```c
#if !defined(SQLITE_BUILDING_FOR_COMDB2)
#if defined(SQLITE_DEBUG) || defined(SQLITE_HAVE_OS_TRACE)
// sqlite3DebugPrintf()
// sqlite3TreeViewLine()
#endif
#endif
```

The `sqlite3DebugPrintf()` and related debug output functions are disabled.

## Why These Changes

1. **Buffer Growth Fix**: This is likely a bug fix that was discovered during comdb2 usage with very large strings. The fix ensures the buffer can use all available space up to `mxAlloc` even when exponential growth is no longer possible.

2. **Debug Print Disabled**: Comdb2 has its own logging infrastructure (`logmsg`). Disabling SQLite's debug print functions:
   - Avoids duplicate/conflicting debug output mechanisms
   - Prevents accidental debug output to stdout in production
   - Reduces binary size slightly when debug symbols are included
