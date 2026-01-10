# os_unix.c Modifications

## Summary

Modifications to Unix file operations for **debugging**, **portability**, and **temp directory configuration**.

## Modifications

### 1. Additional Headers (Lines 49-54, 150)
```c
#ifdef SQLITE_UNIX_THREADS
#include <pthread.h>
#endif
#include <logmsg.h>
```

### 2. SQLITE_BUSY Debugging (Lines 1727-1846)
Multiple additions of debug logging when `SQLITE_BUSY` is returned:
```c
logmsg(LOGMSG_ERROR, "%s:%d SQLITE_BUSY\n", __FILE__, __LINE__);
cheap_stack_trace();
```

These appear at:
- File lock contention (pending/exclusive lock conflicts)
- Failed read locks
- Failed exclusive lock attempts
- Network mount unlock failures

### 3. Extended getpagesize Support (Lines 4214-4225)
```c
#elif defined(_DEFAULT_SOURCE)
  return getpagesize();
```

Added support for `_DEFAULT_SOURCE` macro (modern glibc) in addition to the existing `_BSD_SOURCE`.

### 4. Custom Temp Directory (Lines 5693-5731)
```c
static const char *unixTempFileDir(void){
#if defined(SQLITE_BUILDING_FOR_COMDB2)
   return comdb2_get_tmp_dir();
#else
  // ... original search logic ...
#endif
}
```

Comdb2 provides its own temp directory via `comdb2_get_tmp_dir()` instead of searching system directories.

## Why These Changes

1. **SQLITE_BUSY Debugging**: Comdb2 is a distributed database where lock contention should be rare. When it occurs, it usually indicates a bug or misconfiguration. The logging and stack traces help diagnose these issues.

2. **Portability**: The `_DEFAULT_SOURCE` macro is the modern replacement for `_BSD_SOURCE` in glibc 2.19+. This ensures compatibility with newer Linux distributions.

3. **Temp Directory**: Comdb2 manages its own temp directory location (typically configured per-instance) rather than using system defaults. This allows:
   - Instance isolation on shared machines
   - Placement on appropriate storage (fast SSD, etc.)
   - Monitoring and cleanup of temp files
