# threads.c Modifications

## Summary

Modified thread creation to use comdb2's wrapper function and simplify error handling.

## Modifications

### 1. Use Pthread_create Wrapper (Lines 75-82)
```c
// Original:
if( sqlite3FaultSim(200) ){
  rc = 1;
}else{
  rc = pthread_create(&p->tid, 0, xTask, pIn);
}
if( rc ){
  p->done = 1;
  p->pOut = xTask(pIn);
}

// Modified:
if( sqlite3FaultSim(200) ){
  p->done = 1;
  p->pOut = xTask(pIn);
}else{
  Pthread_create(&p->tid, 0, xTask, pIn);
}
```

Key changes:
1. Uses `Pthread_create` (capital P) instead of `pthread_create`
2. Removes the fallback-to-synchronous execution on thread creation failure

## Why These Changes

1. **Pthread_create wrapper**: Comdb2 has its own pthread wrapper (`Pthread_create`) that likely:
   - Handles thread creation errors differently (possibly aborting on failure)
   - Adds thread tracking/debugging capabilities
   - Configures thread attributes consistently

2. **Removed error fallback**: The original code would fall back to running the task synchronously if thread creation failed. With comdb2's wrapper, thread creation failures are presumably fatal or handled elsewhere, so this fallback is unnecessary.

This integrates SQLite's threading with comdb2's broader thread management infrastructure.
