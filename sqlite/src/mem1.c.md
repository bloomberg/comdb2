# mem1.c Modifications

## Summary

Modifications to the low-level memory allocator to use **comdb2's custom blob memory allocator** for large allocations.

## Modifications

### 1. Change Parameter Type
```c
// Changed from:
static void *sqlite3MemMalloc(int nByte);
// To:
static void *sqlite3MemMalloc(size_t nByte);
```

The parameter type changed from `int` to `size_t` to support larger allocations.

### 2. Large Allocation Routing (Lines 128-148)
```c
extern unsigned gbl_blob_sz_thresh_bytes;
extern comdb2bma blobmem;

if( nByte>gbl_blob_sz_thresh_bytes ){
  p = comdb2_bmalloc( blobmem, nByte );
}else{
  p = SQLITE_MALLOC( nByte );
}
```

When an allocation exceeds `gbl_blob_sz_thresh_bytes`, it is routed to comdb2's blob memory allocator (`blobmem`) instead of the standard allocator.

### 3. Same Pattern for Non-SQLITE_MALLOCSIZE Path (Lines 156-166)
The same large-allocation routing is applied in the code path where `SQLITE_MALLOCSIZE` is not defined.

## Why These Changes

1. **Size Type**: Using `size_t` instead of `int` allows for allocations larger than 2GB, which may be needed for very large blobs or result sets.

2. **Blob Memory Allocator**: Comdb2 has a specialized memory allocator (`comdb2bma`) for large "blob" allocations. This allocator likely:
   - Uses memory mapping for very large allocations
   - Has different fragmentation characteristics optimized for large blocks
   - May be monitored separately for memory usage tracking
   - Allows different memory limits for blob storage vs. general allocations

This separation helps comdb2 manage memory more effectively and prevents large blob allocations from fragmenting the general-purpose heap.
