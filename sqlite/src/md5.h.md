# md5.h

## Summary

**MD5 hash function** implementation for comdb2.

## Contents

### MD5Context Structure
```c
struct MD5Context {
  int isInit;
  uint32 buf[4];
  uint32 bits[2];
  unsigned char in[64];
};
```

The context structure maintains state during incremental MD5 computation.

### Functions

| Function | Description |
|----------|-------------|
| `MD5Init(ctx)` | Initialize an MD5 context |
| `MD5Update(ctx, buf, len)` | Add data to the hash |
| `MD5Final(digest, ctx)` | Finalize and get the 16-byte digest |
| `MD5DigestToBase16(digest, zBuf)` | Convert digest to hex string (32 chars) |
| `MD5DigestToBase10x8(digest, zDigest)` | Convert digest to decimal string format |

## Purpose

MD5 is used in comdb2 for:
1. **Query fingerprinting** - Creating unique identifiers for SQL queries
2. **Data checksums** - Verifying data integrity
3. **Cache keys** - Generating keys for caching mechanisms

## Why This File Exists

While MD5 is cryptographically broken and shouldn't be used for security, it's still useful for:
- Non-cryptographic hashing (fast, widely supported)
- Compatibility with existing systems expecting MD5 checksums
- Query normalization and tracking

Note: This appears to be derived from public domain MD5 implementations commonly used in database systems.
