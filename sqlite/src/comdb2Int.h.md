# comdb2Int.h

## Summary

A simple header file that **aggregates the main comdb2 internal headers**.

## Contents

```c
#ifndef COMDB2INT_H
#define COMDB2INT_H

#include "comdb2build.h"
#include "comdb2vdbe.h"

#endif
```

## Purpose

This is a convenience header that provides a single include for the two main comdb2 SQLite extension headers:

1. **comdb2build.h** - Contains declarations for DDL/schema building functions
2. **comdb2vdbe.h** - Contains declarations for VDBE (Virtual Database Engine) extensions

By including `comdb2Int.h`, other files get access to all comdb2-specific SQLite internals in one include statement.

## Why This File Exists

This pattern of having a "master" internal header is common in C projects. It:
- Simplifies include statements in implementation files
- Provides a single point for adding new comdb2-specific headers
- Maintains consistency across the codebase
