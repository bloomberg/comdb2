---
title: Contributing to Comdb2
sidebar: mydoc_sidebar
permalink: contrib.html
---

Please submit a pull request!  We welcome code and idea contributions.

## Code style

### Indentation, etc.

Comdb2 contains code from multiple open source libraries.   If editing existing code for, for example, BerkeleyDB and
SQLite, please stick to existing conventions for those code bases.  For Comdb2 code, it's highly recommended that
before you creating a pull request, please format your code with `clang` via the following:

```
clang-format -style="{BasedOnStyle: llvm, IndentWidth: 4, UseTab: Never, BreakBeforeBraces: Linux, SortIncludes: false, IndentCaseLabels: false, AlwaysBreakTemplateDeclarations: true, AllowShortFunctionsOnASingleLine: false, AllowShortCaseLabelsOnASingleLine: true, AllowShortIfStatementsOnASingleLine: true}"
```

### Producing trace

Please use logmsg with an [appropriate log level](op.html#logmsg-level) instead of writing directly to the output.

### Documentation

If introducing new commands, please update the corresponding documentation.
