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

## Handling security bugs

Like many other Open Source projects, the security bugs are handled differently
as compared to ordinary bugs (for obvious reasons). In case, you happen to have
found a bug in Comdb2 which you believe could be classified as a security issue
or have a patch for the same, it is highly recommended to report it directly to
the Comdb2 team at [opencomdb2@bloomberg.net](mailto:opencomdb2@bloomberg.net).
