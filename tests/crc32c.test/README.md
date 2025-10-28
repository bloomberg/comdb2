# `crc32c`

In the build, we compile crc32c twice:

1. First, as a library so it can be linked into the database server.
2. Second, as an executable that provides a `main()` function.

The latter is compiled with additional instrumentation (UBSAN) and its `main()`
runs a series of sanity checks that can be thought of as unit tests. We simply
run that executable in this test case.
