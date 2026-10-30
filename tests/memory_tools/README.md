# Memory-tool variants and detector self-test

This directory defines the four supported ways to build and run Comdb2 under a
memory-safety tool, and a self-test that proves each tool is really active
before anything relies on its silence.

## Variants

| Variant | `COMDB2_SANITIZER` | `COMDB2_PER_THREAD_MALLOC` | Runtime detector | Probes that must be caught |
|---|---|---|---|---|
| `asan-system` | `address-undefined` | `OFF` | ASan + UBSan + LSan | heap-use-after-free, leak, comdb2-allocator-leak, signed-overflow |
| `asan-native` | `address-undefined` | `ON` | ASan + UBSan, leak checking disabled | heap-use-after-free, signed-overflow |
| `tsan-system` | `thread` | `OFF` | TSan | data-race |
| `valgrind-system` | `none` | `OFF` | Valgrind Memcheck | invalid-read, uninitialized-read, leak, comdb2-allocator-leak |

Every variant also runs the `clean` probe, which must exit 0 with no finding.

## Running the self-test

```bash
tests/memory_tools/selftest.sh --variant asan-system \
    --build-dir /tmp/comdb2-memory/asan-system/build \
    --artifact-dir /tmp/comdb2-memory/asan-system/artifacts \
    --jobs 8
```

`--build-dir` and `--artifact-dir` default to
`${TMPDIR:-/tmp}/comdb2-memory/<variant>/{build,artifacts}`. Both are cleared
first; the script refuses to clear a non-empty directory it did not create.
Set `CMAKE_GENERATOR=Ninja` to build with ninja. CI runs exactly this command.

The self-test:

1. checks the required tools (`gcc`, `cmake`, `python3`, and `valgrind` for
   `valgrind-system`) and records versions and resource limits;
2. refuses to run a sanitizer variant under a virtual-memory limit (`ulimit -v`);
3. configures a clean build with the variant's CMake options and checks the
   resulting cache;
4. builds the full `comdb2` target and `memory-tool-probes`;
5. checks with `ldd`/`readelf` that `comdb2` and every probe link and reference
   exactly the expected sanitizer runtimes (none for Valgrind);
6. runs each probe and checks both the exit status and the detector signature;
7. writes `summary.json` and `summary.txt`, and exits nonzero on any failure.

Artifacts:

```text
<artifact-dir>/
  tool-versions.txt  configure.log  build.log  linkage.txt
  summary.json  summary.txt
  probes/<probe>/
    command.txt  stdout.log  stderr.log  exit-code.txt  report.json
    detector/      asan.<pid>, tsan.<pid>, valgrind.<pid>.log, ...
```

## Running database tests under a memory tool

Set `MEMORY_TOOL_VARIANT` for an ordinary test run, and point `COMDB2_EXE` at
a server built for that variant. Client tools (`cdb2sql`, `pmux`, `comdb2ar`,
...) should come from a normal build: tests run them without any prefix.

```bash
MEMORY_TOOL_VARIANT=asan-system make -C tests dohsql_pack \
    COMDB2_EXE=/path/to/asan-system-build/db/comdb2
```

With `MEMORY_TOOL_VARIANT` unset nothing changes. When it is set,
`runtestcase` (through `lifecycle.sh`):

1. rejects `DEBUGGER`, validates the variant, and sets `DEBUG_PREFIX` to
   `exec.sh`, the launch prefix the harness already puts in front of server
   starts. `exec.sh` replaces itself with the server (same pid, so `$!` and
   pidfiles are unchanged), applies the variant's settings from `config.sh`,
   and records each process under
   `$TESTDIR/.memory-tools/$TESTCASE/nodes/<host>/invocations/<pid>/`
   (`launch.json`, a copy of stderr, detector logs);
2. runs `--create` through the same prefix, copies the runtime files to every
   cluster node, scales the test timeouts and gives servers longer to exit
   (`exitalarmsec`, unsetup's wait), always asking them to exit cleanly;
3. after unsetup, waits for any recorded server still exiting, collects remote
   records, and scans every process with `check_report.py scan`, writing
   `summary.json` and `summary.txt` next to the records.

A test passes only if both the test and the memory check pass. The memory
status is:

| Status | Meaning |
|---|---|
| `pass` | every recorded server exited cleanly (or was checkpointed before an intended kill) with no finding |
| `fail` | any finding (leak, use-after-free, race, UB, Valgrind error, sanitizer crash) |
| `incomplete` | coverage cannot be proven: no clean exit and no checkpoint, missing node or artifacts, a server started without the launcher, a failed checkpoint |
| `not-run` | no server ran (e.g. `COMDB2_UNITTEST` tests, or setup failed first) |

`fail` and `incomplete` make the test fail (`!<test>: failed memory check`
in `test.log`, nonzero exit), even if the test itself passed.

Server side (`db/memdebug.c`), active only in these runs:

- a server reaching the end of a clean exit writes `clean-exit` into its
  invocation directory; a clean sanitizer run writes no report otherwise;
- `exec procedure sys.cmd.send("memdebug leakcheck")` runs LeakSanitizer's
  recoverable check (reports leaks, keeps running) and records a checkpoint;
- a server started in such a run without `exec.sh` (a direct `$COMDB2_EXE` in a
  test) leaves an `uncovered/` record, which makes the result incomplete.

Tests that deliberately SIGKILL a server call
`${TESTSROOTDIR}/memory_tools/before_db_kill.sh <dbname> [host]` right before
the kill (already done in `tools/cluster_utils.sh` `kill_by_pidfile`). The kill
itself is unchanged. Tests that start servers themselves should use
`${DEBUG_PREFIX} ${COMDB2_EXE} ...`, with the prefix directly in front of the
binary (inside the command string when started over ssh).

`MEMORY_TOOL_INJECT_LEAK=1` (testing the harness only, needs a `COMDB2_TEST`
build) makes the server leak one block right after setup; the test must then
fail its memory check.

## Interface for other tooling

Use these files instead of repeating their contents:

| File | Owns |
|---|---|
| `cmake/Comdb2Sanitizers.cmake` | `COMDB2_SANITIZER` (`none`, `address-undefined`, `thread`) → compiler and linker flags |
| `tests/memory_tools/config.sh` | variant → CMake options, runtime environment, command prefix, expected probes |
| `tests/memory_tools/check_report.py` | detector output → finding categories and JSON |
| `tests/memory_tools/selftest.sh` | the end-to-end proof for one variant |
| `tests/memory_tools/exec.sh`, `lifecycle.sh` | running database tests under a variant |

To run any program under a variant:

```bash
source tests/memory_tools/config.sh
memory_tool_configure asan-system /abs/path/for/detector/logs || exit 1
cmake -S . -B build "${MEMORY_TOOL_CMAKE_ARGS[@]}"
env "${MEMORY_TOOL_ENV[@]}" "${MEMORY_TOOL_COMMAND_PREFIX[@]}" ./program
```

The detector log directory must be an absolute path without whitespace or
`:`, because sanitizer option strings are split on those characters.

Finding categories reported by `check_report.py`: `heap-use-after-free`,
`leak`, `undefined-behavior`, `data-race`, `invalid-read`, `invalid-write`,
`uninitialized-read`, `unknown-error`. Any detector log that is empty or not
recognised is a failure, and so is any finding other than the expected one.

## Known limits

- **`asan-native` is not a leak lane.** Comdb2's own allocations come from
  dlmalloc mspaces, which LSan cannot see, so `detect_leaks=0`. The mspace
  allocator also has no ASan poisoning, so ASan cannot see use-after-free
  inside mspaces either; this lane covers memory outside them (stack,
  globals, plain `malloc`, third-party libraries). Use `asan-system` and
  `valgrind-system` for Comdb2 leaks.
- **UBSan is built recoverable** (`-fsanitize-recover=undefined`): whether a
  finding stops the program is the runtime `UBSAN_OPTIONS=halt_on_error`
  setting. The probe configuration halts on the first finding; whole-server
  runs can report every finding and continue (berkdb triggers several, e.g.
  misaligned log writes, while creating a database).
- **UBSan writes to stderr.** With GCC's combined ASan+UBSan runtime, UBSan
  ignores `log_path` and prints findings to stderr. `run_probe.sh` captures and
  scans stderr; any harness that runs the server must do the same.
- **GCC only in CI.** The CMake option also accepts Clang, but Clang links
  sanitizer runtimes statically by default, so the `ldd` part of the linkage
  check expects GCC.
- **Some warnings are not fatal in sanitizer builds**, each scoped to the
  mode where it was shown to break the build: with ASan, `-Warray-bounds`
  (GCC 14/15 false positive in berkdb's `P_ENTRY`) and `-Wformat-truncation` /
  `-Wformat-overflow` (NULL paths that recoverable UBSan checks leave behind);
  with TSan, `-Wformat-truncation` (a real truncation in `db/fdb_fend.c`, to be
  fixed separately). They are still reported. Everything else stays fatal; default builds are
  unchanged. See `cmake/Comdb2Sanitizers.cmake`.
- **No suppressions.** None are shipped; findings in Comdb2 code are not to be
  baselined here.
- **Killed servers.** SIGKILL skips the exit-time leak check and Valgrind's
  final leak summary. `asan-system` covers intended kills with the pre-kill
  checkpoint; Valgrind keeps the errors found before the kill but has no leak
  summary for that process. Recovery that `comdb2ar`/`copycomdb2` run with
  `-x $COMDB2_EXE` cannot be prefixed, and a server a test starts directly
  over ssh without the prefix is not detected (no environment reaches it).
- **TSan and ASLR.** Some kernels with `vm.mmap_rnd_bits` above 28 make older
  TSan runtimes fail at startup with `FATAL: ThreadSanitizer: unexpected memory
  mapping`. The self-test records the value in `tool-versions.txt` and reports
  this as an unknown error rather than a pass.
