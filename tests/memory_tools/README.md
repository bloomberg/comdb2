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

## Interface for other tooling

Use these files instead of repeating their contents:

| File | Owns |
|---|---|
| `cmake/Comdb2Sanitizers.cmake` | `COMDB2_SANITIZER` (`none`, `address-undefined`, `thread`) → compiler and linker flags |
| `tests/memory_tools/config.sh` | variant → CMake options, runtime environment, command prefix, expected probes |
| `tests/memory_tools/check_report.py` | detector output → finding categories and JSON |
| `tests/memory_tools/selftest.sh` | the end-to-end proof for one variant |

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
- **TSan and ASLR.** Some kernels with `vm.mmap_rnd_bits` above 28 make older
  TSan runtimes fail at startup with `FATAL: ThreadSanitizer: unexpected memory
  mapping`. The self-test records the value in `tool-versions.txt` and reports
  this as an unknown error rather than a pass.
