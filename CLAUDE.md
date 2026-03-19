# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Comdb2 is a clustered RDBMS built on Optimistic Concurrency Control (OCC) techniques. It provides multiple isolation levels including Snapshot and Serializable Isolation. The system uses a master/replicant architecture where read/write transactions can run on any node.

Key architectural paper: [VLDB 2016 paper](http://www.vldb.org/pvldb/vol9/p1377-scotti.pdf)

## Build System

### Building from Source

```bash
# Standard build
mkdir build && cd build
cmake .. && make -j$(nproc)

# Build with testing features enabled
cmake -DCOMDB2_TEST=1 .. && make -j$(nproc)

# Build test tools (required before running tests)
make -j$(nproc) test-tools

# Install (default: /opt/bb)
sudo make install
```

### Build Options

- `CMAKE_BUILD_TYPE`: Debug, RelWithDebInfo (default), Release
- `COMDB2_TEST=1`: Enable testing features and test instrumentation
- `COMDB2_PER_THREAD_MALLOC=OFF`: Disable custom allocator (use for Valgrind)
- `GENERATE_COVERAGE=ON`: Enable gcov/lcov coverage
- `CMAKE_INSTALL_PREFIX`: Installation prefix (default: /opt/bb)

### Common Build Targets

- `make comdb2`: Build the main database server
- `make cdb2api`: Build the client API library
- `make cdb2sql`: Build the SQL CLI tool
- `make test-tools`: Build utilities needed for running tests

## Testing

### Running Tests

Tests are located in `tests/` directory. Each test resides in a `*.test/` directory.

```bash
# From build directory, build test tools first
make -j$(nproc) test-tools

# Run a specific test
cd ../tests
make testname  # e.g., make basic or make cdb2api

# Run all tests (stops on first failure)
make

# Run all tests in parallel (continues on failure)
make -kj5  # -k continues, -j5 runs 5 tests in parallel

# Run with custom test directory
make testname TESTDIR=/tmp/mytestdir

# Run with custom build directory
make testname BUILDDIR=/path/to/build

# Keep test database after completion
make testname CLEANUPDBDIR=0
```

### Clustered Testing

```bash
# Run test on a cluster of machines
make testname CLUSTER="m1 m2 m3"

# Run parallel tests with subset of cluster nodes
make -j4 -k CLUSTER='node1 node2 node3 node4 node5' NUMNODESTOUSE=3
```

### Test Debugging

```bash
# Run under a debugger
make testname DEBUGGER=gdb      # or valgrind, memcheck, callgrind, perf

# Setup test environment without running
cd testname.test && make setup

# View test logs
ls test_*/logs/  # Contains dbname.testcase, dbname.db, etc.
```

### Important Test Environment Variables

- `TESTDIR`: Test database directory
- `DBNAME`: Database name for the test
- `CDB2_OPTIONS`: Options to pass to cdb2sql
- `CDB2_CONFIG`: Path to cdb2api config file
- `CLUSTER`: Machines for clustered test

## Architecture

### High-Level Components

**Client-Server Architecture:**
- **cdb2api/** - Client library implementing the wire protocol
- **protobuf/** - Protocol buffer definitions for client-server communication (sqlquery.proto, sqlresponse.proto)
- **net/** - Network layer handling cluster communication
- **tools/pmux/** - Port multiplexer for service discovery

**Database Core:**
- **db/** - Main database logic, types layer, and overall coordination
- **sqlite/** - SQLite VM SQL engine (Comdb2's SQL layer)
- **bdb/** - Table layer providing ACID properties
- **berkdb/** - Berkeley DB btree layer for physical storage
- **schemachange/** - Schema evolution (CREATE/ALTER/DROP TABLE)

**Data and Storage:**
- **csc2/** - CSC2 schema definition processing
- **comdb2rle/** - Run-length encoding compression
- **crc32c/** - Checksum implementation
- **datetime/** - Datetime handling
- **dfp/** - Decimal floating point support
- **mem/** - Memory accounting subsystem

**Plugin System:**
- **plugins/newsql/** - New SQL protocol plugin
- **plugins/remsql/** - Remote SQL plugin
- **plugins/simpleauth/** - Simple authentication plugin
- **plugins/sockbplog/** - Socket backpressure logging
- Additional plugins in plugins/

**Tools:**
- **tools/cdb2sql/** - SQL command-line interface
- **tools/pmux/** - Port multiplexer
- **tools/comdb2ar/** - Archive/backup utilities

### Transaction Model

Comdb2 uses optimistic concurrency control:
1. SQL statements run on replicants with short-duration read locks only
2. Replicants forward write operations to the master
3. Master applies operations under write locks at commit time
4. Master replicates synchronously to all nodes
5. Conflicts trigger automatic retry (unless VERIFYRETRY is OFF)

Records are identified by genids (generated IDs) which change on every update, enabling conflict detection.

### Isolation Levels

- **Default**: Fast but doesn't see own updates mid-transaction
- **READ COMMITTED**: Sees own updates via shadow tables
- **SNAPSHOT**: Time-frozen view, requires `enable_snapshot_isolation` in lrl
- **SERIALIZABLE**: Strongest guarantees, requires `enable_serial_isolation` in lrl
- **LINEARIZABLE**: External consistency guarantees

## Code Formatting

The project uses clang-format for C/C++ code:

```bash
# Check formatting (from repository root)
./ci/scripts/format --verbose --dry-run --compare-base origin/main

# Format changed files
./ci/scripts/format
```

## Common Development Workflows

### Adding a New Test

1. Create `tests/testname.test/` directory
2. Create minimal `Makefile`:
   ```makefile
   include $(TESTSROOTDIR)/testcase.mk
   ```
3. Create `runit` script that exits 0 on success, 1 on failure
4. The script receives database name as first argument

### Working with Protocol Buffers

Protocol buffer files are in `protobuf/`. After modifying `.proto` files:

```bash
# Regenerate C code (protoc-c must be installed)
cd protobuf
protoc-c sqlquery.proto --c_out=.
```

### Working with CSC2 Schema Files

CSC2 (Comdb2 Schema Configuration) files define table schemas. They're processed by the `csc2/` component and stored in `csc2files/`. Schema changes go through the `schemachange/` subsystem.

### Running a Local Database

```bash
# Start pmux (port multiplexer)
pmux -n

# Create a database
comdb2 --create --dir ~/db testdb

# Start the database
comdb2 --lrl ~/db/testdb.lrl testdb

# Connect and run SQL
cdb2sql testdb local 'SELECT 1'
```

## Important Notes

- Database uses `/opt/bb/bin` by default for binaries
- Default database root is `/opt/bb/var/cdb2/`
- Default log directory is `/opt/bb/var/log/cdb2/`
- Configuration files go in `/opt/bb/etc/cdb2/config/comdb2.d/`
- The build system requires protobuf-c, libevent, lz4, openssl, readline, libunwind, and other dependencies
- Pmux runs on port 5105 by default (use `PMUXPORT=xxxx` to override)
- Test timeouts: 5 minutes per test (override with `TEST_TIMEOUT`), 1 minute for setup (override with `SETUP_TIMEOUT`)
