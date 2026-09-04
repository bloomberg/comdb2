# cluster — local Comdb2 dev cluster in Docker

`cluster` spins up a multi-node Comdb2 cluster in Docker for development and
running the test suite. Comdb2 is built **from your source tree inside the
container**, so no host toolchain is needed and it works the same on Linux and
macOS (including Apple Silicon).

## Prerequisites

- Docker installed and running (`docker info` must succeed).
- This repository checked out (the script auto-detects the repo root).

## Quick start

```sh
# run from the repo root, so the "Comdb2 source root" default is detected correctly
cd /path/to/comdb2
c=contrib/dev-util/cluster

$c init          # answer the prompts; builds the comdb2:dev image (first run is slow)
$c run           # start the containers (m0 = client, m1..mN = cluster nodes)
$c setup [db]    # create a db and copy it cluster-wide; it becomes the default
$c startdb [db]  # start it on every node (omit [db] for the default)
```

`m0` is the **client** node — run `cdb2sql` from it and let it route to the
cluster with `default`:

```sh
./cluster c 0                        # shell on the client (m0)
cdb2sql <db> default "select 1"      # query the cluster
```

Or as a one-liner from your host:

```sh
docker exec m0 /opt/bb/bin/cdb2sql <db> default "select 1"
```

## Rebuilding after a code change

Nothing is copied between host and cluster: every container bind-mounts your
source tree, and `/opt/bb/bin/*` are symlinks into `build-docker/`. One build
reaches all nodes, and no image rebuild is needed:

```sh
./cluster update   # rebuild Comdb2 from your latest source
```

`update` is incremental (only rebuilds what changed) and takes seconds. A server
that is already running keeps executing the binary it started with, so restart
it to pick the build up:

```sh
./cluster stopdb && ./cluster startdb
```

Re-run `init` only when you need a clean image from scratch.

## Running tests

```sh
./cluster test                  # run the whole suite
./cluster test comdb2_files     # run one test (a cluster test is a good smoke check)
```

`test` recreates the cluster from a clean slate first (it runs `clean` then
`run`, which rebuilds Comdb2), so it will tear down any cluster you already have
up. After it finishes it
prints where the logs are — on the `m0` container under
`/opt/bb/tmp/testdir/logs/` (`<test><id>.testcase` = test output,
`<test><id>.<node>.db` = per-node server logs) — and lists the actual files from
the run. Logs are kept until the next `test` run.

## Common commands

| Command | What it does |
|---|---|
| `init` | Wipe settings and build the image from scratch |
| `run` | Start the cluster containers |
| `setup [db]` | Create a db and copy it cluster-wide |
| `startdb [db]` | Start the database on every node |
| `stopdb [db]` | Stop the databases (or just one), leaving the containers up |
| `rmdb <db>` | Delete a database cluster-wide (stops it first) |
| `update` | Rebuild Comdb2 from source and sync to all nodes |
| `test [name]` | Rebuild, then run the test suite (or one test) |
| `status` | Report image / container / pmux / db health |
| `stop` / `start` | Stop or start the containers |
| `c \| clnt [n]` | Shell on node `mN` (`n` defaults to `0`, the client) |
| `tmux` | One window: local shell over client `m0` on the left, nodes stacked right |
| `gdb [db]` / `vg [db]` | Start the db under gdb / valgrind on every node |
| `set <key> <val>` | Change one setting (`info` lists them) without re-running `init` |
| `clean` | Kill and remove all containers |

Run `./cluster` with no arguments for the full command list.

`tmux` leaves the panes unsynchronized so you can drive `cdb2sql` from `m0`
while watching the nodes; toggle broadcast with `prefix + : setw
synchronize-panes`. `gdb`/`vg` instead open a synchronized node-only window (no
client pane — it would be told to start a server too), and stop the database
first if it's running: pmux refuses a second server on a node, so the debugger
would come up on a server that died at startup.

`gdb` refuses to run when the containers are a different architecture than the
host (an amd64 image on Apple Silicon, say): ptrace can't read the registers of
an emulated process, so gdb only reports `Couldn't get registers`. `vg` still
works there — valgrind runs the program on its own synthetic CPU. For live
debugging on such a host, use a native Linux box, or build and run Comdb2
natively (`cmake -B build-mac && ninja -C build-mac`) and use `lldb`.

## Notes

- The first `init` compiles all of Comdb2 in the image and takes a while;
  later `update`/`test` runs are incremental and only rebuild what changed.
- Re-run `init` any time to rebuild the image from a clean slate.
- The cluster builds into `build-docker/`, separate from a host `build/`, so a
  host build and an in-container build don't invalidate each other's CMake cache
  (they see the source at different paths). Both live in your source tree.
