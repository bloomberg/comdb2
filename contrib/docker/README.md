# Comdb2 on Docker

This directory contains an example [Docker Compose](https://docs.docker.com/compose/)
setup that builds Comdb2 from the source tree and stands up a 3-node cluster
plus a client container, with a single command and no manual steps.

## Prerequisites

- [Docker Engine](https://docs.docker.com/engine/install/) with the
  [Compose](https://docs.docker.com/compose/install/) plugin (`docker compose`).

## Quick start

From this directory (`contrib/docker/`):

```sh
docker compose up -d
```

The first run compiles Comdb2 from source, so it takes a few minutes. Subsequent
runs reuse the built image and start in seconds.

Once it's up, open a shell on the client container and query the database:

```sh
docker compose exec -it dev bash
# inside the container:
cdb2sql testdb default "select 1"
cdb2sql testdb default "create table t(i int)"
cdb2sql testdb default "insert into t values(42)"
cdb2sql testdb default "select * from t"
```

Or run a one-off query without an interactive shell:

```sh
docker compose exec dev cdb2sql testdb default "select * from t"
```

To shut the cluster down (keeping the data):

```sh
docker compose down
```

To shut down and wipe all database state so the next `up` starts fresh:

```sh
docker compose down -v
```

### What gets created

Running `docker compose up` starts the following containers:

| Container | Role |
| --- | --- |
| `init` | One-shot job that creates `testdb` and seeds every node's data directory, then exits. |
| `node1` ... `node3` | The Comdb2 cluster. Each runs `pmux` and the database from its own volume. |
| `dev` | Client / "application" container. Stays running so you can `exec` into it and run `cdb2sql`. In real use this would be replaced by your application. |

The nodes wait for `init` to finish, so by the time they start they all have an
identical copy of a freshly created `testdb` and a shared `cluster nodes` line in
their lrl. They discover each other over the Compose network (by container
hostname) and elect a master.

Each node's data lives in a named Docker volume (`node1` ... `node3`), so database
state survives `docker compose down`/`up`. Use `docker compose down -v` to remove
the volumes and start over.

### Connecting to the cluster

The `dev` container mounts [`testdb.cfg`](testdb.cfg) at
`/opt/bb/etc/cdb2/config.d/testdb.cfg`, which tells the client where `testdb`
lives:

```
testdb node1 node2 node3
comdb2_config: allow_pmux_route 1
comdb2_config: default_type docker
```

The first line is what actually makes routing work: it lists the hosts for
`testdb`, so the client can connect to any of them and discover the rest of the
cluster (writes are routed to the master). The `default_type` line is only a
convenience — it lets you pass `default` as the tier so you don't have to name
one on the command line. Without it, `cdb2sql testdb default` errors with "no
default type"; its value is otherwise unused here (any non-reserved string works
— `docker` is just a placeholder).

You can also target a specific node directly with the `@` syntax, e.g.
`cdb2sql testdb @node3 "select 1"`.

### Useful commands

```sh
# Follow logs for a node (watch the cluster form / elect a master)
docker compose logs -f node1

# Open a shell on a specific cluster node
docker compose exec -it node1 bash

# Check cluster/container status
docker compose ps
```

## Running a standalone database

The same image can also run one or more **standalone** (non-clustered)
databases in a single container.

First make sure the image is built (Compose builds it, or build it directly):

```sh
docker compose build
# or, from the repository root:
#   docker build -f contrib/docker/Dockerfile.dev -t comdb2-dev:latest .
```

Then run a container, naming the database(s) you want:

```sh
# create and start two standalone databases, db1 and db2
docker run -d --name comdb2 comdb2-dev:latest db1 db2
```

Query them with `cdb2sql` using the `local` tier, which connects to the database
running inside the container:

```sh
docker exec comdb2 cdb2sql db1 local "create table t(i int)"
docker exec comdb2 cdb2sql db1 local "insert into t values(1)"
docker exec comdb2 cdb2sql db1 local "select * from t"
docker exec comdb2 cdb2sql db2 local "select 1"
```

The database files live under `/opt/bb/var/cdb2/<name>` in the container. The
entrypoint restarts existing databases when the container is restarted, so
their data survives a `docker restart comdb2`.

Stop and remove the container when you're done:

```sh
docker rm -f comdb2
```

## Files

| File | Description |
| --- | --- |
| `Dockerfile.dev` | Multi-stage build: compiles Comdb2 from the repo, then produces a slim runtime image (used for every container). |
| `Dockerfile.jdbc.build` | Builder image for the JDBC driver (used by `cdb2jdbc/Makefile`, not by this compose setup). |
| `compose.yaml` | Defines the `init` job, the 3-node cluster, and the `dev` client. |
| `cluster-init.sh` | Creates `testdb` once and copies it into every node's volume. |
| `cluster-entrypoint.sh` | Node entrypoint: starts `pmux` and the database. |
| `standalone-entrypoint.sh` | Default image entrypoint for running a database in a standalone container (Compose overrides it per service). |
| `client-entrypoint.sh` | Keeps the `dev` container alive so you can exec into it. |
| `testdb.cfg` | `cdb2sql` client config pointing at the cluster. |
| `maven-settings.xml` | Maven settings used by the JDBC builder image. |

## Notes

- The image is built from `ubuntu:22.04` for reproducible package versions. The
  build context is the repository root (see `.dockerignore`).
- The containers are granted broad capabilities (`cap_add: ALL`) to keep the
  example simple; tighten this for anything beyond experimentation.
