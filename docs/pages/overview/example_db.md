---
title: Your first Comdb2 database
keywords:
tags:
sidebar: mydoc_sidebar
permalink: example_db.html
---

## Creating a database

### The short version

```
# Create
$ comdb2 --create testdb  &
...
# Bring up
$ comdb2 testdb &
...
# Run queries
$ cdb2sql testdb local 'select 1+1 as "math"'
(math=2)
```

### The slightly longer version

The ```comdb2``` program installed (by default) into /opt/bb/bin is the database server.  It's used to both create a database and to run a previously created database.  The only value you need to specify for creating a database are the database name.  You can also optionally specify the path where you want it to live.  The default is `/opt/bb/var/cdb2`.  A database name should be unique per machine.  You can create several tiers of your database to have separate instances for dev, beta, production, etc. The [Setting up clusters](cluster.html) section will delve into more details. For now, let's just create a database to experiment with.

Let's call our database 'testdb'. Creating a new database is as simple as:

```
comdb2 --create testdb
```

After some lines of trace, you'll see

```
Created database testdb.
goodbye
```
In ```/opt/bb/var/cdb2/testdb```, you'll see several files.  One of these, testdb.lrl is your configuration file.  Comdb2 will create this file for you with recommended
best practice tunables.  The [Configuration files](config_files.html) section goes into lots of details on tunables.  The defaults are fine for now.  The initial file
looks like this:

```
name    testdb
dir     /opt/bb/var/cdb2/testdb
```

## Bringing up the database

You can now bring up the database with ```comdb2 testdb```.  It will print some messages, and then print

```
I AM READY.
```

We're now in business.

## Running queries with cdb2sql

cdb2sql is a simple command line tool to query databases.  It takes a couple of arguments: 

1. database name
2. (optional) database location
3. query to run

If you're running the database on the same machine as cdb2sql, you can omit the location, or specify 'local', ie: `cdb2sql testdb local "$query"`.  There are other schemes: you can specify
a hostname and port, a list of hosts for a cluster, or a name that maps to a list of hosts.  More details later in the [Client setup](clients.html) section.

The query is any valid SQL
query, see the [SQL language](sql.html) section.  You can also specify '-' to go into interactive mode.  When run from the terminal, this gives you
a readline interface.  When run from a script, this reads from stdin, and executes a query per line.

Let's create a simple table, populate it with some data, and see if SQL works!

Comdb2 uses a pretty standard-ish SQL DML dialect.  It uses [SQLite](http://sqlite.org) as the parser/query planner, and will support most things SQLite does (and some others).  The DDL dialect is **not** SQL.  [SQL language](sql.html) section has lots of details.  A table definition looks similar to a C struct.  Here's a short example:

```sh
# create a simple table with a number and a unique index on that number
$ cdb2sql testdb local '
  create table numbers {
      schema {
          int number
      }

      keys {
          "num" = number
      }
  }
'

# can we insert a number?
$ cdb2sql testdb local 'insert into numbers(number) values(0)'
[insert into numbers values(0)] rc 0

# can we read it back?
$ cdb2sql testdb local 'select number from numbers'
(number=0)

# can we insert a duplicate?
$ cdb2sql testdb local 'insert into numbers (number)values(0)'
[insert into numbers(number) values(0)] failed with rc 299 OP #3 BLOCK2_SEQV2(824): add key constraint duplicate key 'NUM' on table 'numbers' index 0
# no, we can't since we have a unique index

# insert a bunch of numbers
$ for num in $(seq 1 1000); do
   echo "insert into numbers(number) values($num)"
done | cdb2sql testdb local - >/dev/null
# we don't want to see success messages for every row, but will still see errors

$ cdb2sql testdb local 'select count(*) as count from numbers'
(count=1001)
```

## Simple cluster demo

Comdb2's raison d'etre is ease of setup of clusters.  Setting up a cluster is 4 easy steps:

1. Create a database
1. Add a list of machines to the lrl file
1. Copy database to machines
1. Start database on all machines

### Create a database

As before:  ```comdb2 --create testdb```

### Add a list of machines

Lets use machines m1, m2, m3 for our cluster.  The lrl file is ```/opt/bb/var/cdb2/testdb/testdb.lrl``` as before.  It needs one more line:
```cluster nodes m1 m2 m3```.

### Copy database to machines

The comdb2 distribution includes a script called ```copycomdb2```.  Its job is copying and backing up databases.  We can distribute the database
we just created with:

```
$ copycomdb2 /opt/bb/var/cdb2/testdb/testdb.lrl m1:
$ copycomdb2 /opt/bb/var/cdb2/testdb/testdb.lrl m2:
$ copycomdb2 /opt/bb/var/cdb2/testdb/testdb.lrl m3:
```

If no path is provided in the destination, the database will be placed in the same directory on the destination as in the source.

See [Setting up clusters](cluster.html) for more details.  The short version is that the target machines will need to have the ssh keys for the current user on the source machine in their authorized_keys file.

### Start database on all machines

```
m1$ comdb2 testdb &
...
m2$ comdb2 testdb &
...
m3$ comdb2 testdb &
...
```

If you're monitoring the output, you'll see the usual startup trace followed by master election.  One of the nodes will be elected master.  The rest are replicants.  Replicants will handle all user requests.  The master will handle all the writes generated by those requests and distribute them to replicants.

### Querying the cluster

You can pass the list of hosts in the cluster to Comdb2 calls directly.  This is the simplest way to address the cluster, though it's not recommended to build applications this way directly.

```
$ cdb2sql testdb @m1,m2,m3 'select 1+1 as "math"'
(math=2)
```

For a fun experiment, you can try to create a table, add a record, and query it on each machine in the cluster to see the data.

## Running as a service

### Using supervisord

If you installed Comdb2 as a package, or followed the install directions printed by `make install`, you can use the bundled supervisord instance to manage Comdb2.
It comes with a `comdb2admin` script that can create databases, start/stop them, and destroy them.

See [Setting up hosted services](hosted.html) for more information.

### Using systemd

`comdb2 testdb --create` will produce a `testdb.service` file in the database directory.  If you're using systemd to manage databases, you can set up the db to run as a service.  To follow our earlier example,

```
sudo cp /opt/bb/var/cdb2/testdb/testdb.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable testdb
sudo systemctl start testdb
```
