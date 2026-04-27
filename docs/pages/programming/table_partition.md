---
title: Table partitioning
keywords: code
sidebar: mydoc_sidebar
permalink: table_partition.html
---


## Partition design and DDL

Starting with release 8.0, partitioning is a property of a table, similar with the other table properties, for example schema.i
A partitioned table is actually a set of tables, called shards, which are accessed under the same name (the partition table name), as if the table would be not partitioned.  The SQL syntax for using a partitioned table is identical with the syntax for regular tables.
The table DDL statements have been expended to allow a table to be partitioned.   The partitioning is, as the other schema changes, a live process.  A partitioned table can be collapsed to a single table by removing the partition.  This transformation is again done live without any data loss.


Creating a new partitioned table:

```
CREATE TABLE <partition table name> ... PARTITIONED BY <partition configuration>
```

Partitioning an existing table:

```
ALTER TABLE <standalone table name> ... PARTITIONED BY <partition configuration>
```

Removing a partition table and its shards:

```
DROP TABLE <partition table name>
```

## Partition configurations


Currently we support two types of partitioning, time-based rollout partitioning and manual rollout partitioning.  
Both configurations are methods for implementing data retention.


## Time-based rollout partitions

Time partitioning is a convenient way to add a retention policy to persistent data. Instead of purging old rows based on a timestamp, which takes time, resources, and planning to avoid impacting regular database activity, the data is partitioned in multiple tables, a.k.a shards. Client uses the partition name to read and write all the shards as they would access a regular table.
The partition configuration requires:

```
NAME = a name under which rows will be accessed, in the same way a regular table name is used
START = a datetime string identifying the absolute time at which the first rollout will happen
PERIODICITY = 'daily'|'weekly'|'monthly'|'yearly'
RETENTION = the number of shards the partition will contain
```

Syntax:

```
#create/alter
CREATE <tablename> <table configuration> PARTITIONED BY TIME PERIOD ['daily'|'weekly'|'monthly'|'yearly'|'manual'] RETENTION n START 'datetime string|integer'`
ALTER TABLE <tablename> <table configuration> PARTITIONED BY TIME PERIOD ['daily'|'weekly'|'monthly'|'yearly'|'manual'] RETENTION n START 'datetime string|integer' RETROACTIVELY`
#removing a partition
DROP TABLE <tablename>
```

Examples:

Creating a partitioned table containing 7 days of data, with daily rollout.

```
#creating a time partiton
CREATE TABLE t(a int) PARTITIONED BY TIME PERIOD 'daily' RETENTION 7 start '2023-04-01T UTC'
#adding partitioning to an existing table, while adding a column 'b'
ALTER TABLE t ADD COLUMN b int, PARTITIONED BY TIME PERIOD 'daily' RETENTION 7 start '2023-04-01T UTC'
#collapsing a partition to a standalone table
ALTER TABLE t PARTITIONED BY NONE
#dropping a partitioned table and its shards
DROP TABLE t
```

The server will schedule a rollout event at the specified `START` time once the partition is created.  At each rollout, the oldest shard is truncated and becomes the newest shard.  All new insert data will go to this shard until a new rollout happens.
Reading from a time partition will return rows from every single shard. 
Updating a row will preserve the shard location of that row.

If `RETROACTIVELY` option is used when partitioning an existing table, the row genid timestamps are used to split existing tables in shards.  The command 
will fail it the database runs with `genid48` tunable enabled.

The `START` should be the time of the NEXT rollout. The rows older than `START` - `RETENTION` x `PERIOD` are stored in the oldest shard, which would be
the shard that will be truncated at the `START` timestamp, i.e. shard 1.

If there are rows newer than `START`, they are stored in the current shard, which is the shard with that will live longest before being rollout, i.e. shard 0.
All the rest of the rows are stored such that their genid timestamps are in between `[LOW , HIGH)` limits of their hosting shards.

The tunable `partition_retroactively_start` can be used to reject any partitioning that has a `START` rollout time ealier than `now() + cast(partition_retroactively_start as hours)` hours.
It defaults to 24 hours.  This is to prevent cases when `START` would be misinterpreted as the oldest rollout time, instead of next rolllout, 
which would put all the rows in current shard and fail to partition the data.

A retroactively time partition operation performs similarly with a table rebuild.


## Manual rollout partitions


Manual rollout partitions are similar with time-based partitions, except that rollout is not based on time.  Rather, the execution of specific `PUT COUNTER <partition name> INCREMENT` statements will trigger a rollout.
Creating such a partition allows clients to control the rollout using a logical clock.  This way the rollout can be done based on table size, contention, or other ad-hoc rules.

Syntax:

```
#create a manual rollout partition
CREATE TABLE t(a int) PARTITIONED BY MANUAL RETENTION 7
#partitioning an existing table
ALTER TABLE t PARTITIONED BY MANUAL RETENTION 7
#performing a rollout
PUT COUNTER t INCREMENT
```



## Granularity details

It is worth mentioning that the retention precision is affected by granularity. It is always between `PERIODICITY` x (`RETENTION`-1) and `PERIODICITY` X `RETENTION`. For example, specifying a periodicity `weekly` and retention 4 will result in having data corresponding from 3 weeks to 4 weeks of activity. Every week the shard that is 4 weeks old is truncated, and all new inserted data goes into it. The amount of data immediately before the rollout is 4 weeks; after rollout is 3 weeks.

If the client would choose periodicity `daily`, and retention 31, at all time the partition contains between 30 and 31 days worth of data. Every day a rollout occurs in this case. There is a slight overhead of having 31 shards instead of 4 in this case. Alternatively, a client might choose retention to be 5 weeks, in which case there will always be at least 4 weeks worth of data, and no more than 5 weeks.


## Main advantages and limitations

- Partitioning improves scalability by localizing access for each operation to a limited number of shards (ideally the newest one).
- This keeps B-trees smaller and ensures faster, more efficient operations.
- Partitioned tables behave like regular tables but lack global uniqueness.
- Enforcing unique keys would require every write to lock and check all shards, removing the benefits of localized access on smaller B-trees.
- Foreign key constraints are one-way only: a regular table cannot reference a partitioned table.  A partitioned table can reference a regular table.
- UPSERT not supported because there are no unique constraints.


## Legacy implementation

Release 8.0 time and manual partitioning is a redesign of previous partitioning scheme.  It eliminates most deficiencies:
- partitioning is a feature of a table, rather than another abstraction on top of tables
- simplified DDL syntax, consistent with regular table DDL
- no collision between table name and partition name; no need to rename an existing table before partitioning
- simplified, atomic rollout as opposed to multiple stage legacy rollouts
- live reconfiguring of a partition, as opposed to limited retention only changes

For obsolete partition information, see:
[Legacy time partition](timepart.html)

