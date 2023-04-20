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
[CREATE|ALTER TABLE <tablename> <table configuration> PARTITIONED BY TIME PERIOD ['daily'|'weekly'|'monthly'|'yearly'|'manual'] RETENTION n START 'datetime string|integer'`
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


## Main Limitations

The main advantage of partitioning is scalability.  This is achieved by localizing access for each operation to a limited number of shards (ideally the newest one). 
Therefore, while the partitioned table behaves like any other table, there is no
unique key constrain across the whole partition.
Implementing such a constrain would require that every write operation access and lock pages in each shard during constraint validation, effectively denying the main advantage of partitioning, which is localized access on small(er) btrees.
Because there are no unique key constraints across the partition, foreign constraints also can only be implemented one way: a regular table cannot depend on a partitioned table, but the other way around is valid. The UPSERT is not supported for the same reason.


## Legacy implementation

Release 8.0 time and manual partitioning is a redesign of previous partitioning scheme.  It eliminates most deficiencies:
- partitioning is a feature of a table, rather than another abstraction on top of tables
- simplified DDL syntax, consistent with regular table DDL
- no collision between table name and partition name; no need to rename an existing table before partitioning
- simplified, atomic rollout as opposed to multiple stage legacy rollouts
- live reconfiguring of a partition, as opposed to limited retention only changes

For obsolete partition information, see:
[Legacy time partition](timepart.html)

