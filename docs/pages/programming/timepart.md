---
title: Time-based table partitioning
keywords: code
sidebar: mydoc_sidebar
permalink: timepart.html
---

## Time-based partitions

Time partitioning is a convenient way to add a retention policy to persistent data. Instead of purging old rows based on a timestamp, which takes time, resources, and planning to avoid impacting regular database activity, the data is partitioned in multiple tables, a.k.a shards. Client uses the partition name to read and write all the shards as they would access a regular table. The partition configuration requires:

```
NAME = a name under which rows will be accessed, in the same way a regular table name is used
SHARD0 = an existing table to be used as first shard
START = a datetime string identifying the absolute time at which the first rollout will happen
PERIODICITY = 'daily'|'weekly'|'yearly'
RETENTION = the maximum number of shards the partition will contain
```

The server will schedule a rollout event at the specified `START` time once the partition is created.  The rollout involves creating a new shard, schedule the deletion of the oldest shard if there are more than `RETENTION` shards, and update the partition info.  Updating the partition info triggers a schema update for existing sqlite engines. 
Reading from a time partition will return rows from every single shard.  Inserting in a partition will add the rows to the newest shard.  Updating a row will preserve the shard location of that row.

## Logical time partitions

Creating a partition with `manual` period allows client to control the rollout using a logical clock.  This way the rollout can be done based on table size, contention, or ad-hoc rules.  The logical clock is created using `PUT COUNTER <name>`, where `name` is the same as the partition name.  The `START` value for a manual rollout is the value of the counter at which the rollout occurs.

## Sql syntax examples

Creating a partition syntax is:

`CREATE TIME PARTITION ON shard0 as name PERIOD ['daily'|'weekly'|'yearly'|'manual'] RETENTION n START 'datetime string|integer'`

Dropping an existing partition syntax is:

`DROP TIME PARTITION name`

Reading and writing a time partition (no different from regular tables):

`SELECT * FROM name`; `INSERT INTO name VALUES (...)`; and so on.


## Granularity details

It is worth mentioning that the retention precision is affected by granularity. It is always between `PERIODICITY` x (`RETENTION`-1) and `PERIODICITY` X `RETENTION`. For example, specifying a periodicity `weekly` and retention 4 will result in having data corresponding from 3 weeks to 4 weeks of activity. Every week a new shard is added to the partition, and all new inserted data goes into it. The shard that is 4 weeks old is deleted through a fast table drop operation. The amount of data immediately before the rollout is 4 weeks; after rollout is 3 weeks.

If the client would choose periodicity `daily`, and retention 31, at all time the partition contains between 30 and 31 days worth of data. Every day a rollout occurs in this case. There is a slight overhead of having 31 shards instead of 4 in this case. Alternatively, a client might choose retention to be 5 weeks, in which case there will always be at least 4 weeks worth of data, and no more than 5 weeks.


## Current limitations

The name space for tables and partitions is the same.  Creating a partition name cannnot reuse an existing table name.  This is inconvenient and it will be addressed by future efforts.  
Enforcing unique constraints is not possible accross shards of a time partition.  An option to allow this at the expense of commit performance will be goal of a future project.


## Rollout implementation details

The partition rollout has three phases:

I) creating a new shard (basically generates next table name, creates the table, schedule phase II)

II) update metadata to include the new shard and (usually) evict oldest shard

III) drop the evicted shard

In addition, there is a recovery phase (IV) running on any master swing.  The recovery is responsible for handling master crashes in the middle of rollouts, and scheduling the next rollouts on the new master.

Rollout phase I details:

I.1) generates the name of the next shard 

I.2) schema change to create a new shard with the schema and settings identical to existing shards

I.3) schedule phase II

Note: phase I preceeds the rollout time by a safe time window, such that by the time the partitioninig info needs to be updated, the table is available

Rollout phase II details:

II.1) update in memory shard bookkeeping, creating a new shard and identifying the separation time T between new shard and the latest one 

II.2) publish the in memory new bookkeeping, which basically writes this persistently as a json objec in llmeta,  which gets replicated (upon remote sqlite engines will refresh their partition information)

II.3) update the local views version that would trigger local sqlite, if any, to update their partition information

II.4) schedule phase III with a small delay (if we have to evict the oldest shard)

II.5) schedule next phase I for the next rollout 


Rollout phase III details:

III.1) do a schema change `drop` table for the provided shard


Recovery phase:

IV.1) deserialize the views from the llmeta saved json object

IV.2) for each view, run individual view recovery

IV.2.1) check if the shard exists physically 

IV.2.2) check the next rollout event; if a shard needs to be evicted, schedule a phase III; if a shard was already created, schedule a phase II, otherwise schedule phase I 


