---
title: System Tables
keywords: code
sidebar: mydoc_sidebar
permalink: system_tables.html
---

## comdb2_tables

This is a table of all the existing tables in the database.

    comdb2_tables(tablename)

* `tablename` - Name of the table.

## comdb2_columns

Describes all the columns for all of the tables in the database.

    comdb2_columns(tablename, columnnname, type, size, sqltype,
    varinlinesize, defaultvalue, dbload, isnullable)

* `tablename` - Name of the table.
* `columnname` - Name of the column.
* `size` - The storage size of the column.
* `sqltype` - The type as recognized by sql.
* `varinlinesize` - The size of the data stored in the column when inlined.
* `defaultvalue` - The default value for this column.
* `dbload` - The default value for this column loaded by the server.
* `isnullable` - `Y` if this column can hold nulls.

## comdb2_keys

Describes all of the keys in the database.

    comdb2_keys(tablename, keyname, keynumber, isunique, isdatacopy,
    isrecnum, condition)

* `tablename` - Name of the table.
* `keyname` - Name of the key.
* `isunique` - `Y` if this key is unique.
* `isdatacopy` - `Y` if the data is inlined with this key.
* `isrecnum` - `Y` if this key has recnums.
* `condition` - Where condition for this index.
* `uniqnulls` - `Y` if this key treats NULL values as unique.

## comdb2_keycomponents

Describe all the components of the keys.

    comdb2_keycomponents(tablename, keyname, columnnumber, columnname,
    isdescending)

* `tablename` - Name of the table.
* `keyname` - Name of the key.
* `columnnumber` - Position of `columnname` in `keyname`.
* `columnname` - Name of a column in `keyname`.
* `isdescending` - `Y` if this column is descending.

## comdb2_constraints

Shows all foreign key constraints on tables in the database.

    comdb2_constraints(tablename, keyname, foreigntablename,
    foreignkeyname, iscascadingdelete, iscascadingupdate)

* `tablename` - Name of the table.
* `keyname` - Name of the key.
* `foreigntablename` - Name of the foreign table.
* `foreignkeyname` - Name of the foreign key.
* `iscascadingdelete` - `Y` if this is a cascading delete.
* `iscascadingupdate` - `Y` if this is a cascading update.

## comdb2_tablesizes

Shows the sizes on disk of the tables.

    comdb2_tablesizes(tablename, bytes)

* `tablename` - Name of the table.
* `bytes` - Size of the table in bytes.

## comdb2_users

Table of users for the database that do or do not have operator access.

    comdb2_users(username, isOP)

* `username` - Name of the user.
* `isOP` - `Y` if `username` has operator access.

## comdb2_tablepermissions

Table of permissions for tables in the database.

    comdb2_tablepermissions(tablename, username, READ, WRITE, OP)

* `tablename` - Name of the table.
* `username` - Name of the user.
* `READ` - `Y` if `username` has read access to `tablename`.
* `WRITE` - `Y` if `username` has write access to `tablename`.
* `OP` - `Y` if `username` can modify `tablename` schema.

## comdb2_triggers

Table of triggers in the database.

    comdb2_triggers(name, type, tbl_name, event, col)

* `name` - Name of the trigger.
* `type` - Type of the trigger.
* `tbl_name` - Name of the table.
* `event` - Event to trigger on.
* `col` - Column to trigger on.

## comdb2_keywords

Describes all the keywords used in the database. A reserved keyword needs to be
quoted when used as an identifier.

    comdb2_keywords(name, reserved)

* `name` - Name of the keyword.
* `reserved` - 'Y' if the keyword is reserved, 'N' otherwise.

## comdb2_limits

Describes all the hard limits in the database.

    comdb2_limits(name, description, value)

* `name` - Name of the limit.
* `description` - Description of the limit.
* `value` - Value of the limit.

## comdb2_tunables

Describes all the tunables in the database.

    comdb2_tunables(name, description, type, value, read_only)

* `name` - Name of the tunable.
* `description` - Description of the tunable.
* `type` - Type of the tunable.
* `value` - Current value of the tunable.
* `read_only` - 'Y' if the tunable is READ-ONLY, 'N' otherwise.

## comdb2_threadpools

Information about thread pools in the database.

    comdb2_threadpools(name, status, num_thd, free_thd, peak_thd, num_creates,
                       num_exits, num_passed, num_enqueued, num_dequeued,
                       num_timeout, num_failed_dispatches, min_thds, max_thds,
                       peak_queue, max_queue, queue, long_wait_ms,
                       linger_secs, stack_size, max_queue_override,
                       max_queue_age_ms, exit_on_create_fail, dump_on_full)

* `name` - Name of the thread pool.
* `status` - Status of the thread pool.
* `num_thd` - Total number of threads.
* `free_thd` - Number of free threads.
* `peak_thd` - Peak number of threads.
* `num_creates` - Total number of thread created.
* `num_exits` - Total number of threads exited.
* `num_passed` - Work items done immediately.
* `num_enqueued` - Number of work items enqueued.
* `num_dequeued` - Number of work items dequeued.
* `num_timeout` - Number of work items timed-out.
* `num_failed_dispatches` - Number of failed dispatches.
* `min_thds` - Desired number of threads.
* `max_thds` - Maximum number of threads.
* `peak_queue` - Work queue peak size.
* `max_queue` - Work queue maximum size.
* `queue` - Work queue current size.
* `long_wait_ms` - Long wait alarm threshold.
* `linger_secs` - Thread linger time.
* `stack_size` - Thread stack size.
* `max_queue_override` - Maximum queue overload.
* `max_queue_age_ms` - Maximum queue age.
* `exit_on_create_fail` - If 'Y', exit on failure to create thread.
* `dump_on_full` - If 'Y', dump on queue full.

## comdb2_active_osqls

Information about OSQL requests in the database

    comdb2_active_osqls(type, origin, argv0, where, cnonce, request_id, nops,
                        start_time, commit_time, nretries)

* `type` - "OSQL" for active osql streams and "BPLOG" for active block processors.
* `origin` - Where the request is from
* `argv0` - Program that generates this request
* `where` - Stack trace of this request
* `cnonce` - Cnonce of this request
* `request_id` - UUID of this request
* `nops` - Number of OSQL operations
* `start_time` - Time when this request is created
* `commit_time` - Commit time of this request
* `nretries` - Number of retries

## comdb2_blkseq

Information about BLKSEQ stored in the database

    comdb2_blkseq(stripe, index, id, size, rcode, time, age)

* `stripe` - Stripe of a blkseq file
* `index` - Index of a blkseq file
* `id` - Identifier of the request
* `size` - Size of the blkseq entry
* `rcode` - Return code of this request
* `time` - Epoch time when this blkseq was added
* `age` - Time in seconds since the blkseq was added

## comdb2_timepartitions

Information about time partitions 

    comdb2_timepartitions(name, period, retention, nshards, version, shard0name, starttime, sourceid)

* `name` - Name of the time partition
* `period` - How often this partition rollouts as "DAILY|WEEKLY|MONTHTLY|YEARLY"
* `retention` - How many shards are preserved; older gets removed when retention is reached
* `nshards` - How many shards are already present, which is retention for fully grown time partitions
* `version` - Schema change version, matching the version of underlying tables
* `shard0name` - Name of the initial table used to seed the time partition
* `start` - "epoch" seconds when the first rollout happens/happened
* `sourceid` - uuid identifying the partition

## comdb2_timepartshards

Information about time partition shards

    comdb2_timepartshards(name, shardname, low, high)

* `name` - Name of the time partition
* `shardname` - Name of the underlying shard table
* `low` - minimum "epoch" seconds for the shard; all rows in it were inserted after this time
* `high` - maximum "epoch" seconds for the shard; all rows in it were inserted before this time

## comdb2_timepartevents

Information about time partition events in the dedicated cron scheduler (alias for filtered comdb2_cron_events systable)

    comdb2_timepartevents(name, type, arg1, arg2, arg3, sourceid)

* `name` - Name of the event as "AddShard"|"RollShard"|"DropShard"
* `type` - Type of the scheduler, here defaults to "timepart_sched"
* `arg1` - First argument for the event, usually the shard table name involved
* `arg2` - Generic second argument for the event
* `arg3` - Generic third argument for the event
* `sourceid` - uuid identifying the partition generating the event, if any

## comdb2_cron_schedulers

Information about schedulers running

    comdb2_cron_schedulers(name, type, running, nevents, description)

* `name` - Name of the scheduler
* `type` - Type of the scheduler, as "WALLTIME" for a time cron
* `running` - Set to 1 if the scheduler is running an event at this time, 0 otherwise
* `nevents` - How many events are queued in this scheduler
* `description` - Details the purpose of the scheduler; for example, there is a time partition scheduler, or a memory modules stat scheduler

## comdb2_cron_events

Information about events queued in all the schedulers running

    comdb2_cron_events(name, type, epoch, arg1, arg2, arg3, sourceid)

* `name` - Name of the event
* `type` - Type of the scheduler
* `epoch` - "epoch" seconds when the event is intended to run
* `arg1` - First argument for the event, usually the shard table name involved
* `arg2` - Generic second argument for the event
* `arg3` - Generic third argument for the event
* `sourceid` - uuid identifying the client generating the event, if any

