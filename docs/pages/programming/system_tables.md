---
title: System Tables
keywords: code
sidebar: mydoc_sidebar
permalink: system_tables.html
---

## comdb2_active_osqls

Information about OSQL requests in the database.

    comdb2_active_osqls(type, origin, argv0, where, cnonce, request_id, nops,
                        start_time, commit_time, nretries)

* `type` - "OSQL" for active osql streams and "BPLOG" for active block processors.
* `origin` - Where the request is from
* `argv0` - Program that generates this request
* `where` - Stack trace of this request
* `cnonce` - cnonce (client nonce) of this request
* `request_id` - UUID of this request
* `nops` - Number of OSQL operations
* `start_time` - Time when this request is created
* `commit_time` - Commit time of this request
* `nretries` - Number of retries

## comdb2_appsock_handlers

Lists all available APPSOCK handlers.

   comdb2_appsock_handlers(name, usage, exec_count)

* `name` - Name of the APPSOCK handler
* `usage` - Usage information
* `exec_count` - Execution count

## comdb2_blkseq

Information about BLKSEQ stored in the database. (TODO: Explain BLKSEQ)

    comdb2_blkseq(stripe, index, id, size, rcode, time, age)

* `stripe` - Stripe of a BLKSEQ file
* `index` - Index of a BLKSEQ file
* `id` - Identifier of the request
* `size` - Size of the BLKSEQ entry
* `rcode` - Return code of this request
* `time` - Epoch time when this BLKSEQ was added
* `age` - Time in seconds since the BLKSEQ was added

## comdb2_clientstats

Lists statistics about clients.

    comdb2_clientstats(task, stack, host, ip, finds, rngexts, writes,
                       other_fstsnds, adds, upds, dels, bsql, recom,
                       snapisol, serial, sql_queries, sql_steps, sql_rows,
                       svc_time)

* `task` - Name of the client program
* `stack` - Stack of the client program
* `host` - Client host name
* `ip` - Client IP address
* `finds` - `obsolete`
* `rngexts` -  `obsolete`
* `writes` - `obsolete`
* `other_fstsnds` - `obsolete`
* `adds` - `obsolete`
* `upds` - `obsolete`
* `dels` - `obsolete`
* `bsql` - `obsolete`
* `recom` - `obsolte`
* `snapisol` - `obsolete`
* `serial` - `obsolete`
* `sql_queries` - Total number of SQL queries executed
* `sql_steps` - Total number of steps (basic internal operations) executed
* `sql_rows` - Total number of rows returned
* `svc_time` - Total time taken to execute queries (in milliseconds)

## comdb2_cluster

Information about nodes in the cluster.

    comdb2_cluster(host, port, is_master, coherent_state)

* `host` - Host name of the node
* `port` - Port number of the node
* `is_master` - Is the node master?
* `coherent_state` - Is the node coherent?

## comdb2_columns

Describes all the columns for all of the tables in the database.

    comdb2_columns(tablename, columnnname, type, size, sqltype,
    varinlinesize, defaultvalue, dbload, isnullable)

* `tablename` - Name of the table
* `columnname` - Name of the column
* `size` - The storage size of the column
* `sqltype` - The type as recognized by SQL
* `varinlinesize` - The size of the data stored in the column when inlined
* `defaultvalue` - The default value for this column
* `dbload` - `obsolete`
* `isnullable` - `Y` if this column can hold nulls

## comdb2_completion

This table lists all keywords & identifiers based on the current state of
the database. cdb2sql uses it to provide auto-completion feature.

    comdb2_completion(candidate)

* `candidate` - keywords/identifiers

## comdb2_constraints

Shows all foreign key constraints on tables in the database.

    comdb2_constraints(tablename, keyname, foreigntablename,
    foreignkeyname, iscascadingdelete, iscascadingupdate)

* `tablename` - Name of the table
* `keyname` - Name of the key
* `foreigntablename` - Name of the foreign table
* `foreignkeyname` - Name of the foreign key
* `iscascadingdelete` - `Y` if this is a cascading delete
* `iscascadingupdate` - `Y` if this is a cascading update

## comdb2_cron_events

Information about events queued in all the schedulers running.

    comdb2_cron_events(name, type, epoch, arg1, arg2, arg3, sourceid)

* `name` - Name of the event
* `type` - Type of the scheduler
* `epoch` - Unix time when the event is intended to run
* `arg1` - First argument for the event, usually the shard table name involved
* `arg2` - Generic second argument for the event
* `arg3` - Generic third argument for the event
* `sourceid` - UUID identifying the client generating the event, if any

## comdb2_cron_schedulers

Information about schedulers running.

    comdb2_cron_schedulers(name, type, running, nevents, description)

* `name` - Name of the scheduler
* `type` - Type of the scheduler, as "WALLTIME" for a time cron
* `running` - Set to `1` if the scheduler is running an event at this time,
              `0` otherwise
* `nevents` - How many events are queued in this scheduler?
* `description` - Details the purpose of the scheduler; for example, there is
                  a time partition scheduler, or a memory modules stat scheduler

## comdb2_keycomponents

Describes all the components of the keys.

    comdb2_keycomponents(tablename, keyname, columnnumber, columnname,
    isdescending)

* `tablename` - Name of the table
* `keyname` - Name of the key
* `columnnumber` - Position of `columnname` in `keyname`
* `columnname` - Name of a column in `keyname`
* `isdescending` - `Y` if this column is descending

## comdb2_keys

Describes all of the keys in the database.

    comdb2_keys(tablename, keyname, keynumber, isunique, isdatacopy,
    isrecnum, condition)

* `tablename` - Name of the table
* `keyname` - Name of the key
* `isunique` - `Y` if this key is unique
* `isdatacopy` - `Y` if the data is inlined with this key
* `isrecnum` - `Y` if this key has recnums
* `condition` - Where condition for this index
* `uniqnulls` - `Y` if this key treats NULL values as unique

## comdb2_keywords

Describes all the keywords used in the database. A reserved keyword needs to be
quoted when used as an identifier.

    comdb2_keywords(name, reserved)

* `name` - Name of the keyword
* `reserved` - 'Y' if the keyword is reserved, 'N' otherwise

## comdb2_limits

Describes all the hard limits in the database.

    comdb2_limits(name, description, value)

* `name` - Name of the limit
* `description` - Description of the limit
* `value` - Value of the limit

## comdb2_locks

Lists all active comdb2 locks.

   comdb2_locks(thread, lockerid, mode, status, object, locktype, page)

* `thread` - Thread Id of the owner thread
* `lockerid` - Locker Id
* `mode` - Lock mode (`DIRTY_READ`, `IREAD`, `IWR`, `IWRITE`, `NG`, `READ`,
          `WRITE`, `WRITEADD`, `WRITEDEL`, `WAS_WRITE`, `WAIT`)
* `status` - Status (`ABORT`, `ERROR`, `FREE`, `HELD`, `WAIT`, `PENDING`,
             `EXPIRED`)
* `object` - Locked object
* `locktype` - Lock type (`PAGE`, `HANDLE`, `KEYHASH`, `ROWLOCK`, `MINMAX`,
              `TABLELOCK`, `STRIPELOCK`, `LSN`, `ENV`)
* `page` - Page number

## comdb2_logical_operations

Lists all logical operations

    comdb2_logical_operations(commitlsn, opnum, operation, tablename, oldgenid,
                              oldrecord, genid, record)

* `commitlsn` - Log sequence number
* `opnum` - Index of operation within a transaction
* `operation` - Type of logical operation
* `tablename` - Table name
* `oldgenid` - Old record's generation Id
* `oldrecord` - Old record
* `genid` -  New record's generation Id
* `record` - New record

## comdb2_metrics

Shows various operational and performance metrics.

    comdb2_metrics(name, description, type, value, collection_type)

* `name` - Name of the metrics
* `description` - Description
* `type` - Type of the metrics
* `value` - Value
* `collection_type` - Is value `cumulative` or `latest`? A cumulative metric is
                      a cumulative sum over the time; A latest metric is a
                      instantaneous measurement.

## comdb2_net_userfuncs

Statistics about network packets sent across cluster nodes.

    comdb2_net_userfuncs(service, userfunc, count, totalus)

* `service` - Class of network packet
* `userfunc` - Network packet type (handler/user function)
* `count` - Total number of invocations of this user function
* `totalus` - Total execution time of this user function (in microseconds)

## comdb2_opcode_handlers

Lists all opcode handlers available in Comdb2.

    comdb2_opcode_handlers(opcode, name)

* `opcode` - Number assigned to the opcode handler
* `name` - Name of the opcode handler

## comdb2_plugins

Lists all plugins currently available in Comdb2.

    comdb2_plugins(name, description, type, version, is_static)

* `name` - Name of the plugin
* `description` - Description
* `type` - Type of plugin
* `version` - Plugin version
* `is_static` - Is plugin static or dynamic?

## comdb2_procedures

List all stored procedures in the database.

    comdb2_procedures(name, version, client_versioned, default, src)

* `name` - Name of the stored procedure
* `version` - Stored procedure version
* `client_versioned` -  Is client versioned?
* `default` - Is default?
* `src` - Source

## comdb2_queues

List all queues in the database.

    comdb2_queues(queuename, spname, head_age, depth)

* `queuename` - Name of the queue
* `spname` - Stored procedure attached to the queue
* `head_age` - Age of the head element in the queue
* `depth` - Number of elements in the queue

## comdb2_repl_stats

Replication statistics.

    comdb2_repl_stats(host, bytes_written, bytes_read, throttle_waits, reorders,
                      avg_wait_over_10secs, max_wait_over_10secs,
                      avg_wait_over_1min,  max_wait_over_1min)

* `host` - Host name
* `bytes_written` - Number of bytes written
* `bytes_read` - Number of bytes read
* `throttle_waits` - Number of throttle waits
* `reorders` - Number of reorders (see `enque_reorder_lookahead` under
               [Network configurations](config_files.html#networks))
* `avg_wait_over_10secs` - Average of waits over 10 seconds
* `max_wait_over_10secs` - Maximum of waits over 10 seconds
* `avg_wait_over_1min` - Average of waits over a minute
* `max_wait_over_1min` - Maximum of waits over a minute

## comdb2_replication_netqueue

This table lists the current state of the replication network queue per node.

    comdb2_replication_netqueue(machine, total, min_lsn, max_lsn, alive,
                                alive_req, all_req, dupmaster, file, file_req,
                                log, log_more, log_req, master_req, newclient,
                                newfile, newmaster, newsite, page, page_req,
                                plist, plist_req, verify, verify_fail,
                                verify_req, vote1, vote2, log_logput,
                                pgdump_req, gen_vote1, gen_vote2, log_fill,
                                uncategorized, unknown)

* `machine` - Host name of the node
* `total` - Number of total messages
* `min_lsn` - Minimum LSN in the queue
* `max_lsn` - Maximum LSN in the queue
* `alive` - Number of 'I am alive' messages
* `alive_req` - Number of requests for an alive message
* `all_req` - Number of requests for all log records greater than LSN
* `dupmaster` - Number of 'Duplicate master detected' messages
* `file` - Number of 'page of a database file' messages
* `file_req` - Number of requests for a database file
* `log` - Number of log record messages
* `log_more` - Number of 'more log records to request' messages
* `log_req` - Number of requests for a log record
* `master_req` - Number of 'Who is the master' messages
* `newclient` - Number of 'presence of new clients' announcements
* `newfile` - Number of 'log file change' announcements
* `newmaster` - Number of 'who the master is' announcements
* `newsite` - Number of 'heard from a new site' announcements
* `page` - Number of 'database page' messages
* `page_req` - Number of requests for a database page
* `plist` - Number of 'database page list' messages
* `plist_req` - Number of requests for a page list
* `verify` - Number of 'verification log record' messages
* `verify_fail` - Number of 'client is outdated' messages
* `verify_req` - Number of requests for a log record to verify
* `vote1` -  Number of 'basic information for election' messages
* `vote2` - Number of 'you are master' messages
* `log_logput` - Master internal, same as `log`
* `pgdump_req` - Number of requests to dump a page for a given file Id (for debugging)
* `gen_vote1` - Same as vote1 (also contains the generation number)
* `gen_vote2` - Same as vote2 (also contains the generation number)
* `log_fill` - Number of log_fill messages
* `uncategorized` - Number of 'uncategorized' messages
* `unknown` - Number of 'unknown' messages

## comdb2_sqlpool_queue

Information about SQL query pool status.

    comdb2_sqlpool_queue(time_in_queue_ms, sql)

* `time_in_queue_ms` - Total time spent in queue (in milliseconds)
* `sql` - SQL query

## comdb2_systables

List all available system tables in Comdb2.

    comdb2_systables(name)

* `name` - Name of the system table

## comdb2_tablepermissions

Table of permissions for tables in the database.

    comdb2_tablepermissions(tablename, username, READ, WRITE, DDL)

* `tablename` - Name of the table
* `username` - Name of the user
* `READ` - `Y` if `username` has read access to `tablename`
* `WRITE` - `Y` if `username` has write access to `tablename`
* `DDL` - `Y` if `username` can modify `tablename` schema

## comdb2_tables

This is a table of all the existing tables in the database.

    comdb2_tables(tablename)

* `tablename` - Name of the table

## comdb2_tablesizes

Shows the sizes on disk of the tables.

    comdb2_tablesizes(tablename, bytes)

* `tablename` - Name of the table
* `bytes` - Size of the table in bytes

## comdb2_threadpools

Information about thread pools in the database.

    comdb2_threadpools(name, status, num_thd, free_thd, peak_thd, num_creates,
                       num_exits, num_passed, num_enqueued, num_dequeued,
                       num_timeout, num_failed_dispatches, min_thds, max_thds,
                       peak_queue, max_queue, queue, long_wait_ms,
                       linger_secs, stack_size, max_queue_override,
                       max_queue_age_ms, exit_on_create_fail, dump_on_full)

* `name` - Name of the thread pool
* `status` - Status of the thread pool
* `num_thd` - Total number of threads
* `free_thd` - Number of free threads
* `peak_thd` - Peak number of threads
* `num_creates` - Total number of thread created
* `num_exits` - Total number of threads exited
* `num_passed` - Work items done immediately
* `num_enqueued` - Number of work items enqueued
* `num_dequeued` - Number of work items dequeued
* `num_timeout` - Number of work items timed-out
* `num_failed_dispatches` - Number of failed dispatches
* `min_thds` - Desired number of threads
* `max_thds` - Maximum number of threads
* `peak_queue` - Work queue peak size
* `max_queue` - Work queue maximum size
* `queue` - Work queue current size
* `long_wait_ms` - Long wait alarm threshold
* `linger_secs` - Thread linger time
* `stack_size` - Thread stack size
* `max_queue_override` - Maximum queue overload
* `max_queue_age_ms` - Maximum queue age
* `exit_on_create_fail` - If 'Y', exit on failure to create thread
* `dump_on_full` - If 'Y', dump on queue full

## comdb2_timepartevents

Information about time partition events in the dedicated cron scheduler (alias
for filtered comdb2_cron_events system table).

    comdb2_timepartevents(name, type, arg1, arg2, arg3, sourceid)

* `name` - Name of the event (`AddShard`|`RollShard`|`DropShard`)
* `type` - Type of the scheduler, here defaults to "timepart_sched"
* `arg1` - First argument for the event, usually the shard table name involved
* `arg2` - Generic second argument for the event
* `arg3` - Generic third argument for the event
* `sourceid` - UUID identifying the partition generating the event, if any

## comdb2_timepartitions

Information about time partitions.

    comdb2_timepartitions(name, period, retention, nshards, version, shard0name, starttime, sourceid)

* `name` - Name of the time partition
* `period` - How often this partition rolls out? (`DAILY`|`WEEKLY`|`MONTHTLY`|`YEARLY`)
* `retention` - How many shards are preserved; older gets removed when retention is reached
* `nshards` - How many shards are already present, which is retention for fully grown time partitions
* `version` - Schema change version, matching the version of underlying tables
* `shard0name` - Name of the initial table used to seed the time partition
* `start` - "epoch" seconds when the first rollout happens/happened
* `sourceid` - UUID identifying the partition

## comdb2_timepartshards

Information about time partition shards.

    comdb2_timepartshards(name, shardname, start, end)

* `name` - Name of the time partition
* `shardname` - Name of the underlying shard table
* `start` - Minimum "epoch" seconds for the shard; all rows in it were inserted after this time
* `end` - Maximum "epoch" seconds for the shard; all rows in it were inserted before this time

## comdb2_timeseries

This table lists various metrics.

    comdb2_timeseries(metric, time, value)

* `metric` - Name of the metric
* `time` - Timestamp
* `value` - Value

## comdb2_transaction_logs

Lists all the transaction log records.

    comdb2_transaction_logs(lsn, rectype, generation, timestamp, payload)

* `lsn` - Log sequence number
* `rectype` - Record type
* `generation` - Generation ID
* `timestamp` - Timestamp
* `payload` - Paylod

## comdb2_triggers

Lists triggers in the database.

    comdb2_triggers(name, type, tbl_name, event, col)

* `name` - Name of the trigger
* `type` - Type of the trigger
* `tbl_name` - Name of the table
* `event` - Event to trigger on
* `col` - Column to trigger on

## comdb2_tunables

Describes all the knobs in the database.

    comdb2_tunables(name, description, type, value, read_only)

* `name` - Name of the tunable
* `description` - Description of the tunable
* `type` - Type of the tunable
* `value` - Current value of the tunable
* `read_only` - 'Y' if the tunable is READ-ONLY, 'N' otherwise

## comdb2_type_samples

Describes the sample values of all the data types supported by Comdb2.

    comdb2_type_samples(integer, real, cstring, blob, datetime, intervalym,
                        intervalds, datetimeus, intervaldsus)

* `integer` - Sample value of 'integer' type
* `real` - Sample value of 'real' type
* `cstring` - Sample value of 'cstring' type
* `blob` - Sample value of 'blob' type
* `datetime` - Sample value of 'datetime' type
* `intervalym` - Sample value of 'intervalym' type
* `intervalds` - Sample value of 'intervalds' type
* `datetimeus` - Sample value of 'datetimeus' type
* `intervaldsus` - Sample value of 'intervaldsus' type

## comdb2_users

Table of users for the database that do or do not have operator access.

    comdb2_users(username, isOP)

* `username` - Name of the user
* `isOP` - 'Y' if 'username' has operator access

## comdb2_sc_status

Information about recent schema changes.

    comdb2_sc_status(name, type, newcsc2, start, status, seed, last_updated,
                     converted, error)

* `name` - Name of the table.
* `type` - Type of the schema change.
* `newcsc2` - New schema in csc2 format.
* `start` - Start time of the schema change.
* `status` - Current status of the schema change.
* `seed` - Seed (ID) of schema change running for this table (NULL if not currently running).
* `last_updated` - Time of the last status change.
* `converted` - Number of records converted.
* `error` - Error message of the schema change.

## comdb2_views

List of views in the database.

    comdb2_views(name, definition)

* `name` - Name of the view
* `definition` - View definition

