---
title: Operational Commands
keywords: operational
sidebar: mydoc_sidebar
permalink: op.html
---

Database internal state can be queried with the sys.cmd.send() stored procedure.  It takes a text command,
and returns a response as lines of text (ie: a set of rows with a single text column).  If authentication is 
enabled on the db, the user needs to have OP privilege to execute sys.cmd.send().  This section outlines the 
available commands and sample output.  We'll refer to strings passed to sys.cmd.send() as commands in the 
rest of this document.

## Shutting down the database 

The 'exit' command will do a clean shutdown of a database process.  Namely, the database will

  * Tell the rest of the cluster that it's exiting cleanly (so it won't count as an unconnected node for the purposes of election.)
  * If it's the master node, it'll downgrade to a replicant.
  * Flush its buffer pool to disk.
  * Exit

This command only effects the current node.  It can be run with the "@" target syntax to shut down a specific node, eg:

    cdb2sql testdb @host1 "exec procedure sys.cmd.send('exit')"

## Toggling tunables

The [configuration files](config_files.html) section mentions a great many tunables.  Most of these can be changed at runtime.

### BDB tunables:

All `bdb` tunables can be set with `exec procedure sys.cmd.send('bdb setattr name value')`.  

They can be queried with `exec procedure sys.cmd.send('bdb attr')`

### On/Off switches

Any switches configured with `on`/`off` in the configuration file can also be toggled with 
`exec procedure sys.cmd.send('on switchname')` or `exec procedure sys.cmd.send('off switchname')`.  

They can be queried with `exec procedure sys.cmd.send('stat switch')`.


### BerkeleyDB tunables

All BerkeleyDB tunables can be changed with `exec procedure sys.cmd.send('berkattr set name value')`.

They can be queried with `exec procedure sys.cmd.send('berkattr')`

## Thread pool tunables

There are several thread pools in Comdb2.  They have a unified interface.  These commands exist for changing parameters
and querying the state of thread pools.  For example the SQL query threads pool stats can be addressed with 
'sqlenginepool stat' command.

### stat 

Displays threads pool status.

```
Thread pool [memptrickle] stats
  Status                    : running
  Current num threads       : 0
  Num free threads          : 0
  Peak num threads          : 4
  Num thread creates        : 12
  Num thread exits          : 12
  Work items done immediate : 20
  Num work items enqueued   : 172
  Num work items dequeued   : 172
  Num work items timeout    : 0
  Num failed dispatches     : 0
  Desired num threads       : 1
  Maximum num threads       : 4
  Work queue peak size      : 98
  Work queue maximum size   : 8000
  Work queue current size   : 0
  Long wait alarm threshold : 30000 ms
  Thread linger time        : 10 seconds
  Thread stack size         : 1048576 bytes
  Maximum queue overload    : 0
  Maximum queue age         : 0 ms
  Exit on thread errors     : yes
  Dump on queue full        : no
  Busy threads histogram    :  0:       9,  1:       5,  2:       3,  3:       3
  Busy threads histogram    :  4:     172
```

Output looks like the above.  Most of the details are self-explanatory.  The histogram at the bottom
displays how many requests have been processed by each thread.

### stop

Shuts down the threadpool.  It'll stop accepting work.

### resume

Resumes the thread pool if it's been previously stopped.

### mint

Takes an integer argument.  Sets the number of threads that will always be running
waiting for work.  After a certain amount of time (the [linger](#linger) time) threads above
this count will go away.

### maxt

The maximum number of threads in the pool.  If work is required, and this many threads are busy,
the work goes in a pool.

### maxq

The maximum length of the threadpool queue.  If the queue reaches this size, further work items are rejected.

These commands are used to query/configure the setup of a cluster.

### longwait

Takes number of milliseconds as an argument.  An item waiting for a thread long than that will generate an alarm.

### linger

Takes number of milliseconds as an argument.  Defined how long thread linger without work before going away (as long
as at least [mint](#mint) threads exist.

### Thread Pools

  * appsockpool   - thread pool for client connection
  * sqlenginepool - thread pool for SQL queries
  * iopool        - thread pool for background flushing dirty pages

## Cluster commands

### downgrade

Downgrade has no effect when run on a replicant.  When run on the master, it will downgrade itself to a replicant and hand off mastership
to another node.  Note that there's no guarantee that the master will end up on a different node - the node that's furthest along
in the log stream will be preferred.

### losemaster

Similar to downgrade.  No effect on replicant.  On master will attempt to downgrade.  Unlike "downgrade", will not hand off mastership,
and will instead force an election.


### upgrade

Similar to downgrade.  Takes a node name as a required argument.  Will downgrade and attempt to hand off mastership to the specified node.
Node that rules about the node furthest along becoming master are still followed.  If the specified node is behind, it may not become master.

### synccluster

Flush the buffer pool to disk, force a checkpoint (master only) and wait for all replicants to catch up to the checkpoint's LSN. 
Note that this doesn't force replicants to write their own buffer pools to disk.

### bdb cluster

Show the cluster status.  The output looks like this:

```
db engine cluster status
        machine1:19134 MASTER c fd 13  lsn 1:726017 f 0 
        machine2:19019        l fd -1  lsn 1:726017 f 0 
```

Each line starts with the machine name and database port.  'c' indicates that the current machine is connected.
'l' means the machine is local (ie: the machine the output came from).  'fd N' is the file descriptor for the 
connection to the specified machine from the current machine (-1 for local).  `1:726017` is the LSN (log sequence
number) that the node last reported.  Note that nodes only report their LSN to the master, so the numbers reported
here are only accurate when the command is run on the master machine.  The master is marked with 'MASTER'.  'f N'
is the lowest log file the machine is willing to delete.

### bdb add

Takes a hostname as a required argument.  Adds the host to the cluster's "sanctioned" list.  Machines in the
cluster will not delete logs when the sanctioned list isn't intact (that is when machines from the sanctioned list
are not connected and part of the cluster).  Note that this command is necessary only when a brand new node is being
added to an active cluster.  Nodes configured in any database lrl file are always in the sanctioned list of all nodes.
The `bdb add` message is replicated to all nodes on the cluster -- adding a machine to one node adds it on all 
machines in the cluster.

### bdb del

The opposite of `bdb add`.  Takes a hostname as a required argument.  Removes that machine from the sanctioned list.
Nodes will not hold logs on behalf of that machine anymore.  The message is likewise replicated to all nodes in the 
cluster.

### bdb sanc

This dumps the sanctioned list for the cluster. It also indicates whether the list is intact or if nodes are missing.

### bdb log_archive

This lists all the log files that can be considered for deletion (ie: they are older than the oldest active transaction).
They may still not be deleted for other reasons (eg: needed to recover other nodes, etc.)

### pushnext

Force enough write traffic to have the database switch to a new log file.  Log files are 40MB by default.

### early

Acknowledge transactions early.  This is the default setting.  Transactions will be acknowledged by a replicant as soon as it
acquired all the necessary locks to apply the transaction.  At this point, any clients that try to access data modified by
the transaction will block.  Enabling early acks will allow better transaction latency.  Note that this doesn't change
read-after-write semantics - any record written by a committed transaction is guaranteed to be readable on any node in the cluster.

### noearly

Disable early replication acks.  Replicants will ack a transaction only after all the log records for it are applied against the data.

### reallyearly

Enables "really early" replication acknowledgments.  A replicant will ack a transaction immediately after receiving the commit log record.
Enabling this DOES affect read-after-write semantics. It's possible that a record written by a committed transaction may not yet
be applied when an application tries to read it.  For applications where this doesn't matter, reallyearly offers lower transaction
latency than the [early](#early) setting.

### noreallyearly

Disable really early replication acks.  Goes back to default behavior: ack a transaction only after all the log records for it 
are applied against the data.

### testcompr

This command can be used to test the available compression algorithms on a sampled subset of a table, that way user can see which algorithm is best suited for the given table. To run it you can issue `testcompr table <tbl>`. There are two parameters you can set: `testcompr percent <value>` to set the percentage of the table to sample, default is set to 10%, and `testcompr max <value>` to set the max number of records to process, set to 0 to process all records, default is set to 300,000.

### repscon

Like [scon](#scon-and-scof), turns on per-second reporting of replication/acknowledgment times to other nodes.

### repscof

Turns off replication/acknowledgment times.


## SQL commands

### sql dump

Dump a list of currently running SQL queries.

### sql keep

Takes an integer argument.  Will keep that many prior queries in memory to be displayed with the [sql keep](#sql-hist) command.
The default setting is 25.

### sql hist

Dumps the last set of SQL queries that completed.  The number it remembers can be adjusted with the [sql keep](#sql-keep) command.

### sql cancel

[sql dump](#sql-dump) displays an id number next to every query. `sql cancel` takes that number as the argument and cancels 
the query with the given number.  An error comes back to the application that started the query.

### analyze abort

Analyze commands configure parameters that determine how index statistics are gathered by the [analyze](sql.html#analyze)
command.  There are a few options available, but they should be set in the ANALYZE statement itself.  The only useful commands
are `analyze backout` and `analyze abort`.  `Analyze abort` makes the database stop the current ANALYZE run.  Index statistics
that haven't been updated already are not updated.

### analyze backout

The database keeps 2 sets of ANALYZE results. `Analyze backout` makes it switch to the previous set (useful if query plans
get worse after an ANALYZE) run.

## sqllogger

SQL logger options.  This takes the same options as [sqllogger configuration options](config_files.html#sqllogger-commands).
In addition, it takes the following commands:

### off

Turn off SQL logging.

### roll

Roll the SQL log now.

## reql

Request logging options.  The request logger is responsible for producing the [statreqs and longreqs files](logs.html). The system
has a few features for setting up custom logging.  'reql help' will show full usage.  The most relevant options are:

### diffstat

Takes a time in seconds.  Sets the frequency at which we dump information to the [statreqs file](logs.html).

### longsqlrequest

Sets the threshold (in ms) at which we log SQL statements to the [longreqs file](logs.html).  Set to 0 to log everything.
The default is 5000ms.

### longrequest

Sets the threshold (in ms) at which we log transaction requests from replicants to the master to the [longreqs file](logs.html).  Set to 0 to log everything.
The default is 2000ms.

### stat

Display logging statistics.

### longreqfile

Takes a filename as argument.  Starts logging long requests (those that go over the [longsqlrequest](#longsqlrequest) and [longrequest](#longrequest) limits)
to the specified file.

## Logging commands

Trace produced by the database is controlled by the `logmsg` command.  The default log level is `warn`.  This displays important startup messages
and runtime errors.

### logmsg level

Accepts a log level as an argument.  Sets the default database logging level to this.  The following levels are available.  Each level includes messages
of all lower (less strict) levels.

| Level | Explanation
+-------|------------------------
| debug | all messages 
| info  | information messages
| warn  | warnings - things that demand attention but won't result in errors
| error | errors - things that will result in application errors

### logmsg timestamp

Log a timestamp with each message.  Useful if the database is in your terminal for testing. 

### logmsg notimestamp

Disable timestamp prefixes for messages.

### logmsg syslog

Send messages to syslog instead of stdout.

### logmsg nosyslog

Send messages to stdout.  Useful if the database is running under a process manager like supervisord.

## Misc commands

### delay

Takes a number of milliseconds as the argument.  Adds an additional pause of that many milliseconds after every commit.
This is an operational tool - it allows an operator to throttle database traffic in order to allow replicants to 
catch up under very heavy load.  Replicants, when they come up and can't catch up to the master will gradually ask
it to raise this value.

### delaymax

Takes a number of milliseconds as an argument.  Caps what the masters is willing to set as the maximum delay value.  The
default is 8ms.

### maxwt

Sets the maximum number of threads to allocate on the master to processing transactions.  Note that these are threads
that run an actual transaction scheduled by a replicant on behalf of the user - this is NOT where SQL queries run.

### maxq

Maximum queue size for master transaction threads.  When a master transaction thread isn't available, a transaction
goes on queue.

### ling

How long a master transaction thread should linger after being created if there's no work for it to do.

### debg

Take a number of seconds as an argument.  For that many seconds, the database will produce verbose trace about what
it's doing.  The trace is meant more for database developers than application developers, but still contains useful
information.

### ndebg

Like ndebg, but takes the next N requests to produce debug information for, not the number of seconds.


### flush

Force the database to flush the buffer pool to disk and write a checkpoint.

### sync

Allows changing database [sync settings](config_files.html#sync-commands) at runtime.  Takes the same arguments
as the corresponding configuration file options.

### electtime

Takes a number of seconds as the argument.  Sets the election timeout to that many seconds.  The default is 5 seconds,
but the database will scale that value if it can't elect in that interval.

### morestripe

Takes an integer argument.  Increases the database striping factor for data.  The default value is 8.  The maximum value
is 16.  Note that it's not possible to shrink this value.  If changing the value, you'll also need to add 'dtastripe N' to
your configuration file (the database will print a reminder to its stdout).  This is one of the few settings that 
needs to be persisted in the lrl file rather than the db metadata.

### pushlogs

Takes an LSN in file:offset format.  Does enough writes to make the logs reach the specified LSN.

### screportfrew

Takes an integer argument.  Sets the reporting frequency for the database [status log](logs.html) to that many seconds.

### scabort

Causes the database to stop any running schema change operation.

### readonly

Forces the database to be readonly.  It will not accept write requests from applications.

### readwrite

Makes the database readwrite (undoes the [readonly](#readonly) command)

### allow, disallow, clrpol, setclass

Allows changing source machines [permissions](config_files.html#allowdisallow-commands)

### dumoprecord

Takes a table name and a rowid.  Displays the associated record, and any index values.

### upgraderecord

Some schema changes can be applied "instantly" by recording the new version of the record.  A common case is adding
a new field.  Fetching records that are from older schema version requires an extra upgrade step per record every time
it's read.  This command takes a table name and a rowid, and forces an upgrade of this record to the most recent schema
version.

### upgradetable

Like upgraderecord, but takes a name of the table, and starts upgrading all the records in the backgroup.

### chkpoint_alarm_time

Takes a number of seconds as an argument. Warns when a checkpoint takes longer than this to run.  The default is 60 seconds.

### incoherent_alarm_time

Takes a number of seconds as an argument.  If a node remains incoherent for longer than this, alarm.  The default is 2 minutes.

### max_incoherent_nodes

Takes a number as an argument.  Alarm when more than this number of machines becomes incoherent at the same time. The default is 1.

### dumpsqlattr

Dumps a list of SQLite tunables (tunables that modify the SQLite query optimizer).

### setsqlattr

Takes an SQLite query optimizer [tunable](#dumpsqlattr) name and a numeric argument.  Sets the tunable to the given values.

### querylimit

Allows setting query limit tunables at runtime.  See [Query Limit Commands](config_files.html#query-limit-commands).

### maxretries

Takes an integer argument.  Sets the maximum number of times that the database will retry a transaction on a deadlock.  The default value is 500.

### page_order_scan

This controls table scan order.  The default order is to scan the data file by page.  Disabling page order tablescan makes
table scans travel in rowid order.  This is normally less optimal, except for cases where many rows are deleted.

#### enable

Takes a table name as argument. Enables page order table scan for that table.

#### disable

Takes a table name as argument. Disables page order table scan for that table.  Setting the bdb PAGE_ORDER_TABLESCAN tunable can be disabled to disable
page order table scans altogether.

#### stat

Displays statistics about page-order table scans.  Output includes list of all tables and their settings.

#### thresh

Sets a percent threshold at which page-order table scan remains enabled (in percent).  A table scan that finds more than
this percentage of pages to be empty will disable page-order scans for this table.

#### minnext

Sets the minimum number of pages for which page-order table scan will be disabled if it hits the threshold above.

### tcp

"Pings" other nodes in the cluster by using existing network connections and measuring response times.

### udp

Controls the UDP subsystem. UDP is used by Comdb2 to issue coherency leases and acknowledge transactions.  Subcommands are
listed below:

#### stat

Displays udp statistics for all nodes, for all connected networks.

#### reset

Resets udp counters for all nodes.

#### ping

Measure response times between this node and other nodes in the cluster.

#### on

Enables UDP if disabled.

#### off

Disabled UDP.  If disabled, coherency leases and transaction acks are delivered by TCP along with other replication traffic.

### decimal_rounding

Sets decimal rounding behavior at runtime.  See [Decimal Rounding Options](config_files.html#decimal-rounding-options).

### listpools

Lists the [thread pools](#thread-pool-tunables) known by the system.

### pools_do_all

Runs the rest of the command against all thread pools, see [thread pools](#thread-pool-tunables).

### rcache

Enable btree root page cache.

### norcache

Disable btree root page cache.

### stat4dump

Dump SQLite's stat4 table.

### panic

Cause a database panic.

### memstat

Display memory usage statistics.  There are a lot of options.  Try 'help memstat for a full list.' 

### partinfo

Display information about time partitioned tables.

### partitions

Commands to control time partitioned tables.

#### roll

Roll the tables now - create a new partition, delete oldest.

#### purge

Delete the oldest partition.

## Trace logs

### ctrace_rollat

Sets the threshold for when the database will roll its trace file, [see Logs](logs.html).  No number is set by default.

### ctrace_nlogs

Sets how many logs should be kept when rolling trace log files.  The default is 7.

### ctrace_roll

Roll the trace log now.

Display information about time partitions.

### fdb

Display information about foreign tables (tables in other databases) that have been referenced by this database.  Has
several subcommands.  `'fdb help` will show further usage information.

### netpoll

Sets the amount of time the database will block waiting for a new connection to identify itself.  Takes a single
integer argument.  The default is 100ms.  Connections that don't manage it in that time are dropped (client
connections will try again on another node).

### osqlnetpoll

Like [netpoll](#netpoll), but for the the [SQL network](config_files.html#networks).

### ioalarm

Sets the alarm threshold, in ms, for IO.  Page reads and writes that go over this threshold are logged to the database 
[debug log](logs.html).

### memp_sync_alarm

Like [ioalarm](#ioalarm) but for `fsync` calls and checkpoints.

### purge

Takes a table name, and a rowid as arguments.  Attempts to delete the row, along with any corresponding indices and blobs.
Does not stop on failure.  This is only needed if an index/blob is corrupt and a row can't be deleted by a regular `DELETE`
statement.

***TODO***
    } else if (tokcmp(tok, ltok, "disable_osql_prefault") == 0) {
    } else if (tokcmp(tok, ltok, "enable_osql_prefault") == 0) {
    } else if (tokcmp(tok, ltok, "get_osql_prefault_status") == 0) {
    } else if (tokcmp(tok, ltok, "enable_prefault_udp") == 0) {
    } else if (tokcmp(tok, ltok, "disable_prefault_udp") == 0) {
    } else if (tokcmp(tok, ltok, "set_udp_prefault_latency") == 0) {
    } else if (tokcmp(tok, ltok, "get_udp_prefault_status") == 0) {

### delfiles

This forces the database to delete any old files for a table.  On schema change, new files are created.  Old files are
normally deleted when the earliest log file that existed at the time is deleted.  This command forces immediate deletion
(eg: when space is needed and logs are being held for another node).

### scon and scof

Score ON and Score OFF.  Enables per-second trace from the database that shows events that occurred over the last
second.  This will display the number of SQL requests, lock waits, cache hits, cache missed, etc.

### stax

Display the size of each data record and key.

### stal

Displays information about the number of writer threads (relevant on current master).  Write
requests are managed by a thread pool.  This display the number of thread creations, deletions, waits, etc.

### thr

Displays a list of current connection and SQL processing threads, and their state.

### long

Display a count of opcodes executed by the current node.  eg:

```
REQUEST STATS FOR DB 0 't1'
    OSQL_DONE        3
    OSQL_DELREC      3
    OSQL_INSREC      1
    OSQL_UPDREC      3
    OSQL_UPDCOLS     2
```

### dmpl

Displays the current status of writer threads.  Part of the display is which routine the thread
is currently in, and how long it's been in that state.  This is useful for a quick "what's going on
in the database" view.

### nowatch

By default the database has a watchdog thread.  Its job is running a quick sanity check: can the database perform
basic operations like allocating memory, creating threads, etc.  A check failure causes the db to abort.  `nowatch`
disables the watchdog.

## Stat commands

### stat

Display a quick status.  Takes optional arguments to display stats for various subsystems.  Example follows bellow.  Anything
starting with a '#' is an explanation (doesn't appear in actual output).  The numbers are all cumulative since database start time.
  
```
cmd:'stat'
# Number of requests over an obsolete interfaces
num qtraps              0
num fstraps              0
  num bad parms         0
  num bad swapins       0
  num incoherent reject 0
  num missed            0
# Count of commits and aborts:
# commit         - number of transactions committed (if master)
# abort          - number of transactions aborted
# repcommit      - number of transactions committed (if replicant)
# retry          - number of transactions retried automatically on an abort
# verify retry   - number of transactions that are retried automatically because they were rejected due
#                - to being out of date
# rep retry      - number of transactions retried on the replicant due to deadlock with running SQL
commit 3 abort 0 repcommit 0 retry 0 verify retry 0 rep retry 0 max retry 0
# Is the database readonly?
readonly                N
# Number of SQL queries over an older (obsolete) interface
num sql queries         0
# Number of SQL queries over the current (cdb2api) interface
num new sql queries     9
# Number of steps (find/next operations done by any running queries).
sql ticks               14
# Number of times an SQL query had to retry due to a deadlock and the number of times re-establishing
# state due to deadlock failed.
sql deadlocks recover attempts 0 failures 0
blocksql->socksql reqs  0
# Are rowlocks enabled
rowlocks is             disabled
# Total number of client connections
num appsock connections 18
# Number of client connections currently active
num active appsock connections 1
# Number of commands sent by client connections (normally one per connection)
num appsock commands    18
# Number of snd and receive SQL operations done in various transaction modes
# Obsolete mode
osql snd(failed) 0(0) rcv(failed, redundant) 0(0,0)
# Default mode
sosql snd(failed) 3(0) rcv(failed, redundant) 19(0,0)
# Read committed mode
recom snd(failed) 0(0) rcv(failed, redundant) 0(0,0)
# Serial mode
serial snd(failed) 0(0) rcv(failed, redundant) 0(0,0)
# Default mode with extra cost information
cost_osql snd(failed) 0(0) rcv(failed, redundant) 0(0,0)
cost_sosql snd(failed) 0(0) rcv(failed, redundant) 0(0,0)
# Snapshot mode
snapisol snd(failed) 0(0) rcv(failed, redundant) 0(0,0)
(null) snd(failed) 0(0) rcv(failed, redundant) 0(0,0)
# Current setting for election time
elect timeout           5.000000 (global config)
schema change running   NO
# Current transaction delay
txn commit delay        0 ms (max 8 ms)
# Current master (xps is machine name in this example)
# When a master is another node, will say 'I AM NOT MASTER. MASTER IS othermachine'
I *AM* MASTER.  MASTER IS xps
# Cluster coherency mode
FULL CLUSTER CACHE COHERENCY
# Log deletion policy
LOG DELETE ENABLED
LOG DELETE POLICY: delete all eligible log files
# Database buffer pool size
cachesize 64.000 mb
# Cache hits and misses, page read/write information
hits        457
misses      50
page reads  102
page writes 108
hit rate     90.1%
tmp hit rate 94.8%
# Version information (baked into binary at build time)
@(#)plink [comdb2] MAKEFILE: Makefile
version: unknown version
Codename:      "R6.1"

```

### stat osql

Dump currently active SQL transaction list.

### stat net

Display network information for the [replication network](config_files.html#networks).

```
cmd:'stat net'
host        xps:19000 fd 22  cnt_thr rd_thd wr_thd hello
  enque count 0     peak 20     at 03/29 19:05:21 (hit max 0 times)
  enque bytes 0     peak 3253     at 03/29 19:05:21
host        xps2:19001 fd 23  cnt_thr rd_thd wr_thd hello
  enque count 0     peak 21     at 03/29 19:05:21 (hit max 0 times)
  enque bytes 0     peak 4422     at 03/29 19:05:21
```

This displays a list of nodes in the cluster, their port, and a list of flags (in the case above
the flags are a list of threads active for the node).  For healthy nodes this will include the reader,
writer and connection thread. 'hello' indicates that we received a greeting from the node and it identified
itself as part of the cluster.

Lines below each host list the number of messages and bytes, respectively, outstanding to that node.  The peak
numbers indicate what the highest recorded value was, and at what time it was hit.  Finally, we list how many
times we needed to enqueue a message, but the net queue was full.

### stat osqlnet

Same as [net](#stat-net) but for the SQL network.

### stat signalnet

Same as [net](#stat-net) but for the signal network.

### stat rep

Display basic replication stats

### stat appsock

Displays stats about connection information

### stat compr

Display information about compression methods set on various tables and a few other table-wide settings.

### stat dumpsql

Display currently running SQL statements. Display includes the query start time, the originating machine, the
text of the query, the paths taken by the query, and the number of steps taken through those paths.

```
id 601735 03/30/2017 11:08:43 localhost select a from t1
  cursor on table t1 nmove 0 nfind 1 nwrite 0
```

### stat csc2vers

Display table version numbers.  A schema change will bump the version number.

### stat dumpcsc

Takes a table name and a version number as argument.  Dumps the schema for that table at the specified version number.

### stat rmtpol

Takes a machine name as the argument. Reports a policy, if any, for that machine.  See [allow commands](config_files.html#allowdisallow-commands)
for details.


### stat size

For each table in the database, displays current size information.  Displays table size, size of index/data files,
and what percentage of the total database size each table accounts for.  Also displays information about space taken
by database logs.

### stat reql

See [request logging](#reql).

### stat switch

Display status of toggl-eable on/off switches.

### stat clnt

Dumps client source information.  For every machine that contacted the database, display a count of requests.

### stat ixstat

Display index usage information.

### stat sc

Display information about a currently running schema change, if any.

### stat analyze

Display information about currently running ANALYZE SQL command, if any.

### stat iopool

Display information about the thread pool responsible for flushing dirty pages to disk.

### stat reqrates

Display information about request rates over the last minute, hour, and since startup.

### stat keycompr

Display key compression information.

### stat rcache

Display root page cache information.

### Other stats

Various other commands can also be run prepended with stat: [stax](#stax), [long](#long), [stal](#stal), [thr](#thr), [dmpl](#dmpl).

## Request logging

## BDB commands

BDB commands 

### bdb cachestat

Display buffer pool information from BerkeleyDB

### bdb cacheinfo

Display buffer pool buckets

### bdb cachestatall

Display per-file buffer-pool statistics.

### bdb repstat

Display replication statistics.

### bdb logstat

Display logging statistics

### bdb lockstat

Display locking statistics

### bdb lockinfo lockers

Show lock status, grouped by lockers.

### bdb lockinfo locks

Show lock status, grouped by locks.

### bdb activelocks

Show lock status, active locks only.

### bdb dumpcache 

More verbose buffer pool information.

### bdb txnstat

Display transaction statistics.

### bdb ltranstat

Display logical transaction statistics (logical transactions are used when rowlocks are enabled)

### bdb temptable

Display information about temporary tables.

### bdb dblist

Display information about open files

### bdb curlist

Display information about open cursors

### bdb attr

Dump all [bdb-level tunables](config_files.html#bdbattr-tunables).

### bdb setattr

Modify a [bdb-level tunable](config_files.html#bdbattr-tunables).

### bdb memdump

Show memory usage information for BerkeleyDB regions.

### bdb repworkers

This is a thread pool for replication workers.  Append any of the standard [thread pool](config_files.html#thread-pools)
commands.

### bdb lcccache stat

Show information about cached log records (used on replicants to speed up replication)

### bdb reptimes

Display rolling average of replication times over the last 10 seconds and 1 minute (master only)

