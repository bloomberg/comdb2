---
title: Configuration files
tags:
sidebar: mydoc_sidebar
permalink: config_files.html
---

## LRL files

Comdb2 configuration files have an .lrl extension for historic reasons.  No configuration file is required.  If no file is 
specified, the database will pick reasonable defaults.  On startup, the database will look for the following files, and 
read them, in order:
1. `$COMDB2_ROOT/etc/cdb2/config/comdb2.lrl`
1. `$COMDB2_ROOT/etc/cdb2/config/comdb2_local.lrl`
1. `$COMDB2_ROOT/etc/cdb2/config.d/*.lrl` 
1. `dbname.lrl` (for your database's name)

`COMDB2_ROOT` is set at compile time (defaults to `/opt/bb/`).  Setting a `$COMDB2_ROOT` environment variable overrides
the default value.

The intent behind multiple config files is to make deployment of settings easier. `comdb2.lrl` is meant to contain settings
that are identical across many databases in your application.  `comdb2_local.lrl` is for settings that are local to this
machine.  Files under `config.d` are useful for organizing settings in multiple files (the 'include' directive in .lrl files
can also accomplish this).

The last file read is the database specific lrl file.  This contains settings specific to a given database.  Comdb2 will look
for this file in one of these locations, in this order:

1. Current directory
1. In database directory if it's specified with `--dir /path/to/db` on command line
1. In the default database location, if the database directory isn't specified
1. In the location specified with '--lrl /path/to/dbname.lrl` on command line

Lines beginning with '#' are treated as comments and skipped.  Other lines should consist of a directive and its value.

## Database tunables.

### Cache size

Cache size can be set with the the 'cache' tunable. It takes a cache size and a unit (kb, mb, gb).  For example

    cache 64 mb

This gets you 64 megabytes of cache.  The database will print its cache size when starting up:

    Cache:64MB  Segments:1  Segment-size:64MB

The default cache size is 64MB.  You can set policies on a machine by capping the min and max settings for cachesize (putting
these in global comdb2.lrl or comdb2_local.lrl is a good idea.  For example to set the minimum cache size at 8MB, and restrict
the maximum at 1GB (say, for a shared machine), one can have:

    cachekbmin 8192
    cachekbmax 1048576

Maximum and minimum values are given in KB.

### Thread Pools

There are various threadpools in Comdb2 responsible for specific work.  They all take a common set of options.  

|Name                   |Function                              |Default size    |Default max queue depth|
|-----------------------|--------------------------------------|----------------|-----------------------|
|appsockpool            |Pool for application connections      |Unlimited       |0                      |
|sqlenginepool          |Pool for sql runner threads           |48              |500                    |
|iopool                 |Cache flusher threads                 |4               |8000                   |

The following options are available for configuring thread pool parameters:

|Parameters             |Function
|-----------------------|---------
|mint                   |Minimum number of threads to keep around.  Threads above this value will exit after `linger` seconds.  Raise this if the thread pool reports lots of thread creates.
|maxt                   |Maximum number of threads to keep around.  Lower this you don't get gains from additional concurrency for the specific subsystem.
|linger                 |How long threads above `mint` will hang around (in seconds) before exiting.
|maxagems               |Max age of an item on a queue in ms.  Items older than given will be dropped.
|exit_on_error          |If set (argument is `on`), database will exit if it can't spin up new threads for this threadpool.
|dump_on_full           |If set, argument is `on`) will dump the current state of the threadpool when the queue is full
|maxq                   |Maximum queue depth.  If `maxt` threads are active and none are available, items are enqueued.  If the queue reaches this depth, requests to enqueue further are dropped.
|maxqover               |Maximum queue override depth.  Queued items below this limit won't generate warnings.

Examples:

To limit the number of concurrent SQL threads for this node to 12:

`sqlenginepool maxt 12`

To dump the SQL thread pool every time the queue is full:

`sqlenginepool maxq dump_on_error on`

### Networks

There are a few references to various "networks" in the descriptions below.  All machines specified by the 
`cluster nodes` tunable (or added dynamically) form connections.  There is 2 dedicated threads for each machine
for reading and writing messages.  There's 3 "networks" formed for each database (so 3 sets of connections).
Each network has its own set of tunables.  Messages are queued for each destination, and are flushed when
necessary.  All nodes periodically send heartbeats.  Lack of a heartbeat is treated like a network drop.
Network tunables for controlling heartbeat parameters, queue sizes, etc. are listed below.  The following 
sections describe each network's uses.

|Option              |Default              |Description
|--------------------|---------------------|------------
|netbufsz | 1048576 (1 MB) | Size of the network buffer (per node) for the replication network
|netbufsz_signal | 65536 | Size of the network buffer (per node) for the signal network
|heartbeat_check_time | 10 (seconds) | Consider an error if no heartbeat for this many seconds
|net_max_queue                    |25000       | Maximum number of items to keep on replication network queue before dropping (per replicant)
|nax_max_mem                      |0 (not set) | Maximum size (in MB) of items keep on replication network queue before dropping (per replicant)
|net_throttle_percent             |50          | Throttles write requests from replicants if the replication network queue is `net_throttle_percent` full
|net_max_queue_signal             |100         | Maximum number of items to keep on the signal network queue before dropping (per replicant)
|net_poll                         |100 ms      | Allow a connection to linger for this many ms before identifying itself. Connections that take longer are shut down.
|osql_net_poll                    |100 ms      | Like `net_sql`, but for the offload network (used by write transactions on replicants to send work to the master)
|net_portmux_register_interval    |600 ms      | Check on this interval if our port is correctly registered with pmux for the replication net
|signal_net_portmux_register_interval | 600 ms | Like `osql_net_poll` for the signal network
|osql_net_portmux_register_interval | 600 ms   | like `net_portmux_register_interval`
|osql_max_queue | 25000 | Like `net_max_queue` for offload net
|osql_bkoff_netsend | 100 ms | On a full offload net queue, attempt to wait this long before attempting to resend
|osql_bkoff_netsend_lmt | 300000 | Wait a total of this many ms attempting to send on the offload net
|toblock_net_throttle | not set | If set, will throttle writes on a full network queue
|no_toblock_net_throttle | | Disables no_toblock_net_throttle
|enque_flush_interval | 1000 | Try to flush network queue after this many writes for the replication net
|enque_flush_interval_signal | 1000 | Try to flush network queue after this many writes for the signal net
|enque_reorder_lookahead | 20 | When messages are sent out of order, peek at this many messages on the queue in attempt to reorder
|osql_heartbeat_send_time | 5 (sec) | Like heartbeat_send_time for the offload network
|osql_heartbeat_alert_time | 10 (sec) | Like heartbeat_check_time for the offload network
|net_explicit_flush_trace | not set | Produce a stack dump for long network flushes 
|no_net_explicit_flush_trace | | Turns off stack dumps for long network flushes
|udp | set | Transaction acks are sent back to master via UDP.  Since UDP is potentially lossy, replicants will inject the current LSN ack into their TCP channel to the master every 500 ms.  On a lossy network, if you see lots of 500ms transactions, you may want to disable UDP.  Such cases aren't typical.
|noudp | | Disables `udp`.

#### Replication

The replication network handles replication-specific traffic between database nodes.  This includes
election messages, log messages (ie: normal replication), requests for re-transmits, etc.  Steps 3 and 4
in [Transaction Life cycle](transaction_model.html#life-cycle-of-a-transaction) go over the replication network.

#### Offload

The offload net is used by replicants to send work to the master to apply.  Replicants run SQL and assemble
a set of record modifications to be applied.  Steps 2 and 5 in [Transaction Life cycle](transaction_model.html#life-cycle-of-a-transaction) 
go over the offload net.

#### Signal

The signal net is reserved for future use.

### Sync commands

These tunables control how often the database flushes its log to disk, and what nodes are required to acknowledge
a log entry before control is returned to the user.  All these tunables start with the 'sync' command, and the
following word determine which option is being set.  The default mode is to NOT flush logs on every transaction,
and to wait for all nodes to acknowledge a transaction before returning success.


|Option              |Default              |Description
|--------------------|---------------------|------------
|full                |not set              |Logs are flushed on every commit
|normal              |set                  |Logs are NOT flushed on every commit (ie: you can lose transactions if all the nodes in the cluster crash).  This is the default.  This option also implies fully synchronous replication - ie: wait for all nodes to ack before returning success.
|rep_always_wait     |not set              |Wait for all nodes, including those that aren't connected (not recommended)
|none                |not set              |Asynchronous replication, don't wait for any replication acknowledgments before returning success.
|room                |not set              |Wait for acknowledgments only from nodes in the same availability zone/data center (see [room affinity](clients.html#room)). 
|log-sync-time       |10 (seconds)         |Sets how often we flush the logs to disk.
|log-delete-now      |set                  |Make all log files eligible for deletion when not needed
|log-delete-before   |not set              |Make all log files older than current time are eligible for deletion when not needed
|log-delete-age-set  |                     |Takes epoch time in seconds, log files older than the given time are eligible for deletion when not needed
|log-delete          |on                   |`on` enables log deletion, `off` disables it


### Allow/Disallow commands

These commands control machine class permissions.  For every incoming connection, Comdb2 will try to read its class
from comdb2db once.  `allow` and `disallow` can be used to set up policies for which classes of machines are allowed
to access databases.  For example, you may have a blanket rule that production databases can only be accessed by
production machines, etc.  Commands have this syntax:

    allow|disallow writes|cluster|broadcast [from] class|machine

Class should be one of 'dev', 'alpha', 'beta', 'prod'.  Any other value is assumed to be a machine name.

`writes` allows/disallows transactions that change the state of the database from the given source. `cluster`
allows/disallows forming clusters with machines from the given source. `broadcast` allows/disallows consumer 
connections from the given source.

### Switches

Switches are simple on/off settings that generally control debugging or experimental features.  They can
be enabled with the `on` option, and disabled with `off`.  Unless debugging/developing Comdb2, toggling these
is generally not advised.

|Option      |Default             |Description
|------------|--------------------|------------
bad_lrl_fatal|  off |Unrecognized lrl options are fatal errors
t2t|  off |New tag->tag conversion code
fix_cstr|  on |Fix validation of cstrings
warn_cstr|  on |Warn on validation of cstrings
scpushlogs|  on |Push to next log after a schema changes
pfltverbose|  on |Verbose errors in prefaulting code
plannedsc|  on |Use planned schema change by default
pflt_toblock_lcl|  on |Prefault toblock operations locally
pflt_toblock_rep|  on |Prefault toblock operations on replicants
dflt_livesc|  on |Use live schema change by default
dflt_plansc|  on |Use planned schema change by default
consumer_rtcpu|  on |Don't send update broadcasts to machines that are marked offline
sqlite3openserial|  on |Serialize calls to sqlite3_open to prevent excess CPU
thread_stats|  on |Berkeley DB will keep stats on what its threads are doing
lock_timing|  on |Berkeley DB will keep stats on time spent waiting for locks
memp_timing|  off |Berkeley DB will keep stats on time spent in __memp_fget
memp_pg_timing|  on |Berkeley DB will keep stats on time spent in __memp_pg
shalloc_timing|  on |Berkeley DB will keep stats on time 
reset_queue_cursor_mode|  on |Reset queue consumeer read cursor after each consume
key_updates|  on |Update non-dupe keys instead of delete/add
emptystrnum|  on |Empty strings don't convert to numbers
schemachange_perms|  on |Check if schema change allowed from source machines
verifylsn|  on |Verify if LSN written before writing page
allow_broken_datetimes|  on |Allow broken datetimes
verify_directio|  off |Run expensive checks on directio calls
parallel_sync|  on |Run checkpoint/memptrickle code with parallel writes
verify_dbreg|  off |Periodically check if dbreg entries are correct
verifycheckpoints|  off |Highly paranoid checkpoint validity checks
support_datetime_in_triggers|  on |Enable support for datetime/interval types in triggers
prefix_foreign_keys|  on |Allow foreign key to be a prefix of your key
superset_foreign_keys|  on |Allow foreign key to be a superset of your key
repverifyrecs|  off |Verify every berkeley log record received
enable_osql_logging|  off |Log every osql packet received in a special file, per iq
enable_osql_longreq_logging|  off |Log untruncated osql strings
check_sparse_files|  off |When allocating a page, check that we aren't creating a sparse file
core_on_sparse_file|  off |Generate a core if we catch berkeley creating a sparse file
cache_sockpool_tran|  on |Cache sent rows for an sql transaction, and survive a master swing
sqlclient_use_random_readnode|  off |Sql client will use random sql allocation by default 
check_sqlite_numeric_types|  on |Report if our numeric conversion disagrees with SQLite's
use_fastseed_for_comdb2_seqno|  off |Use fastseed instead of context for comdb2_seqno unique values
disable_stable_for_ipu|  on |For inplace update tables, disable stable find-next cursors
debug_mpalloc_size|  off |Alarm on suspicious allocation requests;
disable_exit_on_thread_error | off | don't exit on thread errors
catch_response_on_retry|  on |print trace when we try to send replies on a retry
requeue_on_tran_dispatch|  on |Requeue transactional statement if not enough threads
check_wrong_db|  on |Return error if connecting to wrong database
dbglog_use_sockpool|  on |Use sockpool for connections opened for dbglog
debug_temp_tables|  off |Debug temp tables
check_sql_source|  off |Check sql source
flush_check_active_peer|  on |Check if still have active connection when trying to flush
private_blkseq|  on |Keep a private blkseq
use_blkseq|  on |Enable blkseq
track_queue_time|  on |Track time sql requests spend on queue
update_startlsn_printstep|  off |Print steps walked in update_startlsn code
locks_check_waiters|  off |Light a flag if a lockid has waiters
rowlocks_commit_on_waiters|  off |Don't commit a physical transaction unless there are lock waiters
broadcast_check_rmtpol|  on |Check rmtpol before sending triggers
track_curtran_locks|  off |Print curtran lockinfo
print_deadlock_cycles|  off |Print all deadlock cycles
replicate_rowlocks|  on |Replicate rowlocks
gather_rowlocks_on_replicant|  on |Replicant will gather rowlocks
force_old_cursors|  off |Replicant will use old cursors
disable_rowlocks_logging|  off |Don't add logical logging for rowlocks
disable_rowlocks|  off |Follow rowlocks codepath but don't lock
random_rowlocks|  off |Grab random, guaranteed non-conflicting rowlocks
already_aborted_trace|  off |Print trace when dd_abort skips an 'already-aborted' locker
disable_update_shadows|  off |stub out update shadows code
verbose_toblock_backouts|  off |print verbose toblock backout trace
sql_release_locks_on_si_lockwait|  on |Release sql locks from si if the rep thread is waiting
sql_release_locks_on_emit_row_lockwait|  off |Release sql locks when we are about to emit a row
sql_release_locks_on_slow_reader|  on |Release sql locks if a tcp write to the client blocks
no_timeouts_on_release_locks|  on |Disable client-timeouts if we're releasing locks
sql_release_locks_in_update_shadows|  on |Release sql locks in update_shadows on lockwait
release_locks_trace|  off |Print trace if we release locks
verbose_waiter_flag|  off |Print trace setting the waiter flag in lock code
dump_locks_on_repwait|  off |Dump locks on repwaits
dump_page_on_byteswap_error|  off |fsnap a malformed page from byteswap
dump_after_byteswap|  off |dump page after byteswap
rl_retry_on_deadlock|  on |retry micro commit on deadlock
disable_blob_check|  off |return immediately in check_blob_buffers
disable_new_si_overhead|  off |return immediately in several new snapisol functions
verify_all_pools|  off |verify objects are returned to the correct pools
print_blockp_stats|  off |print thread-count in block processor
allow_parallel_rep_on_pagesplit|  on |allow parallel rep on pgsplit
allow_parallel_rep_on_prefix|  on |allow parallel rep on bam_prefix
verbose_net|  off |Net prints lots of messages
only_match_on_commit|  off |Only rep_verify_match on commit records
check_page_in_recovery|  off |verify that a page has or hasn't gotten corrupt
comptxn_inherit_locks|  on |Compensating transactions inherit pagelocks
rep_printlock|  off |Print locks in rep commit
accept_on_child_nets|  off |listen on separate port for osql/signal nets
disable_etc_services_lookup|  off |When on, disables using /etc/services first to resolve ports
rowlocks_deadlock_trace|off |Prints deadlock trace in phys.c

#### `sqllogger` commands

There's a detailed SQL logging system in place.  It allows fine-grained data collection on queries.  This
can be controlled with the `sqllogger` commands.  Logs themselves are binary files that are dumped into
the database directory with the name $DBNAME.sqllog.  When rolled, they before $DBNAME.sqllog.1,
$DBNAME.sqllog.2, etc.

|sql logger command| description
|------------------|-------------
|sqllogger on||Enables sql logging
|sqllogger rollat|not set|Automatically roll logs when they get to this size (in bytes)
|sqllogger keep|2|Keep this many copies of rolled logs
|sqllogger every|1|Log every Nth statement (1 means every), occasionally good for sampling queries
|sqllogger async|1|Log asynchronously - don't write to file in the same thread as SQL query
|sqllogger sync|1|Log synchronously - write to file in the same thread as SQL query
|sqllogger asyncsize|4194304|For async logging, the max size of unflushed query data

### Incoherent nodes overview

One problem with synchronous replication is that commit time is dominated by the slowest node.  What's worse,
a node that stops responding altogether can hold up replication forever.  To avoid these problems, the master
may choose to mark nodes ***incoherent***.  Incoherent nodes continue to operate as before with 2 exceptions:

   * They will no longer service any requests - any new queries/transactions that come in are rejected and
     will go to another node.
   * The master will no longer wait for replication acknowledgments from incoherent nodes.

There's 2 ways for a node to be marked incoherent.  The first catches outright broken nodes.  If any nodes
have acknowledged a transaction, and any other node takes longer than a configured threshold to respond, it's
considered broken and marked incoherent.  The second option captures nodes that are statistically slower than
other nodes.  A node where the running average of acknowledgment times over the last minute twice as bad as 
the second bad node (withing some minimum threshold) is considered slow, and will become incoherent.

Incoherent nodes are self-healing.  If a node marked incoherent catches up to the master, the master will
mark it coherent again, will resume waiting for replication, and the node will be free to service requests.  If
a node was marked slow (second method, listed above), it will become coherent once its running average is back
within the configured threshold.

The tunables in this section control the various tunables used to decide when nodes are to be considered 
incoherent

DB layer tunables:

|Option              | Default        | Description
|--------------------|----------------|-------------
|incoherent_msg_freq | 3600 (seconds) | Drop a warning file if a node is incoherent (see [incoherent nodes overview](#incoherent-nodes-overview)) for more information.
|incoherent_alarm_time | 120 (seconds) | Warn about incoherent nodes this often.
|max_incoherent_nodes | 1 | Number of incoherent nodes before an alarm triggers.
|net_lmt_upd_incoherent_nodes | 70 (%)| When this percent of a network queue is full, throttle messages to incoherent nodes.

BDB layer tunables (see [bdbattr tunables](#bdbattr-tunables))

|Option              | Default        | Description
|--------------------|----------------|---------------
|REPTIMEOUT_LAG | 50 (PERCENT) | Used in replication.  Once a node has received our update, we will wait REPTIMEOUT_LAG% of the time that took for all other nodes.  
|REPTIMEOUT_MINMS | 10000 (MSECS) | Even if the first node comes back quickly, wait at least this many ms to replicate to other nodes.
|REPTIMEOUT_MAXMS | 5 * 60 * 1000 (MSECS) | We should wait this long for one node to acknowledge replication.  If after this time we have failed to replicate anywhere then the entire cluster is incoherent!
|REP_DEBUG_DELAY | 0 (MSECS) | For debugging - set an artificial replication delay
|TOOMANYSKIPPED | 2 (QUANTITY) | Call for election again and delay commits if more than this many nodes are incoherent
|SKIPDELAYBASE | 100 (MSECS) | Delay commits by at least this much if forced to delay by incoherent nodes
|REPMETHODMAXSLEEP | 300 (SECS) | Delay commits by at most this much if forced to delay by incoherent nodes

### `sql_tranlevel_default` options

These options allow you to set the default SQL transaction level across the database.  Any setting here can
be overwritten by running 'SET TRANSACTION ...' after connecting to the database.  See [Isolation Levels](transaction_model.html#isolation-levels-and-artifacts) for available transaction levels. 

|sql_tranlevel option | description
|---------------------|-------------
|sql_tranlevel_default default|Set to `default` isolation level
|sql_tranlevel_default recom|Set to `READ COMMITTED` isolation level
|sql_tranlevel_default snapshot|Set to `SNAPSHOT` isolation level
|sql_tranlevel_default serial|Set to `SERIALIZABLE` isolation level

### Query limit commands

Queries running in the database can be limited in cost.  The database can optionally warn when a query reaches
a certain cost, or prevent it from running further.

|querylimit option | description
|---------------------|-------------
|querylimit maxcost N|Set the maximum cost the database will allow for a query to N|
|querylimit warn maxcost N|Warn if a query has a cost > N | 
|querylimit maxcost off|Turn off returning errors for queries over a cost limit
|querylimit warn maxcost off|Turn off warnings for queries over a cost limit

### Decimal rounding options

The rounding policy for rounding for columns of decimal types can be controlled with the `decimal_rounding` option. 
Available options are:

|decimal_rounding option | description
|------------------------|-------------
decimal_rounding DEC_ROUND_NONE|Don't round
decimal_rounding DEC_ROUND_CEILING|Round towards +infinity 
decimal_rounding DEC_ROUND_UP|Round away from 0               
decimal_rounding DEC_ROUND_HALF_UP|0.5 rounds up                   
decimal_rounding DEC_ROUND_HALF_EVEN|0.5 rounds to nearest even (default)
decimal_rounding DEC_ROUND_HALF_DOWN|0.5 rounds down                 
decimal_rounding DEC_ROUND_DOWN|Round towards 0 (truncate)      
decimal_rounding DEC_ROUND_FLOOR|Round towards -infinity         
decimal_rounding DEC_ROUND_05UP|Round for reround               


### `berkattr` tunables

There's a few low-level tunables that control options in BerkeleyDB subsystems present in Comdb2.

All options below are preceded with "berkattr" in the configuration file.

|option|Default|Description
|------|-------|-----------
iomap_enabled| 1 |Map file that tells comdb2ar to pause while we fsync
flush_scan_dbs_first| 0 |Don't hold bufpool mutex while opening files for flush
skip_sync_if_direct| 1 |Don't fsync files if directio enabled
warn_on_replicant_log_write| 1 |Warn if replicant is writing to logs
abort_on_replicant_log_write | 0 |Abort if replicant is writing to logs
debug_deadlock_replicant_percent | 0 |Percent of replicant events getting deadlocks
always_run_recovery| 1 |Replicant always runs recovery after rep_verify
debug_addrem_dbregs| 0 |Generate debug records for addrems
start_recovery_at_dbregs| 1 |Start recovery at dbregs
apprec_track_lsn_ranges| 1 |During recovery track lsn ranges
dbreg_errors_fatal| 0 |dbreg errors fatal
warn_nondbreg_records| 0 |warn on non-dbreg records before checkpoint
num_write_retries| 8 |number of times to retry writes on ENOSPC
debug_enospc_chance| 0 |DEBUG %% random ENOSPC on writes
check_zero_lsn_writes| 1 |Warn on writing pages with zero LSNs
abort_zero_lsn_writes| 0 |Abort on writing pages with zero headers
abort_zero_lsn_memp_put| 0 |Abort on memp_fput pages with zero headers
preallocate_on_writes| 0 |Pre-allocate on writes
preallocate_max| 256 * MEGABYTE |Pre-allocation size
lsnerr_pgdump| 1 |Dump page on LSN errors
lsnerr_pgdump_all| 0 |Dump page on LSN errors on all nodes
max_backout_seconds| 0 |Refuse to roll back replicant past this many seconds
max_backout_logs| 3 |Refuse to roll back replicant past this many log files
log_applied_lsns| 0 |Log applied LSNs to log
check_applied_lsns| 0 |Check transaction that its LSNs have been applied
check_applied_lsns_fatal| 0 |Abort if check_applied_lsns fails
check_applied_lsns_debug| 0 |Lots of verbose trace for debugging applied LSNs
sgio_enabled| 0 |Do scatter gather I/O
sgio_max| 10 * MEGABYTE |Max scatter gather I/O to do at one time
btpf_enabled| 0 |Enables index pages read ahead
btpf_wndw_min| 100  |Minimum number of pages read ahead
btpf_wndw_max| 1000  |Maximum number of pages read ahead
btpf_wndw_inc| 1 |Increment factor for the number of pages read ahead
btpf_pg_gap| 0 |Min. number of records to the page limit before read ahead
btpf_cu_gap| 5 |How close a cursor should be (pages) to the prefaulted limit before prefaulting again
btpf_min_th| 1 |Preload pages only if the tree has height less than this parameter
recovery_verify| 0 |After recovery, run a full pass to make sure everything is applied 
recovery_verify_fatal| 0 |Abort if recovery_verify is set, and fails. 
check_pwrites| 0 |Read page after direct pwrite, check that it matches 
check_pwrites_debug| 0 |Read page after direct pwrite, check that it matches 
cache_lc| 0 |Collect logs into LSN_COLLECTIONs as they come in 
cache_lc_max| 16 |Keep this many transactions around in LC cache 
cache_lc_debug| 0 |Lots of verbose messages out of LC cache system 
cache_lc_trace_evictions| 0 |Print a message at the point of eviction 
cache_lc_trace_misses| 0 |Print a message on cache miss 
cache_lc_check| 0 |Check LC cache system on every transaction 
cache_lc_memlimit| 2097152 |Limit total memory used by LC cache (0 = unlimited). 
cache_lc_memlimit_tran| 1048576 |Limit per transaction memory used by LC cache 
consolidate_dbreg_ranges| 1 |Combine adjacent dbreg ranges for same file 
max_latch| 200000 |Size of latch array 
max_latch_lockerid| 10000 |Size of latch lockerid array 
lockerid_node_step| 128 |Stepup for preallocated lids 
ilock_step| 2048 |Stepup for preallocated ilock-latches 
db_lock_lsn_step| 1024 |Stepup for preallocated db_lock_lsns 
blocking_latches| 0 |Block on latch rather than deadlock 
latch_max_wait| 5000 |Block at most this many microseconds before returning deadlock 
latch_poll_us| 1000 |Poll latch this many microseconds before retrying 
latch_max_poll| 5 |Poll latch this many times before returning deadlock 
latch_timed_mutex| 1 |Use a timed mutex 
log_cursor_cache| 0 |Cache log cursors 
recovery_processor_poll_interval_us| 1000 |Recovery processor wakes this often to check workers 
lsnerr_logflush| 1 |Flush log on lsn error 
tracked_locklist_init| 10 |Initial allocation count for tracked locks 


### `bdbattr` tunables

These tunables control the 'BDB' layer of the db.  Its main responsibility is setting tunables that control
lower-level functions like replication, various timeouts, etc.  These are set with the 'setattr' tunable.  The
next word determines which option to set, and the following word determines its value.

|Option          |Default (type)      |Description
|----------------|--------------------|-----------
|REPTIMEOUT|20 (SECS) | Replication timeout.
|CHECKPOINTTIME|60 (SECS) |  Write a checkpoint at this interval.
|CHECKPOINTTIMEPOLL|100 (MSECS) | Poll a random amount of time < this before writing a checkpoint (attempting to prevent multiple databases from checkpointing at the same exact time)
|NUMBERKDBCACHES|0 (QUANTITY) | Split the cache into this many segments.
|FULLRECOVERY|0 (BOOLEAN) | Instead of recovering from the last checkpoint, run recover from the start of the available logs.
|DTASTRIPE|8 (QUANTITY) | Partition each table's data into this many stripes.  Note that this is ONLY settable at database creation time.
|COMMITDELAY|0 (MSECS) | Add a delay after every commit.  This is occasionally useful to throttle the transaction rate.
|REPSLEEP|0 (MSECS) | Add a delay on replicants before completing processing a log record.  
|MINREPTIMEOUT|10000 (MSECS) | Wait at leas this long for a replication event before marking a node incoherent (see [incoherent nodes overview](#incoherent-nodes-overview))
|BLOBSTRIPE|0 (BOOLEAN) | Settable at ONLY at database creation time.  Create stripes for every variable length field (eg: blobs)
|SQLITE_SORTER_TEMPDIR_REQFREE|6 (PERCENT) | Refuse to create a sorter for queries if less than this percent of disk space is available (and return an error to the application)
|MEMPTRICKLEPERCENT|99 (PERCENT) | Try to keep at least this percentage of the buffer pool clean.  Write pages periodically until that's achieved.
|MEMPTRICKLEMSECS|1000 (MSECS) | Pause for this many ms between runs of the cache flusher.
|LITTLE_ENDIAN_BTREES|1 (BOOLEAN) | Enabling this sets byte ordering for pages to little endian
|COMMITDELAYMAX|0 (QUANTITY) | Caps the max transaction delay time.  `COMMITDELAY` won't set above this value, unless set manually.
|LOWDISKTHRESHOLD |95 (PERCENT) | Sets the low headroom threshold (percent of filesystem full) above which Comdb2 will start removing logs against set policy.
|SQLBULKSZ | 2097152 (BYTES) | For index/data scans, the database will retrieve data in bulk instead of singlestepping a cursor.  This set the buffer size for the bulk retrieval.
|ZLIBLEVEL |  6 (QUANTITY) | If zlib compression is enabled, this determines the compression level.
|AUTODEADLOCKDETECT |  1 (BOOLEAN) | When enabled, deadlock detection will run on every lock conflict.  When disabled, it'll run periodically (every DEADLOCKDETECTMS ms)
|DEADLOCKDETECTMS |  100 (MSECS) | When automatic deadlock detection is disabled, run the deadlock detector this often.
|LOGSEGMENTS |  1 (QUANTITY) | Changing this can create multiple logfile segments.  Multiple segments can allow the log to be written while other segments are being flushed.
|REPLIMIT | 256 * 1024 (BYTES) | Replication messages will be limited to this size
|REP_LONGREQ | 1 (SECS) | Warn if replication events are taking this long to process.
|TEMPTABLE_MEM_THRESHOLD | 512 (QUANTITY) | If in-memory temp tables contain more than this many entries, spill them to disk.
|TEMPTABLE_CACHESZ | 262144 (BYTES) | Cache size for temporary tables. Temp tables do not share the database's main buffer pool.
|BULK_SQL_MODE | 1 (BOOLEAN) | Enable reading data in bulk when performing a scan (alternative is single-stepping a cursor)
|ROWLOCKS_PAGELOCK_OPTIMIZATION|1 (BOOLEAN) | Upgrade rowlocks to pagelocks if possible on cursor traversals.
|ELECTTIMEBASE|50 (MSECS) | Master election timeout base value
|BULK_SQL_THRESHOLD|2 (QUANTITY) | Use bulk retrieval of data on scan after this many next operations
|SQL_QUERY_IGNORE_NEWER_UPDATES|0 (BOOLEAN) | In transaction modes below SNAPSHOT, skip records updated after the current transaction started.
|CHECK_LOCKER_LOCKS|0 (BOOLEAN) | Sanity check locks at end of transaction 
|DEADLOCK_MOST_WRITES|0 (BOOLEAN) | If AUTODEADLOCKDETECT is off, prefer transaction with most write as deadlock victim
|DEADLOCK_WRITERS_WITH_LEAST_WRITES|1 (BOOLEAN) | If AUTODEADLOCKDETECT is off, prefer transaction with least write as deadlock victim
|DEADLOCK_YOUNGEST_EVER|0 (BOOLEAN) | If AUTODEADLOCKDETECT is off, prefer youngest transaction as deadlock victim
|DEADLOCK_LEAST_WRITES_EVER|1 (BOOLEAN) | If AUTODEADLOCKDETECT is off, prefer transaction with least write as deadlock victim
|DISABLE_WRITER_PENALTY_DEADLOCK|0 (BOOLEAN) | If set, won't shrink max #writers on deadlock
|CHECKPOINTRAND|30 (SECS) | Stagger scheduled checkpoints by this random amount within this many seconds (to prevent multiple databases from checkpointing at the same time)
|DEF_MIN_KEEP_LOGS|5 (QUANTITY) | Keep at least this many logs around, even if they are eligible for deletion (unless disk space is very low)
|MIN_KEEP_LOGS_AGE|0 (SECS) | Keep logs that are at least this old (0 - do not keep logs based on time)
|LOG_DEBUG_CTRACE_THRESHOLD|20 (QUANTITY) | Limit trace about log file deletion to this many events
|DISABLE_UPDATE_STRIPE_CHANGE|1 (BOOLEAN) | Enable to move records between stripes on an update.
|PAGE_ORDER_TABLESCAN|1 (BOOLEAN) | Scan tables in order of pages, not in order of rowids (faster for non-sparse tables)
|TABLESCAN_CACHE_UTILIZATION|20 (PERCENT) |  Attempt to keep no more than this percentage of the buffer pool of table scans.
|INDEX_PRIORITY_BOOST|1 (BOOLEAN) | Treat index pages as higher priority in the buffer pool.
|REP_PROCESSORS|4 (QUANTITY) | Try to apply this many transactions in parallel in the replication stream
|REP_WORKERS|16 (QUANTITY) | Size of worker pool for applying page changes on behalf of transactions (only has effect when `REP_PROCESSORS` is set
|REP_LSN_CHAINING|0 (BOOLEAN) | If set, will force transactions on replicant to always release locks in LSN order.
|REP_MEMSIZE|524288 (QUANTITY) | Maximum size for a local copy of log records for transaction processors on replicants.  Larger transactions will read from the log directly.
|ELECT_DISABLE_NETSPLIT_PATCH|0 (BOOLEAN) | When false - on a net split, the side with the master keeps it instead of downgrading.  New masters still can't be elected without a quorum.
|OSYNC|0 (BOOLEAN) | Enable O_SYNC on writes.  Reads will still use filesystem cache.
|ALLOW_OFFLINE_UPGRADES|0 (BOOLEAN) | Allow machines marked offline to become master.
|MAX_VLOG_LSNS|10000000 (QUANTITY) | Apply up to this many replication record trying to maintain a snapshot transaction.
|PAGE_EXTENT_SIZE|0 (QUANTITY) | If set, allocate pages in blocks of this many (extents)
|DELAYED_OLDFILE_CLEANUP|1 (BOOLEAN) | If set, don't delete unused data/index files in the critical path of schema change - schedule them for deletion later.
|DISABLE_PAGEORDER_RECSZ_CHK|0 (BOOLEAN) | If set, allow page-order table scans even for larger record sizes where they don't necessarily lead to improvement.
|RECOVERY_PAGES|0 (QUANTITY) | Disabled if set to 0.  Othersize, number of pages to write in addition to writing datapages.  This works around corner recovery cases on questionable filesystems.
|REP_DB_PAGESIZE|0 (QUANTITY) | Page size for BerkeleyDB's replication cache db.
|PAGEDEADLOCK_RETRIES|500 (QUANTITY) | On a page deadlock, retry the page operation up to this many times.
|PAGEDEADLOCK_MAXPOLL|5 (QUANTITY) | If retrying on deadlock (see `PAGEDEADLOCK_RETRIES`), poll up to this many ms on each retry.
|ENABLE_TEMPTABLE_CLEAN_EXIT|0 (BOOLEAN) | On exit, clean up temp tables (they are deleted on next startup regardless).
|MAX_SQL_IDLE_TIME|3600 (QUANTITY) | Warn when an SQL connection remains idle for this long.
|SEQNUM_WAIT_INTERVAL|500 (QUANTITY) | Wake up to check the state of the world this often while waiting for replication acks.
|SOSQL_MAX_COMMIT_WAIT_SEC|600 (SECS) | Wait for the master to commit a transaction for up to this long 
|SOSQL_DDL_MAX_COMMIT_WAIT_SEC|259200 (SECS) | Wait for the master to commit a DDL transaction for up to this long 
|SOSQL_POKE_TIMEOUT_SEC|12 (QUANTITY) | On replicants, when checking on master for transaction status, retry the check after this many seconds.
|SOSQL_POKE_FREQ_SEC|5 (QUANTITY) | On replicants, check this often for transaction status.
|SQL_QUEUEING_DISABLE_TRACE|0 (BOOLEAN) | Disable trace when SQL requests are starting to queue.
|SQL_QUEUEING_CRITICAL_TRACE|100 (QUANTITY) | Produce trace when SQL request queue is this deep.
|NOMASTER_ALERT_SECONDS|60 (QUANTITY) | Replicants will alarm there's no master for this many seconds.
|PRINT_FLUSH_LOG_MSG|0 (BOOLEAN) | Produce trace when flushing log files.
|MASTER_REJECT_REQUESTS|1 (BOOLEAN) | Master will reject SQL requests - they'll be routed to a replicant.  The master can serve SQL requests, but it's better to avoid it for better workload balancing.
|NEW_MASTER_DUMMY_ADD_DELAY|5 (SECS) | Force a transaction after this delay, after becoming master.
|GENID_COMP_THRESHOLD|60 (QUANTITY) |  Try to compress rowids if the record data is smaller than this size.
|MASTER_REJECT_SQL_IGNORE_SANC|0 (BOOLEAN) | If `MASTER_REJECT_REQUESTS` is set, reject if no other connected nodes are available.
|KEEP_REFERENCED_FILES|1 (BOOLEAN) | Don't remove any files that may still be referenced by the logs.
|DISABLE_PGORDER_MIN_NEXTS|1000 (QUANTITY) | Don't disable page order table scans for tables less than this many pages.
|DISABLE_PGORDER_THRESHOLD|60 (PERCENT) | Disable page order table scans if skipping this percentage of pages on a scan
|HOSTILE_TAKEOVER_RETRIES|0 (QUANTITY) | Attempt to take over mastership if the master machine is marked offline, and the current machine is online.

#### Auto analyze options

These options control when the database kicks off [analyze](sql.html#analyze) automatically.  Auto-analyze
will be run by the database when the database does some fixed count of inserts/deletes. Various counters
can be queried with the `stat autonalyze` command.

|Option | Default (type) | Description
|-------|----------------|------------
|AUTOANALYZE|0 (BOOLEAN) | Set to enable auto-analyze
|AA_COUNT_UPD|0 (BOOLEAN) | Also consider updates towards the count of operations
|MIN_AA_OPS|100000 (QUANTITY) | Start analyze after this many operations
|CHK_AA_TIME|180 (SECS) | Check whether we should start analyze this often
|MIN_AA_TIME|7200 (SECS) | Don't re-run auto-analyze if already ran within this many seconds
|AA_LLMETA_SAVE_FREQ|1 (QUANTITY) | Persist change counters per table on every N'th iteration (called every `CHK_AA_TIME` seconds)
|AA_MIN_PERCENT|20 (QUANTITY) | Percent change above which we kick off analyze
|AA_MIN_PERCENT_JITTER|300 (QUANTITY) | Additional jitter factor for determining percent change. 

#### SQL planner tunables

|Option | Default (type) | Description
|-------|----------------|--------------
|PLANNER_EFFORT|1 (QUANTITY) | Planner effort (try harder) levels, default is 1
|SHOW_COST_IN_LONGREQ|1 (BOOLEAN) | Show query cost in the database long requests log (see [logs.html#long-requests-log](logs.html))
|PLANNER_SHOW_SCANSTATS|0 (BOOLEAN) | After each query, display statistics about index/data paths taken.
|PLANNER_WARN_ON_DISCREPANCY|0 (BOOLEAN) | After each query warn if the estimate and actual cost are significantly different

#### Schema change tunables

|Option | Default (type) | Description
|-------|----------------|------------
| SC_RESTART_SEC|60 (QUANTITY) | Delay restarting schema change for this many seconds after startup/new master election
|INDEXREBUILD_SAVE_EVERY_N|10 (QUANTITY) | Save schema change state to every n-th row for index only rebuilds
|SC_DECREASE_THRDS_ON_DEADLOCK|1 (BOOLEAN) | Decrease number of schema change threads on deadlock -- way to have schema change backoff
|SC_CHECK_LOCKWAITS_SEC|1 (QUANTITY) | Frequency of checking lockwaits during schemachange in seconds
|SC_USE_NUM_THREADS|0 (QUANTITY) | Start up to this many threads for parallel rebuilding durin schema change.  0 means use one per `dtastripe`.  Setting is capped at `dtastripe`.
|SC_NO_REBUILD_THR_SLEEP|10 (QUANTITY) | Sleep this many microsec when conversion threads count is at max
|SC_FORCE_DELAY|0 (BOOLEAN) | Force schemachange to delay after every record inserted -- to have sc backoff

#### UDP tunables

|UDP option | description
|-----------|-------------
|UDP_DROP_DELTA_THRESHOLD|10 (QUANTITY) | Warn if delta of dropped packets exceeds this threshold
|UDP_DROP_WARN_PERCENT|10 (PERCENT) | Warn only if percentage of dropped packets exceeds this
|UDP_DROP_WARN_TIME|300 (SECS) | No more than one warning per `UDP_DROP_WARN_TIME` seconds
|UDP_AVERAGE_OVER_EPOCHS|4 (QUANTITY) | Average over these many TCP epochs 
|RAND_UDP_FAILS|0 (QUANTITY) | For testing, rate of drop of UDP packets

#### Rowlock tunables

|Rowlocks option | description
|-----------|-------------
|MAX_ROWLOCKS_REPOSITION |  10 | Release a physical cursor an re-establish
|PHYSICAL_COMMIT_INTERVAL | 512 | Force a physical commit after this many physical operations
|ROWLOCKS_MICRO_COMMIT |  1 | Commit on every btree operation
|PHYSICAL_ACK_INTERVAL |  0 | For logical transactions, have the slave send an 'ack' after this many physical operations
|ACK_ON_REPLAG_THRESHOLD | 0 | Drop the undo interval to 1 after this many deadlocks

#### Misc tunables

|Misc option | description
|------------|-------------
|NET_INORDER_LOGPUTS | 1 | Attempt to order messages to ensure they go out in LSN order
|RCACHE_COUNT | 257 | Number of entries in root page cache
|RCACHE_PGSZ | 4096 | Size of pages in root page cache
|DISABLE_CACHING_STMT_WITH_FDB | 1 | Don't cache query plans for statements with foreign table references

#### Log configuration

|Log option | description
|-----------|-------------
|DEBUG_LOG_DELETION | 0 | Enable to see when/why database log files are being deleted
|LOG_DELETE_LOW_HEADROOM_BREAKTIME | 10 | Try to delete logs this many times if the filesystem is getting full before giving up
|MIN_KEEP_LOGS_AGE_HWM | 0 | ...
|LOGFILESIZE|41943040 (BYTES) | Attempt to keep each log file around this size.
|LOGFILEDELTA|4096 (BYTES) | Treat lsn differences less than this as "close enough" for the purpose of determining if a new node is in sync
|LOGMEMSIZE|10485760 (BYTES) | Use this much memory for a log file in-memory buffer.
|LOGDELETELOWFILENUM|-1 (QUANTITY) | Set to set the lowest deleteable log file number.

#### Replay detection tunables

Comdb2 databases allow the client APIs to replay a transaction if its outcome is uncertain (ie: client issues commit,
but the database drops a connection, so uncertain whether it committed).  This system is internally called "blkseq" (block
sequence).  Tunables to control it are below

|BLKSEQ option | description
|--------------|-------------
|PRIVATE_BLKSEQ_CACHESZ | 4194304 | Cache size of the blkseq table
|PRIVATE_BLKSEQ_MAXAGE | 20 | Maximum time in seconds to let "old" transactions live
|PRIVATE_BLKSEQ_STRIPES | 1 | Number of stripes for the blkseq table
|PRIVATE_BLKSEQ_ENABLED | 1 | Sets whether dupe detection is enabled
|PRIVATE_BLKSEQ_CLOSE_WARN_TIME | 100 | Warn when it takes longer than this many MS to roll a blkseq table

|DISABLE_SERVER_SOCKPOOL | 1 | Don't get connections to other databases from sockpool.
|TIMEOUT_SERVER_SOCKPOOL | 10 | Timeout for getting a connection to another database from sockpool.

#### Coherency options

Comdb2 will mark nodes that are non-responsive "incoherent".  Incoherent nodes will not respond to new queries and
replication will not wait for them acknowledge events.  Nodes stay incoherent until the are caught up to the rest of
the cluster.  The master node will send replicants a coherency lease to allow them to stay coherent.  It'll withhold the
lease if a node is either not responding or is significantly slower than the other nodes.

|coherency option | description
|-----------------|-------------
|COMMITDELAYBEHINDTHRESH | 1048576 (BYTES) | Call for election again and ask the master to delay commits if we're further than this far behind on startup.
|GOOSE_REPLICATION_FOR_INCOHERENT_NODES|0 (BOOLEAN) | Call for election for nodes affected by `COMMITDELAYBEHINDTHRESH`
|TRACK_REPLICATION_TIMES|1 (BOOLEAN) | Track how long each replicant takes to ack all transactions.
|WARN_SLOW_REPLICANTS|1 (BOOLEAN) | Warn if any replicant's average response times over the last 10 seconds are significantly worse than the second worst replicant's.
|MAKE_SLOW_REPLICANTS_INCOHERENT|1 (BOOLEAN) | Make slow replicants incoherent.
|SLOWREP_INCOHERENT_FACTOR|2 (QUANTITY) | Make replicants that are this many times worse than the second worst replicant incoherent.  This is the threshold for `WARN_SLOW_REPLICANTS` and `MAKE_SLOW_REPLICANTS_INCOHERENT`.
|SLOWREP_INCOHERENT_MINTIME|2 (MSECS) | Ignore replicantion events faster than this.
|SLOWREP_INACTIVE_TIMEOUT|5000 (SECS) | If a "slow" replicant hasn't responded in this long, mark him incoherent.
|TRACK_REPLICATION_TIMES_MAX_LSNS|50 (QUANTITY) | Track replication times for up to this many transactions
|REMOVE_COMMITDELAY_ON_COHERENT_CLUSTER | 1 | Stop delaying commits when the all the nodes in the cluster are coherent.
|COHERENCY_LEASE | 200 | A coherency lease grants a replicant the right to be coherent for this many MS.
|ADDITIONAL_DEFERMS | 0 | Wait-fudge to ensure that a replicant has gone incoherent
|COHERENCY_LEASE_UDP | 1 | Use udp to issue leases
|LEASE_RENEW_INTERVAL | 50 | How often we renew leases 
|DOWNGRADE_PENALTY | 10000 | Prevent upgrades for at least this many ms after a downgrade
|CATCHUP_WINDOW | 1000000 | Start waiting in waitforseqnum if replicant is within this many bytes of master
|CATCHUP_ON_COMMIT | 1 | Replicant to INCOHERENT_WAIT rather than INCOHERENT on commit if within CATCHUP_WINDOW 
|ADD_RECORD_INTERVAL | 1 | Add a record every <interval> seconds while there are incoherent_wait replicants

#### Misc. tunables

|miscellaneous option | description
|---------------------|-------------
|RLLIST_STEP | 10 | Reallocate rowlock lists in steps of this size.
|GENID48_WARN_THRESHOLD | 500000000 | Print a warning when there are only a few genids remaining */
|DISABLE_SELECTVONLY_TRAN_NOP | 0 | Disable verifying rows selected via SELECTV if there's no other actions done by the same transaction
|SC_VIA_DDL_ONLY | 0 | If DDL_ONLY is set, we don't do checks needed for comdb2sc 
|ASOF_THREAD_POLL_INTERVAL_MS | 500 | For how long should the BEGIN TRANSACTION AS OF thread sleep after draining its work queue
|ASOF_THREAD_DRAIN_LIMIT | 0 | How many entries at maximum should the BEGIN TRANSACTION AS OF thread drain per run
|REP_VERIFY_MAX_TIME | 300 | Maximum amount of time we allow a replicant to roll back its logs in an attempt to sync up to the master.
|REP_VERIFY_MIN_PROGRESS | 10485760 | Abort replicant if it doesn't make this much progress while rolling back logs to sync up to master.
|REP_VERIFY_LIMIT_ENABLED | 1 | Enable aborting replicant if it doesn't make sufficient progress while rolling back logs to sync up to master.
|TIMEPART_CHECK_SHARD_EXISTENCE | 0 | Check at startup/time partition creation that all shard files exist.

### Init time options

These options apply only at database creation time.  Use `-create -lrl /path/to/lrlfile.lrl` with the 
pre-populated options to create the database

|Option                           |Default     | Description
|---------------------------------|------------|------------
|init_with_genid48                |Off         | Use incrementing numbers for rowids instead of timestamps
|init_with_rowlocks               |Off         | Use rowlocks instead of pagelocks for short-term transaction locks
|init_with_rowlocks_master_only   |Off         | Alternate rowlocks scheme that only requires short-term rowlocks on the master
|init_with_ondisk_header          |On          | Add an extra header to records for versioning and compression information.  This can be enabled on tables individually with `ALTER TABLE`. This controls the default for new tables.  Prerequisite for other `init_with_*` options below.
|init_with_inplace_update         |On          | Do record inplace inplace instead of a delete/add.  This is far more efficient. 
|init_with_instant_schema_change  |On          | When possible (eg: when just adding fields) schema change will not rebuild the underlying tables.
|init_with_compr                  |On          | Turns out compression on table data.
|init_with_compr_blobs            |On          | Turns out compression on blob/vutf8 columns.
|init_with_bthash                 |0 (off)     | Turn on a root page lookaside cache for tables
|dtastripe                        |1           | Stripe each table across this many files (several physical files per table)
|blobstripe                       |1           | Also stripe blob files (several physical files per blob field)
|table                            |            | Multiple table options can be added to an lrl file to add tables at database init time.  Arguments are table name and path to .csc2 file.

### Runtime options

These options are toggle-able at runtime.

|Option                           |Default                                                | Description
|---------------------------------|-------------------------------------------------------|------------
|nullfkey                         | Constraints are enforced for all key values|Do not enforce foreign key constraints for null keys.
|fullrecovery                     | Off, recovery runs from previous checkpoint | Attempt to run database recovery from the beginning of available logs
|dir                              | `$COMDB2_ROOT/var/cdb2/$DBNAME` | Database directory
|default_sql_mspace_kbsz          | 1024            | Default size of memory regions owned by SQL threads, in KB 
|directio                         |On          | Bypass filesystem cache for page I/O
|osync                            |Off         | Enables `O_SYNC` on data files (reads still go through FS cache) if `directio` isn't set
|nonames                          |Off         | Use database name for some environment files (older setting, should remain off)
|checksums                        |On          | Checksum data pages.  Turning this off is highly discouraged.
|commitdelaymax                   |0           | Introduce a delay after each transaction before returning control to the application.  Occasionally useful to allow replicants to catch up on startup with a very busy system.
|lock_conflict_trace              |Off         | Dump count of lock conflicts every second
|no_lock_conflict_trace           |On          | Turns off `lock_conflict_trace`
|blocksql_grace                   |10 sec      | Let block transactions run this long if db is exiting before being killed (and returning an error).
|gbl_exit_on_pthread_create_fail |0            | If set, database will exit if thread pools aren't able to create threads.
|enable_sql_stmt_caching | not set | Enable caching of query plans.  If followed by "all" will cache all queries, including those without parameters.
|max_sqlcache_per_thread | 10 | Max number of plans to cache per sql thread (statement cache is per-thread, but see hints below)
|max_sqlcache_hints | 100 | Max number of "hinted" query plans to keep (global) - see `cdb2_use_hints()`
|max_lua_instructions | 10000 | Max lua opcodes to execute before we assume the stored procedure is looping and kill it
|iothreads | 0 | Number of threads to use for I/O prefaulting
|ioqueue | 0 | Max depth of the I/O prefaulting queue
|prefaulthelperthreads | 0 | Max number of prefault helper threads.
|osqlprefaultthreads | 0 | If set, send prefaulting hints to nodes.
|enable_prefault_udp | not set |  Send lossy prefault requests to replicants 
|disable_prefault_udp | | Disable `enable_prefault_udp`
|sqlsortermem | 314572800 | maximum amount of memory to give the sqlite sorter
|sqlsortermaxmmapsize | 2147418112 | maximum amount of file-backed mmap size in bytes to give the sqlite sorter
|cache | 64 mb | Database cache size, see [cache size](#cache-size)
|cachekb | | see [cache size](#cache-size)
|cachekbmin | | see [cache size](#cache-size)
|cachekbmax | | see [cache size](#cache-size)
|cluster nodes | | List of nodes that comprise the cluster for this database.  See [setting up clusters](cluster.html)
|appsockslimit | 500 | Start warning on this many connections to the database
|maxappsockslimit | 1400 | Start dropping new connections on this many connections to the database 
|maxsockcached | 500 | After this many connections, start requesting that further connections are no longer pooled.
|maxlockers |256  | Initial size of the lockers table (there's no current maximum)
|maxtxn | 128 | Maximum concurrent transactions.
|largepages | 0 | Enables large pages.
|maxosqltransfer | 50000 | Maximum number of records modifications allowed per transaction
|heartbeat_send_time | 5 (seconds) | Send heartbeats this often. 
|sc_del_unused_files_threshold |                             |
|tablepenaltyincpercent | | See BDB_ATTR_DISABLE_WRITER_PENALTY_DEADLOCK
|maxwt | 8 | Maximum number of threads processing write requests
|maxq | 192 | Maximum queue depth for write requests
|nice | not set | If set, will call nice() with this value to set the database nice level
|sync | | See [sync command](#sync-commands)
|allow | |  See [permissioning commands](#allowdisallow-commands)
|disallow | | See [permissioning commands](#allowdisallow-commands)
|clrpol | | See [permissioning commands](#allowdisallow-commands)
|setclass | | See [permissioning commands](#allowdisallow-commands)
|sqlflush | not set | Force flushing the current record stream to client every specified number of records
|sbuftimeout | not set | Set a timeout on client connections, connections drop if they
|throttlesqloverlog | 5 (sec) | On a full queue of SQL requests, dump the current thread pool this often
|allow_lua_print | 0 | Enable to allow stored procedures to print trace on DB's stdout
|allow_user_schema | 0 | Enable to allow per-user schemas
|resource | not set | Registers a file with the databases.  Can be referred to from stored procedures.
|repchecksum | 0 | Enable to do additional check-summing of replication stream (log records in replication stream already have checksums)
|use_parallel_schema_change | 1 | Scan stripes for a table in parallel during schema change.
|use_planned_schema_change | 1 | Only change entities that need to change on a schema change. Disable to always rebuild all data files and indices for the changing table.
|enable_bulk_import | 0 | Enable API to quickly bring in tables from another database
|enable_bulk_import_different_tables | 0 | Enable API to bring in tables from another databases that are not present in the current database  
|queuepoll | 0 | Occasionally wake up and poll consumer queues even when no events require it
|replicate_local | 0 | When enabled, record all database events to a comdb2_oplog table.  This can be used to set clusters/instances that are fed data from a database cluster. Alternate ways of doing this are planned, so enabling this option should not be needed in the near future.
|enable_tagged_api | 0 |
|enable_snapshot_isolation | 0 | Enable to allow SNAPSHOT level transactions to run against the database
|enable_new_snapshot | 0 | ***Experimental*** Enable new SNAPSHOT implementation
|enable_new_snapshot_asof | 0 | ***Experimental*** Enable new BEGIN TRANSACTION AS OF implementation
|enable_new_snapshot_logging | 0 | ***Experimental*** Enable alternate logging scheme 
|disable_new_snapshot | | Disables alternate snapshot implementation
|enable_serial_isolation | 0 | Enable to allow SERIALIZABLE level transactions to run against the database
|update_shadows_interval | 0 | Set to higher than 0 to update snaphots on every Nth operation (default is for every operation)
|enable_lowpri_snapisol | 0 | Give lower priority to locks acquired when updating snapshot state 
|disable_lowpri_snapisol | |
|sqlwrtimeout | 10000 (ms) | Set timeout for writing to an SQL connection.
|log_delete_now | 1 | Set log deletion policy to delete logs as soon as possible.
|log_delete_after_backup | 0 | Set log deletion policy to disable log deletion (can be set by backups, thought the default backups provided by copycomdb2 use a different mechanism)
|log_delete_before_startup | 0 | Set log deletion policy to disable logs older than database startup time.
|on/off | | Enable/disable various switches - see [switches](#switches)
|setattr | | Change bdb tunables - see [bdb tunables](#bdbattr-tunables)
|reqldiffstat | 60 (sec) | Set how often the database will dump various usage statistics (each entry will include changes in the last interval)
|reqltruncate | 1 | Disable to always log full SQL queries in request logs (they are truncated by default to save space)
|appsockpool | | See [thread pools](#thread-pools)
|sqlenginepool | | See [thread pools](#thread-pools)
|round_robin_stripes | 0 | Alternate to which table stripe new records are written.  The default is to keep stripe affinity by writer.
|no_round_robin_stripes | |
|chkpoint_alarm_time | 60 (sec) | Warn if checkpoints are taking more than this many seconds.
|report_deadlock_verbose | 0 | If set, dump the current thread's stack for every deadlock.
|disable_pageorder_recsz_check | 0 | If set, allow page order table scans even for pages with overflows.
|enable_pageorder_recsz_check | | Disables enable_pageorder_recsz_check
|disable_overflow_page_trace | 1 | If set, warn when a page order table scan encounters an overflow page.
|enable_overflow_page_trace | | If set, don't warn when a page order table scan encounters an overflow page.
|enable_selectv_range_check | not set | ***Experimental*** If set, SELECTV will send ranges for verification, not every touched record.
|rep_process_txn_trace | not set | If set, report processing time on replicant for all transactions
|no_rep_process_txn_trace | | Unsets rep_process_txn_trace
|ack_trace | not set | Every second, produce trace for ack messages
|no_ack_trace | | Turns off ack trace
|sql_tranlevel_default | | Sets the default SQL transaction level for the database, see (SQL transaction levels)[#sql-transaction-levels)
|sql_time_threshold | 5000 (ms) | Sets the threshold time in ms after which queries are reported as running a long time.
|nowatch | not set | Disable watchdog.  Watchdog aborts the database if basic things like creating threads, allocating memory, etc. doesn't work.
|page_latches | not set | ***Experimental*** If set, in rowlocks mode, will acquire fast latches on pages instead of full locks.
|disable_page_latches | | Turns off page latches
|replicant_latches | not set | ***Experimental*** Also acquire latches on replicants
|disable_replicant_latches | | Turns off page latches on replicants
|notimeout | not set | Turns off SQL timeouts
|setsqlattr | | See (SQL tunables)[#sql-tunables]
|master_swing_osql_verbose | not set | Produce verbose trace for SQL handlers detecting a master change
|debugthreads | off | If set to 'on' enables trace on thread events.
|dumpthreadonexit | off | If set to 'on' dump resources held by a thread on exit
|num_record_converts | 100 | During schema changes, pack this many records into a transaction.
|maxcolumns | 255 | Raise the maximum permitted number of columns per table.  There's a hard limit of 1024.
|enable_partial_indexes | not set | If set, allows partial index definitions in table schema.  See [partial indices](table_schema.html#partial-indices)
|disable_partial_indexes | | Disables partial indices
|querylimit | | See [query limit commands](#query-limit-commands)
|maxretries | 500 | Maximum number of times a transactions will be retried on a deadlock
|deadlock_rep_retry_max | not set | If set, will reset the deadlock mode after this many deadlocks on the replicant while applying the log stream.
|enable_sparse_lockerid_map | set | If set, allocates a sparse map of lockers for deadlock resolution
|disable_sparse_lockerid_map | | Disables enable_sparse_lockerid_map
|enable_inplace_blobs | set | Don't update the rowid of a blob entry on an update 
|disable_inplace_blobs | | Disables enable_inplace_blobs (needs enable_inplace_blob_optimization, and enable_osql_blob_optimization also enabled - which they are by default)
|enable_inplace_blob_optimization | | Enables inplace blob updates (blobs are updated in place in their b-tree when possible, not deleted/added)
|disable_inplace_blob_optimization | | Disables enable_inplace_blob_optimization
|enable_osql_blob_optimization | set | Replicant tracks which columns are modified in a transaction to allow blob updates to be omitted if possible
|disable_osql_blob_optimization | | Disables disable_osql_blob_optimization
|pagedeadlock_maxpoll | 5 (ms) | Randomly poll for this many ms and retry a deadlocked component of a rowlocks transaction
|pagedeadlock_retries | 500 | Retry a deadlocked component of a rowlock transaction this many times before reporting deadlock for the transaction.
|enable_cache_internal_nodes | set | Btree internal nodes have a higher cache priority.
|disable_cache_internal_nodes | | Disable enable_cache_internal_nodes
|analyze_tbl_threads | 5 | Number of threads to go through generated samples when generating index statistics
|analyze_comp_threads | 10 | Number of thread to use when generating samples for computing index statistics
|analyze_comp_threshold | 104857600 | Index file size above which we'll do sampling, rather than scan the entire index.
|print_syntax_err | not set | Trace all SQL with syntax errors. 
|survive_n_master_swings | 600 | Have a node retry applying a transaction against a new master this many times before giving up.
|master_retry_poll_ms | 100 | Have a node wait this long after a master swing before retrying a transaction
|osql_verify_retry_max | 499 | Retry a transaction on a verify error this many times - see [optimistic concurrency control](transaction_model.html#optimistic-concurrency-control)
|osql_verify_ext_chk | 1 | For block transaction mode only - after this many verify errors, see if transaction is non-commitable - see [default isolation level](transaction_model.html#default-isolation-level)
|pageordertablescan | set | Table scans read the table in page order, not row order.
|tablescan_cache_utilization | 20 | Percent of cache to allow to be used for table scans.
|early | set | When set, replicants will ack a transaction as soon as they acquire locks - not that replication must succeed at that point, and reads on that node will either see the records or block.
|noearly | |  Disables `early`.  With `noearly` replicant will wait to commit a transaction locally and release locks before acking.  The only advantage is that subsequent reads won't block on this transaction's lock - the replication semantics don't change.
|reallearly | not set | Ack as soon as a commit record is seen by the replicant (before it's applied).  This effectively makes replication asynchronous, so reads may not see the effects of a committed transaction yet.
|noreallearly | | Disables `reallyearly`
|ctrace_rollat | 0 | Roll database debug trace file (`$COMDB2_ROOT/var/log/cdb2/$dbname.trc.c`) at specified size.  Set to 0 to never roll.
|ctrace_nlogs | 7 | When rolling trace files, keep this many.  The older files will have incrementing number suffixes (.1, .2, etc.)
|ctrace_dbdir | not set | If set, debug trace files will go to the data directory instead of `$COMDB2_ROOT/var/log/cdb2/)
|disable_sql_dlmalloc | not set | If set, will use default system malloc for SQL state machines.  By default, each thread running SQL gets a dedicated memory pool.
|decimal_rounding | DEC_ROUND_HALF_EVEN | See [decimal rounding options](#decimal-rounding-options)
|mempget_timeout | 60 (seconds) |
|berkattr | | See [BerkeleyDB attributes](#berkattr-tunables)
|keycompr | | Enable index compression (applies to newly allocated index pages, rebuild table to force for all pages, see [REBUILD](sql.html#rebuild)
|nokeycompr | | Disable index compression (applies to newly allocated index pages, just like `keycompr`) 
|crypto | | See [Authentication and Encryption](auth.html)
|crc32c | set | Use crc32c (alternate faster implementation of CRC32, different checksums) for page checksums
|nocrc32c | | Disables `crc32c`, fall back to CRC32
|rcache | set | Keep a lookaside cache of root pages for b-trees
|norcache | | Disables `rcache`
|sqllogger | | See [request logging](op.html#reql)
|location | | Sets up default file locations - see [file locations](#lrl-files)
|include | | Include file given as argument.  Named file will be processed before continuing processing the current file.
|temptable_limit | 8192 | Set the maximum number of temporary tables the database can create
|disable_temptable_pool | | Disables the pool of temp tables set by `temptable_limit`, temp tables are created as needed.
|enable_upgrade_ahead | not set | Occasionally update read records to the newest schema version (saves some processing when reading them later)
|disable_upgrade_ahead | | Disables `enable_upgrade_ahead`
|do | | At the end of processing config files, execute the rest of this line as an operational command, see [operational Commands](commands.html)
|memstat_autoreport_freq | 180 (sec) | Dump memory usage to trace files at this frequency
|blob_mem_mb | not set | Blob allocator - sets the max memory limit to allow for blob values (in MB).
|blobmem_sz_thresh_kb | not set | Sets the threshold (in kb) above which blobs are allocated by the blob allocator.
|logmsg   |  | Controls the database logging level - accepts [logging commands](op.html#logging-commands).

<!-- TODO
|enable_datetime_truncation | |
|enable_datetime_promotion | |
|enable_datetime_ms_us_sc | |
|disable_datetime_truncation | |
|disable_datetime_promotion | |
|disable_datetime_ms_us_sc | |
|default_datetime_precision | |
-->
