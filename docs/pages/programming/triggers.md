---
title: Triggers and consumers
keywords: code
sidebar: mydoc_sidebar
permalink: triggers.html
---


## Lua Triggers

The server can be set up to run a stored procedure when records are inserted,
updated or deleted from a table. The stored procedure is called once per row
that is changed. This is true even if the transaction changes several rows in
one commit. The stored procedure receives one argument (an event) - it is a Lua
table which contains data describing the changes to the row:

```
id = unique id associated with this event
name = table name which triggered event
type = add|del|upd
new = nil|{Lua-table with values which were updated/inserted}
old = nil|{Lua-table with values which were updated/deleted}
```

The trigger procedure can obtain the transaction-id for a given event by
passing it to `db:get_event_tid()`.

The [CREATE LUA TRIGGER](sql.html#create-lua-trigger) statement creates the
named-trigger when rows are inserted, updated and (or) deleted from the table.

Let us see a few examples. Consider a table `t` with columns `i`, `j`, `k`,
`l`. We'd like to run stored procedure `audit` which logs changes to `t` into
table `audit`. The stored procedure would like to log values of i, j when a new
row is inserted, log values of j, k when rows are updated and log values of k,
l when rows are deleted.

`CREATE LUA TRIGGER audit ON (TABLE t FOR INSERT OF i, j AND UPDATE OF j, k AND DELETE OF k, l)`

If we wanted to log all columns when a row is inserted, the statement would
look like:

`CREATE LUA TRIGGER audit ON (TABLE t FOR INSERT OF i, j, k, l)`

If we skip column names, then all columns at the time of creating trigger are
considered. Keep in mind that these definitions are not updated automatically
when table `t` is altered. Previous example could be written as:

`CREATE LUA TRIGGER audit ON (TABLE t FOR INSERT)`

Trigger can be set up so the system will assign monotonically increasing ids to events:
`CREATE LUA TRIGGER audit WITH SEQUENCE ON (TABLE t FOR INSERT)`

Statement to set up trigger on insert into multiple tables, say `t1` and `t2`
would look like:

`CREATE LUA TRIGGER audit ON (TABLE t1 FOR INSERT), (TABLE t2 FOR INSERT)`

Behind the scenes, the database will set up a queue to save requested changes
to specified tables. The database will then select one node in the cluster to
start listening on this queue. When there is an item on the queue, the database
reads the item, starts a new transaction, creates a corresponding Lua table and
executes the stored procedure. When stored procedure completes successfully,
the database consumes this item from the queue and commits the transaction. The
database then goes back to listening for subsequent items on the queue.
`db:begin()` and `db:commit()` calls are not available to triggers as the
database starts one implicitly so all of stored procedure actions and
queue-consume operation are guaranteed to commit atomically.

The stored procedure should return 0 on success. Not returning a value or any
other non-zero value will cause the stored procedure transaction to rollback.
This implies that if trigger stored procedure transaction has any failure (ex.
a failed insert, a duplicate error, etc.), that transaction will abort and the
event will not be removed from the queue, rather the event will be reprocessed:
i.e. stored procedure will be rerun (and if it fails, it will be retried,
possibly indefinitely). Only if the stored procedure actions complete
successfully, will the event be consumed from the queue.

Following is an example for a trigger `audit` which logs all changes to table
`t`. Table has two int fields: `i`, `j`. Stored procedure logs data to table
`audit`, storing type of change to `t`, time of log and the changed values.

```
cdb2sql testdb default - <<'EOF'
CREATE TABLE t(i int, j int)$$
CREATE TABLE audit(type cstring(4), tbl cstring(64), logtime datetime, i int, j int, old_i int, old_j int)$$
CREATE PROCEDURE audit VERSION 'sample' {
local function main(event)
    local audit = db:table('audit')
    local chg
    if event.new ~= nil then
        chg = event.new
    end
    if chg == nil then
        chg = {}
    end
    if event.old ~= nil then
        for k, v in pairs(event.old) do
            chg['old_'..k] = v
        end
    end
    chg.type = event.type
    chg.tbl = event.name
    chg.logtime = db:now()
    return audit:insert(chg)
end
}$$
CREATE LUA TRIGGER audit ON (TABLE t FOR INSERT AND UPDATE AND DELETE)
INSERT INTO t VALUES(1,1),(1,2),(1,3),(1,4)
UPDATE t SET i = j WHERE j % 2 = 0
DELETE FROM t WHERE i % 2 <> 0
EOF
```

This should generate 8 events and our sample stored procedure should have
logged them in `audit`.

```
cdb2sql testdb default "select * from audit order by logtime"
(type='add', tbl='t', logtime="2019-12-19T131422.330 America/New_York", i=1, j=1, old_i=NULL, old_j=NULL)
(type='add', tbl='t', logtime="2019-12-19T131422.331 America/New_York", i=1, j=2, old_i=NULL, old_j=NULL)
(type='add', tbl='t', logtime="2019-12-19T131422.332 America/New_York", i=1, j=3, old_i=NULL, old_j=NULL)
(type='add', tbl='t', logtime="2019-12-19T131422.332 America/New_York", i=1, j=4, old_i=NULL, old_j=NULL)
(type='upd', tbl='t', logtime="2019-12-19T131422.333 America/New_York", i=2, j=2, old_i=1, old_j=2)
(type='upd', tbl='t', logtime="2019-12-19T131422.334 America/New_York", i=4, j=4, old_i=1, old_j=4)
(type='del', tbl='t', logtime="2019-12-19T131422.334 America/New_York", i=NULL, j=NULL, old_i=1, old_j=1)
(type='del', tbl='t', logtime="2019-12-19T131422.335 America/New_York", i=NULL, j=NULL, old_i=1, old_j=3)
```

The `DROP LUA TRIGGER` statement will stop execution of the store procedure and
remove the queue associated with the trigger. To delete our example trigger, we
will run the following statement:

`DROP LUA TRIGGER audit`


## Lua Consumers

The [`CREATE LUA CONSUMER`](sql.html#create-lua-trigger) statement creates the
named-consumer for INSERT/UPDATE/DELETE of specified fields from the specified
table. A Lua consumer is similar in mechanics to a Lua trigger. Instead of the
database running the stored procedure automatically, this requires a client
program to run 'EXEC PROCEDURE sp-name()'. Additionally, there is a mechanism
to send back data to the calling program and block until client signals that
data is consumed. This is different from regular server-client protocol which
streams rows and blocks only when socket is full.

Let us study a sample consumer. The client program would like to receive all
changes to table `t`. The example uses `cdb2sql`, however a real application
would use the API and make and the usual calls (`cdb2_run_statement`,
`cdb2_next_record`) to run the stored procedure and obtain rows. Buffering in
`cdb2sql` may cause the rows to not appear immediately.

```
cdb2sql testdb default - <<'EOF'
CREATE TABLE t(i int, d double, c cstring(10), b byte(4), t datetime, y intervalym)$$
CREATE PROCEDURE watch VERSION 'sample' {
local function define_emit_columns()
    local num = db:exec("select count(*) as num from comdb2_columns where tablename='t'"):fetch().num
    --temporary kludge: convert to Lua number
    num = tonumber(tostring(num))
    local total = 2 + num * 2 -- type, tablename, new columns, old columns
    db:num_columns(total)

    local i = 0
    local cols = {}

    i = i + 1
    table.insert(cols, i, {n='tbl', t='cstring'})

    i = i + 1
    table.insert(cols, i, {n='type', t='cstring'})

    local stmt = db:exec("select columnname as name, type as type from comdb2_columns where tablename='t'")
    local r = stmt:fetch()
    while r do
        local n = tostring(r.name)
        local t = tostring(r.type)
        i = i + 1
        table.insert(cols, i, {n = n, t= t})
        table.insert(cols, i + num, {n = 'old_'..n, t = t})
        r = stmt:fetch()
    end
    for k, v in ipairs(cols) do
        db:column_name(v.n, k)
        db:column_type(v.t, k)
    end
end

local function main()
    define_emit_columns()
    -- get handle to consumer associated with stored procedure
    local consumer = db:consumer()
    while true do
        local change = consumer:get() -- blocking call
        local row
        if change.new ~= nil then
            row = change.new
        end
        if row == nil then
            row = {}
        end
        if change.old ~= nil then
            for k, v in pairs(change.old) do
                row['old_'..k] = v
            end
        end
        row.tbl = change.name
        row.type = change.type
        consumer:emit(row) -- blocking call
        consumer:consume()
    end
end
}$$
CREATE LUA CONSUMER watch ON (TABLE t FOR INSERT AND UPDATE AND DELETE)

INSERT INTO t VALUES(1, 22.0/7, 'hello', x'deadbeef', now(), 5)
UPDATE t set i = i + d, d = d + i, b = x'600dc0de'
DELETE from t
EOF
```

A client which executes the `watch` procedure will receive the following rows:
```
cdb2sql testdb default "EXEC PROCEDURE watch()"
(tbl='t', type='add', i=1, d=3.142857, c='hello', b=x'deadbeef', t="2019-12-19T200838.418 ", y="0-5", old_i=NULL, old_d=NULL, old_c=NULL, old_b=NULL, old_t=NULL, old_y=NULL)
(tbl='t', type='upd', i=4, d=4.142857, c='hello', b=x'600dc0de', t="2019-12-19T200838.418 ", y="0-5", old_i=1, old_d=3.142857, old_c='hello', old_b=x'deadbeef', old_t="2019-12-19T200838.418 ", old_y="0-5")
(tbl='t', type='del', i=NULL, d=NULL, c=NULL, b=NULL, t=NULL, y=NULL, old_i=4, old_d=4.142857, old_c='hello', old_b=x'600dc0de', old_t="2019-12-19T200838.418 ", old_y="0-5")
```


The system provides a unique `id` for each event delivered to consumer. If
client successfully processed an event, but crashed before requesting next row,
the database will send the last event again. `id` may be useful to detect such
a condition.

The `dbconsumer:emit()` only returns when the client application requests the
next event by calling `cdb2_next_record`. In the example above,
`dbconsumer:consume()` starts a new transaction and consumes the last event.

The consumer may want to log the change (like the sample in Lua trigger), emit
the event and consume event atomically. This can be accomplished by wrapping
these operations in an explicit transaction as shown below:

```
local function main()
        local audit = db:table("audit") -- from Lua trigger example
        local consumer = db:consumer()
        ...
        while true do
                local change = consume:get()
                -- massage change into audit and emit formats
                ...
                db:begin()
                        audit:insert(lua_tbl_for_audit)
                        consumer:emit(lua_tbl_for_emit)
                        consumer:consume()
                db:commit()
        end
end
```

If several client application runs the same consumer stored procedure, the
database will ensure that only one executes. The rest of the stored procedures
will block in the call to obtain `dbconsumer` handle (call to `db:consumer()`
will block). When the executing application disconnects, the server will pick
any one of the outstanding clients to run next.

The `DROP LUA CONSUMER` statement will stop execution of the store procedure
and remove the queue associated with the consumer. To delete our example
consumer, we will run the following statement:

`DROP LUA CONSUMER watch`

### Batch consume from queue
At times it is useful to consume several events atomically. This can be used to
reduce round trips to the client application and also to reduce transaction
overhead when consuming. This is specially useful for applications which modify
large number of records in a transaction. Consuming in batches which match the
size of the originating transaction helps reduce latency in processing events
as well.

Instead of calling `dbconsumer:consume()` which consumes that last event
immediately, we can call `dbconsumer:next()` to remember to consume the last
event fetched by `dbconsumer:get()` (or `dbconsumer:poll()`). Subsequent call
to `get/poll` will return the next available event. Before calling
`dbconsumer:next()` it is required that user start an explicit transaction by
calling `db:begin()`. On calling `db:commit()`, system will consume all events
for which `dbconsumer:next()` was called. User may choose to commit on
transaction boundary or perhaps after every N records, etc.

Here is an example stored procedure which consumes all events which belong to
the same originating transaction. To reduce round-trips between client-server,
it calls `db:emit()` for all events, and then emits a sentinal row using
`dbconsumer:emit()`, which waits for client to signal that it has finished
processing all the events.

```
local function consume_a_txn(consumer)
        local event0 = consumer:get()
        -- missing error handling here
        local txn0 = db:get_event_tid(event0)
        while true do
                local event = consumer:poll(0)
                -- If there are no more events, or we are at a transaction boundary:
                -- return so we consume everything we have seen so far.
                if event == nil then return end
                if db:get_event_tid(event) ~= txn0 then return end
                -- Otherwise, emit this event and move to next event
                db:emit(event.new.data) -- Does not wait for client to ack
                consumer:next()
        end
end
local function main()
        local consumer = db:consumer()
        while true do
                db:begin()
                consume_a_txn(consumer)
                consumer:emit('--sentinal--') -- Wait here for client to ack
                db:commit() -- consume all events emitted so far
        end
end
```

Let us insert some data:
```
insert into t(data) values('first')
insert into t(data) values('second'),('third')
begin
insert into t(data) values('fourth')
insert into t(data) values('fifth')
commit
insert into t(data) select printf('row %d', value) from generate_series(6, 10)
```

The client which executes this procedure will receive:
```
($0='first')
($0='--sentinal--')
($0='second')
($0='third')
($0='--sentinal--')
($0='fourth')
($0='fifth')
($0='--sentinal--')
($0='row 6')
($0='row 7')
($0='row 8')
($0='row 9')
($0='row 10')
($0='--sentinal--')
```


## Consumer API

### db:consumer

```
dbconsumer = db:consumer(x)
    x: Optional Lua table to set timeout, etc
```

Description:

Returns a dbconsumer object associated with the stored procedure running a Lua
consumer. This method blocks until registration succeeds with master node. It
accepts an optional Lua table with following keys:

```
x.register_timeout = number (ms)
```

Specify timeout (in milliseconds) to wait for registration with master. For
example, `db:consumer({register_timeout = 5000})` will return `nil` if
registration fails after 5 seconds.

```
x.with_sequence = true | false (default)
```

When `with_sequence` is `true`, the Lua table returned by
`dbconsumer:get/poll()` includes an additional property (`sequence`).  If the
trigger was created `with sequence` option enabled, then this
sequence will be a monotonically increasing count of the items that have been
enqueued.

```
x.with_epoch = true | false (default)
```

When `with_epoch` is `true`, the Lua table returned by `dbconsumer:get/poll()`
includes an additional property (`epoch`) which contains the unix time-epoch
(seconds since 00:00:00 January 1, 1970 UTC) at the time when this event was
enqueued.

```
x.with_tid = true | false (default)
```

When `with_tid` is `true`, Lua table returned by `dbconsumer:get/poll()`
include additional property (`tid`). This is the same `tid` returned by
`db:get_event_tid()`

### db:get_event_epoch

```
e = db:get_event_epoch(x)
    x: Lua table returned by `dbconsumer:get/poll()` or argument passed to a `main` in Lua trigger.
    e: Unix time-epoch at the time when this event (x) was enqueued.
```

Returns unix time-epoch for a given event.

### db:get_event_sequence

```
s = db:get_event_sequence(x)
    x: Lua table returned by `dbconsumer:get/poll()` or argument passed to a `main` in Lua trigger.
    s: Sequence for the event (x)
```

If the trigger (or consumer) is created `with sequence`, return the sequence for event `x`.

### db:get_event_tid

```
tid = db:get_event_tid(x)
    x: Lua table returned by `dbconsumer:get/poll()` or argument passed to a `main` in Lua trigger.
```

Returns transaction-id (`tid`) for a given event. All events belonging to same
originating transaction will have the same `tid`. This can be used by
application to detect transaction boundaries as tid changes. `tid`s are not
unique like event's `id` and will eventually recycle.

### dbconsumer:get

```
lua-table = dbconsumer:get()
```

Description:

This method blocks until there an event available to consume. It returns a Lua
table which contains data describing the event. It contains:

|Key  | Value
|-----|--------------------------------------
|id   | unique id associated with this event
|name | table name which triggered event
|type | "add" or "del" or "upd"
|new  | nil or a Lua-table with values which were updated/inserted
|old  | nil or Lua-table with values which were updated/deleted
|tid  | optional transaction id (see `with_tid` above)

### dbconsumer:poll

```
lua-table = dbconsumer:poll(t)
    t: number (ms)
```

Specify timeout (in milliseconds) to wait for event to be generated in the
system. Similar to `dbconsumer:get()` otherwise. Returns `nil` if no event is
avaiable after timeout.

### dbconsumer:consume

Description:

Consumes the last event obtained by `dbconsumer:get/poll()`. Creates a new
transaction if no explicit transaction was ongoing.

### dbconsumer:next

Description:

Adds the last event obtained by `dbconsumer:get/poll()` to list of events to
consume by subsequent `db:commit()` call. Requires that `db:begin()` has been
called prior.

### dbconsumer:emit

Description:

Like `db:emit()`, except it will block until the calling client requests next
row by calling `cdb2_next_record`.

### dbconsumer:emit_timeout

```
dbconsumer:emit_timeout(t)
    t: number (ms)
```

Description:

Specify timeout (in milliseconds) for a `dbconsumer:emit()` call. If a client
does not request the next event (by calling `cdb2_next_record()`) for the
duration of the timeout, and there is another client blocked on call to
`db:consumer()` to read from the same queue, then the blocked client will be
allowed to proceed.


## Default Lua Consumers

Most consumer procedures simply emit whatever events are generated. The system
provides a default consumer which does this as a convenience for developers.
<pre>CREATE <b>DEFAULT</b> LUA CONSUMER ...</pre> statement will create a
default procedure, set the default version for the procedure, and create the
queue for saving and consuming modifications to specified table. A client
application simply runs `exec procedure ...` statement for the named consumer
and start receiving events without any additional work.

In addition to the table's columns, the emitted rows also have `comdb2_event`
and `comdb2_id` columns. `comdb2_id` is the unique id associated with an event
as described in the prior sections. `comdb2_event` will be `add` or `del` for
insert and delete events respectively. For update events, two rows are emitted.
`comdb2_event` for the first row will be `old` and rest of the columns will
contain the old values, while `comdb2_event` for the second row will be `new`
and rest of the columns will provide the updated values. `comdb2_id` for `old`
and `new` rows will be the same.

An application can pass a JSON string as an argument to `main` to modify the
behavior of the default stored procedure.

To include additional metadata (`epoch`, `sequence`, and `tid` as documented in
earlier sections), an application can set the following attributes:

```json
{
    "with_epoch": true,
    "with_sequence": true,
    "with_tid": true
}
```

These will add `comdb2_epoch`, `comdb2_sequence`, and `comdb2_tid` columns
respectively to the emitted rows.

By deafult, one event is consumed per transaction (dbconsumer:get() followed by
dbconsumer:consume()), Application can change this to consume in batches which
match originating transaction sizes (by using `dbconsumer:next()`.) To do this
pass the following parameter to main:

```json
{
    "batch_consume": true
}
```

The default consumer makes blocking calls (`db:consumer()` and
`dbconsumer:get()`.) The consumer also sets `emit_timeout` to 10 seconds. To
override this behavior, applications can set the following attributes
(parameter values in milliseconds):

```json
{
    "register_timeout": 1000,
    "poll_timeout": 2000,
    "emit_timeout": 3000
}
```

When `poll_timeout` is specified, default consumer uses `dbconsumer:poll()`
instead of `dbconsumer:get()`. If `register_timeout` or `poll_timeout` occur,
the procedure will emit a row and `comdb2_event` will contain the string
`register_timeout` and `poll_timeout` respectively, and then continue
execution (unless the client calls `cdb2_close()`.)
