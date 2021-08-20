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

The `DROP LUA TRIGGER` statement will stop execution of the store procedure and
remove the queue associated with the trigger. To delete our example trigger, we
will run the following statement:

`DROP LUA TRIGGER audit`

### Audit Triggers

The `CREATE LUA AUDIT TRIGGER` command will log all changes to a given table in 
a new auto-generated table. Following is an example audit trigger, which logs all 
changes to table `t` in an auto-generated table `$audit_t`.

```
CREATE TABLE t(i int, j int)$$
CREATE LUA AUDIT TRIGGER audit_trigger ON (TABLE t FOR INSERT AND UPDATE AND DELETE)
```

The audit table it creates is the same table that would be created by the following statement:

```
CREATE TABLE "$audit_t"(type cstring(4), tbl cstring(64), logtime datetime, old_i int, new_i int, old_j int, new_j int)$$
```

If the table "$audit_t" already existed, the table created would have been "$audit_t$2", or replace 2 with the smallest integer (larger than 1) that creates a unique tablename.

After we create this audit trigger, we can perform the following updates to `t`:

```
INSERT INTO t VALUES(1,1),(1,2),(1,3),(1,4)
UPDATE t SET i = j WHERE j % 2 = 0
DELETE FROM t WHERE i % 2 <> 0
EOF
```

This should generate 8 events that the audit trigger should have logged in `$audit_t`.

```
cdb2sql testdb default "select * from audit order by logtime"
(type='add', tbl='t', logtime="2021-08-05T123415.676 America/New_York", new_i=1, old_i=NULL, new_j=1, old_j=NULL)
(type='add', tbl='t', logtime="2021-08-05T123415.678 America/New_York", new_i=1, old_i=NULL, new_j=2, old_j=NULL)
(type='add', tbl='t', logtime="2021-08-05T123415.678 America/New_York", new_i=1, old_i=NULL, new_j=3, old_j=NULL)
(type='add', tbl='t', logtime="2021-08-05T123415.679 America/New_York", new_i=1, old_i=NULL, new_j=4, old_j=NULL)
(type='upd', tbl='t', logtime="2021-08-05T123419.211 America/New_York", new_i=2, old_i=1, new_j=2, old_j=2)
(type='upd', tbl='t', logtime="2021-08-05T123419.211 America/New_York", new_i=4, old_i=1, new_j=4, old_j=4)
(type='del', tbl='t', logtime="2021-08-05T123422.751 America/New_York", new_i=NULL, old_i=1, new_j=NULL, old_j=1)
(type='del', tbl='t', logtime="2021-08-05T123422.752 America/New_York", new_i=NULL, old_i=1, new_j=NULL, old_j=3)
```

You can run `DROP LUA TRIGGER audit_trigger` on an audit trigger just like a normal one.

If you find yourself altering audited tables often, you can use the `carry_alters_to_audits`.
When turned on, any alter applied to the audited table will also do the corresponding
alter to the audit table. Due to current limitations of trigger functionality, this feature
is for the most part only useful for type changes, as the data of new columns will not get tracked.

Note: if you already have a procedure of the same name as the audit_trigger, it works just fine as the audit_trigger creates a procedure with the version 'build-in audit'. If your procedure also has that version, the audit_trigger will overwrite your procedure.

#### Current Audit Trigger Limitations

* There is currently no way to create an audit table as a time partition through the audit trigger
* You cannot audit multiple tables using one audit trigger
* You are allowed to alter the audit table, which can cause issues with the audit trigger

If you need to write a trigger similar to a `LUA AUDIT TRIGGER` but not exactly the same, the following is a more verbose way of doing almost exactly the same thing as the `LUA AUDIT TRIGGER`.

```
cdb2sql testdb default - <<'EOF'
CREATE TABLE t(i int, j int)$$
CREATE TABLE "audit_t"(type cstring(4), tbl cstring(64), logtime datetime, old_i int, new_i int, old_j int, new_j int)$$
CREATE PROCEDURE audit_trigger VERSION 'sample' {
local function main(event)
    local audit = db:table('audit_t')
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
CREATE LUA TRIGGER audit_trigger ON (TABLE t FOR INSERT AND UPDATE AND DELETE)
```

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

If several client application runs the same consumer stored procedure, the database will ensure that only one executes. The rest of the stored procedures will block in the call to obtain `dbconsumer` handle (call to `db:consumer()` will block). When the executing application disconnects, the server will pick any one of the outstanding clients to run next.

The `DROP LUA CONSUMER` statement will stop execution of the store procedure and remove the queue associated with the consumer. To delete our example consumer, we will run the following statement:

`DROP LUA CONSUMER watch`

## Consumer API

### db:consumer

```
dbconsumer = db:consumer(x)
    x: Optional Lua table to set timeout, etc
```

Description:

Returns a dbconsumer object associated with the stored procedure running a Lua consumer. This method blocks until registration succeeds with master node. It accepts an optional Lua table with following keys:

```
x.register_timeout = number (ms)
```

Specify timeout (in milliseconds) to wait for registration with master. For example, `db:consumer({register_timeout = 5000})` will return `nil` if registration fails after 5 seconds.

```
x.with_sequence = true | false (default)
```

When `with_sequence` is `true`, the Lua table returned by `dbconsumer:get/poll()` includes an additional property (`sequence`).  If the trigger was created with the `PERSISTENT_SEQUENCE` option enabled, then this sequence will be a monotonically increasing count of the items that have been enqueued.

```
x.with_epoch = true | false (default)
```

When `with_epoch` is `true`, the Lua table returned by `dbconsumer:get/poll()` includes an additional property (`epoch`) which contains the unix time-epoch (seconds since 00:00:00 January 1, 1970 UTC) at the time when this event was enqueued.

```
x.with_tid = true | false (default)
```

When `with_tid` is `true`, Lua table returned by `dbconsumer:get/poll()` include additional property (`tid`). This is the same `tid` returned by `db:get_event_tid()`

### db:get_event_tid

```
tid = db:get_event_tid(x)
    x: Lua table returned by `dbconsumer:get/poll()` or argument passed to a `main` in Lua trigger.
```

Returns transaction-id (`tid`) for a given event. All events belonging to same originating transaction will have the same `tid`. This can be used by application to detect transaction boundaries as tid changes. `tid`s are not unique like event's `id` and will eventually recycle.

### dbconsumer:get

```
lua-table = dbconsumer:get()
```

Description:

This method blocks until there an event available to consume. It returns a Lua table which contains data describing the event. It contains:

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

Specify timeout (in milliseconds) to wait for event to be generated in the system. Similar to `dbconsumer:get()` otherwise. Returns `nil` if no event is avaiable after timeout.

### dbconsumer:consume

Description:

Consumes the last event obtained by `dbconsumer:get/poll()`. Creates a new transaction if no explicit transaction was ongoing.

### dbconsumer:emit

Description:

Like `db:emit()`, except it will block until the calling client requests next row by calling `cdb2_next_record`.
