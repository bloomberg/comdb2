---
title: Triggers and consumers
keywords: code
sidebar: mydoc_sidebar
permalink: triggers.html
---

## Lua Triggers

The server can be set up to run a stored procedure when records are inserted, updated or deleted from a table. The stored procedure is called once per row that is changed. This is true even if the transaction changes several rows in one commit. The stored procedure receives one argument (an event) - it is a Lua table which contains data describing the changes to the row:

```
id = unique id associated with this event
name = table name which triggered event
type = add|del|upd
new = nil|{Lua-table with values which were updated/inserted}
old = nil|{Lua-table with values which were updated/deleted}
```

The trigger procedure can obtain the transaction-id for a given event by passing it to `db:get_event_tid()`.

The [CREATE LUA TRIGGER](sql.html#create-lua-trigger) statement creates the named-trigger for insert/update/delete 
of specified fields from the specified table. Let us see a few examples. Consider a table `t` with columns `i`, `j`,
`k` and `l`. We'd like to run stored procedure `audit` which logs changes to `t` in the `audit_tbl`. The stored 
procedure would like to log when new i, j values are inserted, values of j, k are updated and k, l are delete. 

`CREATE LUA TRIGGER audit ON (TABLE t FOR INSERT OF i, j AND UPDATE OF j, k AND DELETE OF k, l)`

If we were only interested in insert for all columns, the statement would look like:

`CREATE LUA TRIGGER audit ON (TABLE t FOR INSERT OF i, j, k, l)`

If we skip column names, then all columns at the time of creating trigger are considered. Keep in mind that these 
definitions are not updated automatically when table `t` is altered. Previous example could be written as:

`CREATE LUA TRIGGER audit ON (TABLE t FOR INSERT)`

Statement to set up trigger on insert into multiple tables, say `t1` and `t2` would look like:

`CREATE LUA TRIGGER audit ON (TABLE t1 FOR INSERT), (TABLE t2 FOR INSERT)`

Behind the scenes, the database will set up a queue to save requested changes to specified tables. The database will 
then select one node in the cluster to start listening on this queue. When there is an item on the queue, the 
database reads the item, starts a new transaction, creates a corresponding Lua table and executes the stored 
procedure. When stored procedure completes successfully, the database consumes this item from the queue and 
commits the transaction. The database then goes back to listening for subsequent items on the queue.

`db:begin()` and `db:commit()` calls are not available to triggers as the database starts one implicitly so all 
of stored procedure actions and queue-consume operation are guaranteed to commit atomically.

The stored procedure should return 0 on success. Not returning a value or any other non-zero value will cause 
the stored procedure transaction to rollback.

Notice that the trigger stored procedure runs in a independent transaction (separate from the event that triggered it):
```
begin
    exec procedure
    consume (dequeue)
commit
```

This implies that if trigger stored procedure transaction has any failure (ex.
a failed insert, a duplicate error, etc.), that transaction will abort and the
event will not be removed from the queue, rather the event will be reprocessed:
trigger stored procedure will be retried again (and if it fails, it will be
retried again, possibly indefinitely). Only if the stored procedure actions
complete successfully, will the event be consumed from the queue.

Following is an example for a trigger `audit` which logs all changes to table `t`. Table has two int fields: `i`, `j`. 
stored procedure logs data to `audit_tbl`, storing type of change to `t`, time of log and the changed values. 
stored procedure converts the Lua table of new and old values into JSON strings and saves them in vutf8 fields in 
`audit_tbl.`

`t.csc2:`

```
schema {
	int i
	int j
}
```

`audit_tbl.csc2:`

```
schema {
	cstring type[32]
	cstring tbl[32]
	datetime logtime
	vutf8 new[32] null=yes
	vutf8 old[32] null=yes
}
```

`audit.lua:`

```
local function main(event)
	local audit = db:table("audit_tbl")
	if event.new ~= nil then
		event.new = db:table_to_json(event.new)
	end
	if event.old ~= nil then
		event.old = db:table_to_json(event.old)
	end
	--match audit_tbl schema
	event.logtime = db:now()
	event.tbl = event.name
	event.name = nil
	event.id = nil
	return audit:insert(event)
end
```

```
CREATE LUA TRIGGER AUDIT ON (TABLE t FOR INSERT AND UPDATE AND DELETE)
INSERT INTO t VALUES(1,1),(1,2),(1,3),(1,4)
UPDATE t SET i = j WHERE j % 2 = 0
DELETE FROM t WHERE i % 2 <> 0
```

This should generate 8 events and our sample stored procedure should have logged them in `audit_tbl`.

```
cdb2sql> select * from audit_tbl order by logtime
(type='add', tbl='t', logtime="2016-04-05T144519.287 America/New_York", new='{"i":1,"j":1}', old=NULL)
(type='add', tbl='t', logtime="2016-04-05T144519.289 America/New_York", new='{"i":1,"j":2}', old=NULL)
(type='add', tbl='t', logtime="2016-04-05T144519.289 America/New_York", new='{"i":1,"j":3}', old=NULL)
(type='add', tbl='t', logtime="2016-04-05T144519.289 America/New_York", new='{"i":1,"j":4}', old=NULL)
(type='upd', tbl='t', logtime="2016-04-05T144519.290 America/New_York", new='{"i":2,"j":2}', old='{"i":1,"j":2}')
(type='upd', tbl='t', logtime="2016-04-05T144519.290 America/New_York", new='{"i":4,"j":4}', old='{"i":1,"j":4}')
(type='del', tbl='t', logtime="2016-04-05T144519.291 America/New_York", new=NULL, old='{"i":1,"j":1}')
(type='del', tbl='t', logtime="2016-04-05T144519.291 America/New_York", new=NULL, old='{"i":1,"j":3}')
```

The `DROP LUA TRIGGER` statement will stop execution of the store procedure and remove the queue 
associated with the trigger. To delete our example trigger, we will run the following statement:

`DROP LUA TRIGGER AUDIT`


## Lua Consumers

The [`CREATE LUA CONSUMER`](sql.html#create-lua-trigger) statement creates the named-consumer for 
INSERT/UPDATE/DELETE of specified fields from the specified table. A Lua consumer is similar in mechanics to a 
Lua trigger. Instead of the database running the stored procedure automatically, this requires a client 
program to run 'EXEC PROCEDURE sp-name()'. Additionally, there are mechanisms to send back data to the calling 
program and block until client signals that data is consumed. This is different from regular server-client protocol 
which streams rows and blocks only when socket is full. 

Let us study a sample consumer. The client program would like to receive all changes to table `t`. The example 
uses `cdb2sql`, however a real application would use the API and make and the usual calls (`cdb2_run_statement`, 
`cdb2_next_record`) to run the stored procedure and obtain rows. Buffering in `cdb2sql` may cause the rows to 
not appear immediately.

`watch_t.lua:`

```lua
local function define_emit_columns()
	local schema = { {"blob", "id"}, {"text","name"}, {"text","new"}, {"text", "old"}, {"text", "type"} }
	db:num_columns(#schema)
	for k,v in ipairs(schema) do
		db:column_type(v[1], k)
		db:column_name(v[2], k)
	end
end

local function main()
	define_emit_columns()

	-- get handle to consumer associated with stored procedure
	local consumer = db:consumer()

	while true do
		local change = consumer:get() -- blocking call
		if change.new ~= nil then
			change.new = db:table_to_json(change.new)
		end
		if change.old ~= nil then
			change.old = db:table_to_json(change.old)
		end
		consumer:emit(change) -- blocking call
		consumer:consume()
	end
end
```

Example runs:
```sql
cdb2sql testdb default "CREATE LUA CONSUMER watch_t ON (TABLE t FOR INSERT AND UPDATE AND DELETE)"
```

Start two different cdb2sql sessions:

```
cdb2sql testdb default -                cdb2sql testdb default -
                                        cdb2sql> exec procedure watch_t()
cdb2sql> insert into t values(1,1)
                                        (id=x'00000200ccd20657', name='t', new='{"i":1,"j":1}', old=NULL, type='add')
cdb2sql> insert into t values(2,2)
                                        (id=x'00000200ddd20657', name='t', new='{"i":2,"j":2}', old=NULL, type='add')
cdb2sql> update t set i = i + 1
                                        (id=x'00000200e9d20657', name='t', new='{"i":2,"j":1}', old='{"i":1,"j":1}', type='upd')
                                        (id=x'00000400e9d20657', name='t', new='{"i":3,"j":2}', old='{"i":2,"j":2}', type='upd')
cdb2sql> delete from t where 1
                                        (id=x'00000100f2d20657', name='t', new=NULL, old='{"i":2,"j":1}', type='del')
                                        (id=x'00000200f2d20657', name='t', new=NULL, old='{"i":3,"j":2}', type='del')
```

The system provides a unique `id` for each event delivered to consumer. If client successfully processed an event, but crashed before requesting next row, the database will send the last event again. `id` may be useful to detect such a condition.

The `dbconsumer:emit()` only returns when the client application requests the next event by calling `cdb2_next_record`. In the example above, `dbconsumer:consume()` starts a new transaction and consumes the last event.

The consumer may want to log the change (like the sample in Lua trigger), emit the event and consume event atomically. This can be accomplished by wrapping these operations in an explicit transaction as shown below:

```
local function main()
        local audit = db:table("audit_tbl") -- from Lua trigger example
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

`DROP LUA CONSUMER watch_t`

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
x.with_tid = true | false (default)
```

When `with_tid` is `true`, Lua table returned by `dbconsumer:get/poll()` include additional property (`tid`). This is the same `tid` returned by `db:get_event_tid()`

###db:get_event_tid

```
tid = db:get_event_tid(x)
    x: Lua table returned by `dbconsumer:get/poll()` or argument passed to a `main` in Lua trigger.
```

Returns transaction-id (tid) for a given event. All events belonging to same originating transaction will have the same `tid`. This can be used by application to detect transaction boundaries as tid changes. `tid`s are not unique like event's `id` and will eventually recycle.

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
