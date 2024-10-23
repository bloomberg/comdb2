---
title: Comdb2 transaction model
keywords:
tags:
sidebar: mydoc_sidebar
permalink: transaction_model.html
---

We'll refer to this diagram in the rest of this section.
![cluster image](images/cluster.svg)

## Clusters

A Comdb2 cluster refers to a database running on a set of machines that receive the same replication stream.  The [Your First Database](example_db.html) section had a quick demo of setting up a cluster, and the [Setting up clusters](cluster.html) section will provide more details.  This section is largely theoretical, but it's important to understand to use Comdb2 correctly.

When a clustered database comes up, it elects a master.  A node that's the furthest ahead in the replication stream will be elected master.  In the common case, all nodes are at the same place, and the choice is arbitrary.  The rest of the nodes in the cluster are now replicants.  The replicants handle all user SQL statements.  The master replicates changes to the other nodes in the cluster.  In the default configuration, all replication is synchronous.  If an ```INSERT``` statement succeeds from a client labeled (d) in the diagram, the data is available to client (c) (ie: if (c) runs a ```SELECT``` statement that tries to find this record, it'll succeed).

## Life cycle of a transaction.

![replicated transaction image](images/transaction_life_cycle.svg)

Consider an application that updates a record.  This application is connected to a replicant. The statement runs through the system as follows.  The step numbers (eg: (1)) correspond to the diagram above.

1. Application contacts a replicant with an UPDATE statement.  The replicant prepares a query plan for the statement and runs it.  The query may end up not finding any records to update, in which case it returns to the client directly (jumping right into step 6).  
2. If the replicant finds records to update, it contacts the master to start a client transaction.  
     1. Replicant feeds any rows to be updated to the master, as the statement runs.  Note that the master performs no work other than remembering those rows.  
     1. When the query completes, if there's no errors, the replicant issues a commit request to the master.  
     1. Master starts a "real" transaction.  Under locks owned by this transaction, it performs the updates requested by the replicant.  If any operations fail (see the [locking model](#transactions-and-the-locking-model)) we jump right to step 5 and return an error to the replicant making the request.
     1. Any writes to data/index btrees done by the master are written to the database log.
     1. Master commits the transaction locally.
3. Master sends out log records up to the commit record to all replicants in parallel. 
4. Replicants apply the log records locally and send an acknowledgment back to the master. Once log records have been applied on a replicant, reads to that replicant will see the update. *Note that there can be a window within which the update is readable on one replicant but not on another.*
5. Once the master receives acknowledgments from all nodes in the cluster, it replies to the original replicant that made the request for the transaction.
6. Replicant passes the return code (or an error message) back to the application.

When a transaction consists of more than one statement, not much changes.  The replicant does not show statement boundaries to the master (consider the case where a record is added by one statement, and modified by another).  The master still gets a stream of record changes to apply.

## Transactions and the locking model

SQL statements run on a replicant with short duration locks only.  Locks are always shared ("read locks") held on a page-level granularity.  A cursor that's positioned on a row has a lock on the physical page that holds the row.   If the statement is an ```INSERT```/```UPDATE```/```DELETE```, the replicant just forwards the operation to be applied to the master, and keeps running - no write locks are acquired. Replicants receive a stream of log records for transactions that have committed.  The only write locks acquired on replicants are taken by the replication system. To apply a transaction, replicants will acquire all the necessary locks ahead of time.  If while acquiring locks, they deadlock with a running SQL statement, replication is always chosen as the deadlock victim.  Since replication does nothing until all the locks are acquired, there's nothing to undo - just release the locks, and try again.  Once it has all the locks, replication can't fail (or will fail catastrophically for things like I/O errors).  

## Optimistic concurrency control

Rows in Comdb2 are always identified by a row id (internally called a *genid*).  There are 3 things that can be done to a record: 

  * It can be inserted (where the master will generate a brand new genid).
  * It can be deleted. The record to be deleted is identified by its table and genid.  The record ceases to exist.  The genid is never reused.
  * It can be updated. The record to be updated is also identified by its table and genid.  Updating the record changes the genid.  The old value is never reused.

Since ```UPDATE``` and ```DELETE``` statements run on replicants, and do not acquire write locks, it's possible that records they refer to no longer exist on the master at the time it executes the transaction. Consider for example an application that runs the same bulk delete job at the same time on 2 replicants.  Comdb2 calls this a *verify error*.  There are 2 ways to deal with a verify error:

1. Fail the transaction and have the replicant return an error to the application. The application can decide that the transaction no longer applies, or it may re-run it (possibly with a different set of statements) 
2. Fail the transaction and have the replicant retry.  The replicant re-runs all the statements that were part of the transaction, generates a new list of operations to execute, and re-submits it to the master.

Neither solution is ideal.  The first places an additional burden on application developers for the case where the application doesn't care that the row no longer exists.  The second changes the semantics of the statement (consider silently replaying an update statement that gives an employee a raise or anything else that's not idempotent).  Having to choose between 2 evils, the default is option (2).  Applications can choose option (1) by running the SQL statement ```SET VERIFYRETRY OFF```.  Statements that are idempotent (eg: setting columns in a certain row to a fixed value) are always safe to replay.

On the master, while executing record requests on behalf of the replicants, the transactions may deadlock.  This is handled transparently.  One of the transactions in the deadlock cycle is chosen as the deadlock victim and is aborted. The list of record requests is then tried again.  The master will prefer to choose transactions that have done the least work (that hold the least locks) as losers in a deadlock cycle to avoid starving larger transactions if interactions between transactions is very heavy.

You may notice a trend in the last few sections that many isolation problems in Comdb2 are dealt with by a similar "just abort and retry" strategy.  This is what we mean when we say that the concurrency control is optimistic.  Transactions are allowed to run as if they are the only things in the system, and their interactions are reconciled at the end, when they are ready to commit.  In the overwhelmingly common case, transactions do not interact, and there's no conflicts to resolve.  Optimistically not locking allows for better efficiency for the common case.  The rare worst case happens where transactions interact heavily.  Consider for example an application that mistakenly runs the same nightly delete job on 2 machines to delete the same records, at the same time.  One of them is going to try to delete records already deleted by the other, and will be forced to retry.  

## Isolation levels and artifacts

Comdb2 offers no dirty reads isolation level.  Transactions at all transaction levels will only see results of committed transactions.  The supported levels are listed below, from least to most strict.

### Default isolation level

The default isolation level is what you get if you don't specify a higher level explicitly in your database's lrl file, or with a ```SET TRANSACTION ...``` statement.  In this level, transactions do not see **their own** updates.  Consider the [transaction life cycle](#life-cycle-of-a-transaction), and it's easy to see how this is a natural mode for the Comdb2 implementation to use.  Updates are only applied on the master, so a transaction on the replicant that deletes a row doesn't actually do anything until that transaction is committed. In the worst case, you may have a transaction that have 2 delete statements that overlap and try to delete the same row twice.  This transaction is not commitable since the second delete request will fail once the row is deleted by the first request. Retrying the transaction as described [above](#optimistic-concurrency-control) won't fix it.  Transactions like this are detected, not retried, and result in an error being passed to the application.  Having this level be the default is another design choice where we favored speed over convenience.  In our experience, most transactions are simple and serve only to achieve atomicity for a set of operations.  For those, this is a good default choice.  For more complicated scenarios more advanced transaction levels are advised.

Transaction [artifacts](#artifacts) seen at this level:

  * Reads are not repeatable
  * Transactions will not see the results of their own updates
  * Transactions are not serializable.  More details in the [serializable isolation](#serializable-isolation-level) description.

### Read committed isolation level.

```READ COMMITTED``` transactions offer a more familiar transactional model.  These allow transactions to see their own (uncommitted) updates.  They also avoid the "uncommitable transaction" surprise that sometimes happens in the [default isolation level](#default-isolation-level).  The level of isolation is otherwise the same.  Note that the master is still only involved at commit time.  Replicants keep a temporary table/index per transaction that contains any changes made by that transaction.  Any reads (```SELECT``` statements in the same transaction or reads done on behalf of a ```WHERE``` clause of a ```DELETE``` or ```UPDATE``` statement will merge real table/index data with the temporary (*"shadow"*) data.

Applications can run ```SET TRANSACTION READ COMMITTED``` before starting a transaction to enter this isolation level for the existing connection.

Transaction artifacts seen at this level:

  * Reads are not repeatable
  * Transactions are not serializable.

### Snapshot isolation level.

The ```SNAPSHOT``` isolation level presents to a transaction an illusion that time has stopped.  Transactions will not see any records that are newer then their ```BEGIN``` time.  Records that are deleted by other transactions will continue to exist for ```SNAPSHOT``` transactions.  Records that are updated by other transactions are seen with their old values.  This is implemented in a way that's similar to ```READ COMMITTED``` transactions.  The replication transactions, in addition to applying the replication stream also update temporary trees for every active ```SNAPSHOT``` transaction with the old values of updated/deleted records.  Using ```SNAPSHOT``` isolation requires adding an ```enable_snapshot_isolation``` lrl option to enable extra logging that replication needs to recover record values from the replication stream.  Enabling ```SNAPSHOT``` transactions for your connection may be done with ```SET TRANSACTION SNAPSHOT```.

Enabling the snapshot mode allows your application to see a view of a database at some time in the past as well with ```BEGIN TRANSACTION AS OF DATETIME ...```.  How far you can go back depends on your log deletion policy (see [config files](config_files.html)).  Whether looking at a current or past snapshot, when you attempt to update/delete records, at commit time the master is going to apply the changes to current rows (it has no idea of what transaction mode you were in).  That's still subject to optimistic concurrency control effects - rows you added may cause duplicate key constraint violations, rows you delete or update may no longer exist, etc.  The automatic replay of transactions with conflicts won't happen in ```SNAPSHOT``` isolation level.  Replaying won't change things since things can't change in a snapshot, so rerunning the transaction will result in at least the same conflicts.

Transaction artifacts seen at this level:

  * Transactions are not serializable

### Serializable isolation level

```SERIALIZABLE``` is a stronger isolation level than ```SNAPSHOT```.  It ensures that all transactions that were active simultaneously execute in a way that's equivalent to those transactions executing serially in some order.  ```SERIALIZABLE``` transactions require the ```enable_serial_isolation``` option in your lrl file.  Rather than contriving our own example of a simple transaction schedule that's not serializable, we invite you to look at [PostgreSQL's excellent documentation](https://www.postgresql.org/docs/9.6/static/transaction-iso.html#XACT-SERIALIZABLE).  ```SERIALIZABLE``` transactions can be enabled on your connection by running ```SET TRANSACTION SERIALIZABLE```.  Comdb2 enforcement of serializability, like everything else in Comdb2 is deferred and optimistic.  Two transactions that are not mutually serializable, as in the PostgreSQL example, will both be allowed to run until the try to commit.  One of the transactions will eventually fail to commit.

### Linearizable isolation level

```LINEARIZABLE``` is stronger than ```SERIALIZABLE``` in that it imposes a strict, cluster-wide order in which transactions are committed and viewable by clients.  Under ```SERIALIZABLE``` transaction level, there are edge-cases where a read-only sql client can see either the old or new value of an outstanding write request.  Conversely, ```LINEARIZABLE``` isolation level imposes a cluster-wide point-in-time at which any given write request is viewable.  Additionally, ```LINEARIZABLE``` ensures that the written data will be intact in any future version of the cluster which is comprised of at least a majority of the cluster members.  This isolation level is tested for a subset of sql features.  To enable it, you must enable HASql, add ```setattr DURABLE_LSNS 1``` to your cluster's lrl file, and use the ```SERIALIZABLE``` isolation level (by adding ```enable_serial_isolation``` to your lrl file, and specifying ```SET TRANSACTION SERIALIZABLE``` in your sql session.  Like all other isolation levels, ```LINEARIZABLE``` utilizes coherency-leases, and requires that the cluster-machines clocks are synchronized and do not drift beyond a known boundary.  See [Durable LSNs](durable.html) for more information about the durable LSN scheme.

## Constraints

### Transaction constraints

All constraint checks in Comdb2 are deferred.  This includes duplicate key constraints.  This allows you to create situations that aren't commitable because they violate table constraints, but that can be made commitable later in the transaction.

Consider the following session:

```sql
$ cdb2sql testdb local -
-- set a transaction level where we can see our own updates
cdb2sql> set transaction snapshot
[set transaction snapshot] rc 0
cdb2sql> select * from q;
(q=1)
[select * from q] rc 0
-- this is a standalone insert (equivalent to begin; insert; commit)
-- this fails because at the end of the transaction there are duplicate key constraint errors
cdb2sql> insert into q values(1)
[insert into q values(1)] failed with rc 299 OP #3 BLOCK2_SEQV2(824): add key constraint duplicate key 'Q' on table 'q' index 0
-- now do the same thing in a transaction
cdb2sql> begin
[begin] rc 0
-- succeeds!
cdb2sql> insert into q values(1)
[insert into q values(1)] rc 0
cdb2sql> insert into q values(1)
[insert into q values(1)] rc 0
cdb2sql> insert into q values(1)
[insert into q values(1)] rc 0
-- we can see the temporarily constraint violation here
cdb2sql> select * from q;
(q=1)
(q=1)
(q=1)
(q=1)
[select * from q] rc 0
-- committing the transaction will fail, there are constraint violations
cdb2sql> commit
[commit] failed with rc 299 OP #3 BLOCK2_SEQV2(824): add key constraint duplicate key 'Q' on table 'q' index 0
cdb2sql> begin
[begin] rc 0
cdb2sql> insert into q values(1)
[insert into q values(1)] rc 0
-- we're still in constraint violation
cdb2sql> select * from q;
(q=1)
(q=1)
[select * from q] rc 0
-- but not after this
cdb2sql> update q set q=2 where q=1 limit 1;
[update q set q=2 where q=1 limit 1] rc 0
cdb2sql> select * from q
(q=1)
(q=2)
[select * from q] rc 0
-- and now we can commit successfully
cdb2sql> commit;
[commit] rc 0
```

### Immediate and deferred statements

For the sake of speed, statements that don't have a result set won't send back any responses.  If they contain any errors (including syntax errors) they will be deferred until the next statement that sends back a response.  On the plus side, this allows streaming records into the database without blocking waiting for a response which can make quite a difference in performance.  On the minus side, errors can be unexpected, like this example:

```sql
$ cdb2sql testdb local -
cdb2sql> begin
[begin] rc 0
cdb2sql> here, a syntax error for you
-- this statement succeeds because the error is deferred
[here, a syntax error for you] rc 0
cdb2sql> select 1
-- the "here" is from a previous statement, not the one we just ran
[select 1] failed with rc -3 near "here": syntax error
```

Rules for what's deferred and what's immediate:

  * `SELECT`/`SELECTV` and `WITH` statements are always immediate. 
  * `SET` statements outside a transaction are always deferred. They cannot be run inside of a transaction, and will error immediately if attempted.
  * `BEGIN`, `COMMIT`, and `ROLLBACK` are always immediate
  * `INSERT`, `UPDATE`, `DELETE` statements inside a transaction are deferred.
  * `INSERT`, `UPDATE`, `DELETE` statements outside a transaction are immediate.

## Artifacts

Above, when discussing various transaction levels, we referred to various 
artifacts.  This section summarizes what they are.

### Read are not repeatable.

Rows selected by a transaction are not locked.  So between two runs of the 
same query within the transaction, another transaction can modify the set of 
records read by the first query, and the second query may see a different
set of records.  For example

 * T1:  `SELECT a FROM t1 WHERE a BETWEEN 1 AND 10;`
 * T2:  `INSERT INTO t1(a) VALUES(5);`
 * T2:  `COMMIT`
 * T1:  `SELECT a FROM t1 WHERE a BETWEEN 1 AND 10;`

T1 will see the new value inserted by T2.

### Transactions will not see the results of their own updates.

This only occurs in the default transaction mode.  Any updates (`INSERT`/
`UPDATE`/`DELETE`) in this mode aren't applied at all until `COMMIT` time.
So 

 * T1:  `INSERT INTO t1(a) VALUES(5);`
 * T1:  `SELECT a FROM t1 WHERE a = 5;`

T1's `SELECT` will not see the inserted value.

### Transactions are not serializable.

This one is tougher.  Imagine two transactions that do a read and write data
depending on what they read.  If the two transactions are dependent, the first
can end up including the results of the second, but the second may do its read
before the first updates, and not see the results of the first.  If the 
transactions run purely sequentially that can't happen.  A serializable set
of transactions is one that's equivalent to some serial order.

 * T1: `SELECT COUNT(*) from t1  -- say, the count is 50`
 * T2: `SELECT COUNT(*) from t1  -- count still 50`
 * T2: `INSERT INTO t1(a) values(50)`
 * T1: `INSERT INTO t1(a) values(50)`
 * T1  `COMMIT`
 * T2  `COMMIT`

There's two 50 records after these transactions commit.  This couldn't happen
if the transactions ran one after the other - the second transaction would 
insert 51.  The history above is not serializable.

## Coherency Leases

A <i>coherency lease</i> is the mechanism by which a cluster's master permits a replicant to service client requests for a period of time.  A lease gives a replicant permission to service requests for a period of time by extending its <i>coherency_timestamp</i> to some point in the future beyond the replicants current clocktime.  Replicant leases are issued by the master over UDP. A lease contains the master's timestamp when it decided to issue the lease, and the duration of the lease.  When a replicant receives the lease, it updates its <i>coherency_timestamp</i> to 

``MIN(master-timestamp-on-lease,current-replicant-system-time) + lease-duration``

Because this system is time-based, it is subject to clock-skew.  Specifically, if a replicant's clock is ahead the master's clock by an amount larger than the lease itself then the lease itself will never exceed the replicant's current-time, and the replicant will never be able to service requests.  A more serious situation occurs if the lease itself is delayed on the network for a period of time exceeding its duration: if the replicant's clock trails the master's, then this expired lease could incorrectly be deemed as valid, allowing an incoherent replicant to service requests.

Because of this, comdb2, like other distributed systems, requires that the clock-skew between cluster nodes is bound.  A reasonable lease duration will be substantially greater (an order of magnitude or larger) than the maximum clock skew.  The default lease duration is 500 ms, but this can be modified in your database's lrl file via the <i>COHERENCY_LEASE</i> attribute:

``setattr COHERENCY_LEASE <milliseconds>``

Comdb2 conservatively pauses writes for double this time to ensure that a replicant's lease has expired.  The master will issue leases once every 200 milliseconds by default.  This is intentionally conservative in that it allows a replicant to remain coherent if a single coherency packet is dropped by the network.  The lease-renew interval attribute can be modified via the <i>LEASE_RENEW_INTERVAL</i> attribute:

``setattr LEASE_RENEW_INTERVAL <milliseconds>``

Another attribute, <i>ADDITIONAL_DEFERMS</i> extends the pause in writes for an additional period beyond twice the lease duration.  This is set to 100 milliseconds by default.

