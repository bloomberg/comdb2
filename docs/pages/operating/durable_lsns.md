--- 
title: Durable LSNs
keywords: durable, lsn 
sidebar: mydoc_sidebar
permalink: durable_lsns.html 
--- 
# Durable LSNs

## Coherency 

The leader-node in comdb2 commits a transaction locally and then blocks until
that transaction has been applied to all of the cluster's replicants.  This is
known as "distributed commit".  Replicants that cannot apply the transaction
within a certain timeout are marked 'incoherent', and are not allowed to
service new requests.

A writing client is guaranteed to see the transaction it has just committed,
because any node which hasn't applied the transaction will be marked
incoherent, and will not handle new requests.  This model preserves
"reads-follows-writes" consistency.

If a different client reads the database while a writing client is blocked in
distributed commit, the reading-client will see the writer's transaction if
that replicant has already applied it.  Our coherency model does not guarantee
visibility of the commit until the writing client regains control.  While the
writer is blocked, a reader might observe the effects of the transaction on
replicant A, and then not see those effects on replicant B.

## Durability 

A "durable" transaction cannot be unwound by a future leader.  When the
'elect\_highest\_committed\_gen' tunable is enabled, a transaction is
considered durable once it has been applied to a quorum of the cluster's
replicants.  Because elections require a quorum of nodes to promote a new
leader, this durability guarantee follows intuitively: once a transaction is
replicated to a quorum, every future quorum will include at least one node
which has applied it.  The election process ensures that a node which has not
applied the transaction cannot become the new leader.

## Replicant Retry

The 'replicant\_retry\_on\_not\_durable' tunable causes the replicant servicing
the client request to retry any transaction that has not been replicated to a
quorum.  The retry is directed to the current cluster leader, which may change
during course of the request.  A non-durable transaction will be retried until
it becomes durable, or until the retry count (controlled by
'survive\_n\_master\_swings') is exhausted.

If retries are exausted, and the 'hide\_non\_durable\_rcode' tunable is set to
true, the client api will return the transaction's original rcode.  If set to
false, the client api will return a 402 error (CDB2ERR\_NOTDURABLE).

## Metrics

The leader keeps track of the total number of distributed commits as
'distributed\_commits' in the comdb2\_metrics table.  It also tracks the
number of distributed commits that were not durable as 'not\_durable\_commits'.
These metrics are collected regardless of whether durable-LSNs or the
'replicant\_retry\_on\_not\_durable' tunable is enabled.

