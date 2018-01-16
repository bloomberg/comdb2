---
title: Creating database clusters
keywords:
tags:
sidebar: mydoc_sidebar
permalink: cluster.html
---

Comdb2 clusters are fully dynamic.  Nodes can come and go at will.  Clusters can be expanded or shrunk when
needed.   A typical setup involves creating a database, copying it to machines where it will run and bringing
it up.  That's usually all there is to it.  The rest of this document explains this in greater details and
discusses some useful cases.

If you're creating Comdb2 clusters on AWS, you'll be interesting in the [AWS](hosted.html#comdb2-on-aws)
document instead.

## Copying databases

Before discussing clusters, we need to know how to copy databases.  Comdb2 comes with a script called 
`copycomdb2` that should be used for all database copies.  `copycomdb2` in turn calls a lower-level 
utility called `comdb2ar` whose job consists of serializing/deserializing databases.  Databases can always
be copied from a live source.  It's highly suggested that live databases are not copied with other utilities.

### Transport

`copycomdb2` uses ssh as a transport.  In order for it to work, all machines in question should accept
the key for the current user.  Copying a database sets up a pipeline similar to this:

    comdb2ar c /path/to/lrl | ssh remote comdb2ar x ...

`comdb2ar`'s job is to serialize the database on the source machine and deserialize it on the destination 
machine.  When copying a live database this involves a couple of extra steps in addition to copying database
files:

  * Database needs to hold logs necessary for recovery
  * Copy needs to check that it doesn't copy partial page writes
  * Database needs to run recovery on the destination

`copycomdb2` (really `comdb2ar`) takes care of these steps.


### Running copycomdb2

`copycomdb2` takes a path to the database configuration file (.lrl) as input.  It has a couple of modes of 
operation - running it without arguments will display common usage examples.  copycomdb2 will modify the
.lrl file on the destination database to change the `dir` option if told to copy the database to a 
different directory from the source.

#### Pull mode

This takes a database from a remote machine and copies it locally.  To copy testdb from machine m1 and
place it in the same directory where it lives on m1, one would run:

   copycomdb2 m1:/path/to/testdb.lrl

`copycomdb2` can optionally take 2 paths: the path to place database files, and the place to place the .lrl
file.  This is useful for installations that keep configuration files grouped together for multiple databases.
Sample usage:


   copycomdb2 m1:/path/to/testdb.lrl /opt/bb/var/etc/cdb2/testdb.lrl /opt/bb/var/lib/cdb2/testdb

This places the configuration file in /opt/var/etc/cdb2 and the database files in /opt/bb/var/lib/cdb2/testdb.
`copycomdb2` will create directories if necessary.


#### Push mode

Push mode will take a database from the current machine and copy it to another machine.  This is particularly
useful for creating clusters.  To place a database on a remote machine m1 in the same location as the local
machine, one would run:

   copycomdb2 /path/to/testdb.lrl m1:

`copycomdb2` in push mode accepts the same optional directories as it does in pull mode.  To place the lrl file
and data files in a specific place, the command is:

   copycomdb2 /path/to/testdb.lrl m1:/opt/var/etc/cdb2/testdb.lrl m1:/opt/var/lib/cdb2/testdb

Note that machine names for the lrl and data destinations must match.


#### Local copy and move

`copycomdb2` can be used to create a copy of the database in a different location on the same machine.  Just
omit the machine name:

    copycomdb2 /path/to/testdb.lrl /home/mike/db/testdb /home/mike/db/testdb

Note that we need to specify 2 directories here: one for the lrl file and one for the data files.  They can
match.

`-m` can be used to move the database files instead of copying them.  The source database must not be
running in this case.

#### Backup mode

`copycomdb2` can also take a backup of a database.  The usage is simple:

    copycomdb2 -b /path/to/dbname.lrl > backup

copycomdb2 will write the backup file to its stdout - redirect it to wherever you'd like the backup to reside.  


#### Restore mode

    copycomdb2 -B backup /path/to/lrl /path/to/data

Restore mode uses `-B`.  It takes as required arguments the backup produced with `-b`, and the path where the
restore should place the lrl and data files.


## Creating a cluster

Creating a cluster is as simple as:

   1. Create database locally
   1. Add the cluster nodes to the .lrl file
   1. Copy the database to the machines in the cluster
   1. Bring up the database.

### Create database locally

#### Default options

Running `comdb2 --create dbname` will create a database called `dbname`.  It'll create an lrl file with default
settings and place it in `$COMDB2_ROOT/var/cdb2/dbname.lrl`.  The data directory will be 
`$COMDB2_ROOT/var/cdb2/dbname`.  

#### Custom options

Running `comdb2 --create --lrl /path/to/dbname.lrl dbname` will create a database from a template specified in
the provided lrl file.  At a minimum, the lrl file should specify the db name and directory for data files.
A sample minimal lrl file looks something like this:

    name  testdb
    dir   /opt/bb/var/cdb2/testdb

Running `comdb2 --create ...` on this lrl file will initialize a database in the specified directory
with the given options.  For a full explanation of available options, see [Configuration Files](config_files.html).


### Adding cluster information

Cluster information is added to the database lrl file with the 'cluster nodes' tunable.  It takes a list
of machines where the database should run.  For example, to run a database cluster on machines m1, m2, m3, m4, 
m5, the lrl file will look something like this:

    name testdb
    dir /opt/bb/var/cdb2/testdb

    cluster nodes m1 m2 m3 m4 m5

### Copying the database

The database can be pushed to the cluster machines with `copycomdb2`.  For the example lrl file we've been
working with, the database can be deployed to the cluster with:

    for machine in m1 m2 m3 m4 m5; do
       copycomdb2 /opt/bb/var/cdb2/dbname.lrl machine:
    done

### Bring up the database

The database now needs to come up on the machines comprising its cluster.  When a database comes up, it'll
attempt to connect to all nodes specified in its cluster line.  As soon as enough nodes are connected to
constitute the majority of the cluster, the database will elect one of the nodes to be the master.  Other
nodes will be replicants.  The database will become available at that point.

If the database is installed in the default location (ie: whatever is generated by `comdb2 --create`) it'll run
just by specifying the database name.

    $ comdb2 testdb &

Otherwise, you'll need to specify the lrl file location:

    $ comdb2 --lrl /path/to/testdb.lrl testdb &

Note that for uses other than experimentation, it's very useful to define a structured way to manage database
instances on a machine.  TBD.

## Cluster nodes

Cluster membership for a machine has a couple of implications.  First, once a machine is listed as part of the
cluster, the database expects the database to be available on that machine.  This may sound tautological, but
if a database isn't up on a machine that's part of its cluster, and did not exit gracefully, it's considered
to be in a crashed state.  If enough machines are in that state (at last half), the cluster will become 
unavailable.

Second, if a database is down on a machine (whether it came down gracefully or not), all other nodes will
hold log files for it so it may catch up to the cluster when it comes back.  The [Operational Commands](op.html)
document has details how to break a database from the cluster in the [Cluster Commands](op.html#cluster-commands)
section.

The database internally keeps 2 lists of machines.  The first list is the set of machines that are supposed to
be part of the cluster, also known as the "sanctioned list".  The second is the set of machines connected or supposed
to be connected to the cluster (the "connected list").

### Sanctioned list

The sanctioned is the list of machines that are "supposed to be" part of the cluster.  This is the union of the
nodes listed in lrl files on all the nodes in the cluster.  In general, this list is going to be identical on
all nodes, but may vary in special circumstances (when nodes are being added/removed, or the cluster is moving
to a different set of machines).

The sanctioned list can be queried with the 'bdb sanc' command.  For example:

    $ cdb2sql -tabs testdb @m1 "exec procedure sys.cmd.send('bdb sanc')"
    cmd:'bdb sanc'
    sanc dump:
    node m1 
    node m2 
    node m3 
    node m4 
    node m5 
    sanc is intact

The above tells us that the database nodes are configured to be m1, m2, m3, m4, and m5.  All the nodes in that
list are up ("sanc is intact").  If any of the nodes are down, the message would say "sanc nodes are missing".

The database will hold logfiles for all nodes in the sanctioned list.  If a node should no longer be part
of the cluster (for example, if it's being removed permanently because the cluster is shrinking), it can be
removed from the cluster with the 'bdb del' command, eg:

    $ cdb2sql -tabs testdb @m1 "exec procedure sys.cmd.send('bdb del m3')"

Note that removing the node from the lrl files is not good enough - nodes share their sanctioned list when they
join the cluster - as long as any nodes with memory of m3 are up, it'll share that information with any nodes
that join the cluster, even if m3 is missing from their own lrl file.  See the 
[Shrinking Clusters](cluster.html#shrinking-a-cluster) section in this document for more details.

The 'bdb del' command is replicated - sending it to one node will forward it to the rest of the connected nodes.

## The connected list.

This is the list of nodes that attempted to connect to cluster.  The connected list is a union of all the nodes
that attempted to connect.  For example, if a node m6 comes up and connects to node m3, m3 will send around the
fact that it's seen m6, and m6 will now be considered a part of the cluster.  Disconnecting m6 will require
removing it from the cluster with 'bdb del' - see the section on [Shrinking Clusters](#shrinking-a-cluster) for
details.

The connected nodes may be queried with the 'bdb cluster' command.  For example:

    MASTER c m3:18663 fd 22  lsn 1:2357344 f 0 
           c m2:18647 fd 21  lsn 1:2357344 f 0 
           c m1:18051 fd 25  lsn 1:2357344 f 0 
           c m3:18648 fd 17  lsn 1:2357344 f 0 
           n m2:18647 fd -1  lsn 1:2357344 f 0 
           l m1:18647 fd -1  lsn 1:2357344 f 0 

In the output above, we see that m3 is the master (see [bdb cluster](op.html#bdb-cluster) command for details),
and that m2 is disconnected.  We can permanently remove it from the cluster with the 'bdb rem' command:

    $ cdb2sql -tabs testdb @m1 "exec procedure sys.cmd.send('bdb rem m2')"

## Growing a cluster

Adding a node to the cluster is simple:

   1. Add the node to lrl files on other nodes
   1. Add the database to the sanctioned list
   1. Copy the database there
   1. Bring up the database

You'll note that this is almost identical to the steps used to create the cluster in the first place.  The first
step is to make sure that the 'cluster nodes' line on all the nodes (including the new node being added) has 
the complete cluster line that includes the new node.  It should then be added to the database's sanctioned list
("exec procedure sys.cmd.send('bdb add newnodename'").  This step is necessary to prevent the database from
deleting log files that are generated while the copy is taking place.  Note that any of the existing database
nodes  can be used as the source for the copy.  Once the copy is complete, the new node can be brought up.
It will catch up to any updates missed while it was being copied, and will join the cluster fully once that's
done.

There's one small caveat.  Since more than half of the cluster's nodes must be connected to leave the cluster 
available, simultaneously bringing as many (or mode) nodes that are currently in the cluster will make it
unavailable for a short time.  No updates should be missed, but to avoid this situation, it's highly suggested
to add nodes in chunks of < 1/2 current cluster size.

## Shrinking a cluster

Shrinking the cluster is similar to growing the cluster.  The steps are:

   1. Remove the node from the cluster nodes line on all nodes
   1. Bring down the database on the machine that's leaving the cluster.
   1. Tell the other nodes that it's leaving for good

The first step is to remove the node being removed from the cluster from lrl files on all nodes.  The database
can then be brought down (via "exec procedure sys.cmd.send('exit')").  The cluster can be told to "forget" about
the node with (with "exec procedure sys.cmd.send('bdb rem nodename')") and removed from the sanctioned list
(with "exec procedure sys.cmd.send('bdb del nodename')".

## Moving a cluster

Moving a database between cluster is a combination of adding new nodes and removing old nodes.  To 
move a database from m1, m2, m3, m4, m5 to n1, n2, n3, n4, n5, you can just follow the procedure for adding 
n1, n2, then remove m1, m2, etc, until the cluster lives entirely on n1...n5.  There's just one wrinkle - 
informing applications where the database lives.

To account for application configuration, the procedure for migrating the database becomes:

   1. Move half the nodes (add new node, remove old node, repeat until about half the nodes are migrated)
   1. Update the client configuration to point to the new cluster
   1. Finish moving the other half of the nodes (as in the first step)

The [Client setup](clients.html) document details configuring database clients.  It offers several choices for
configuring applications.  As a quick summary:

   * Pass location information explicitly
   * Keep a configuration file that's read by the Comdb2 API
   * Use comdb2db

Regardless which method is chosen, the Comdb2 API will try all the given nodes until it finds one that's 
accessible as ask it for cluster information.  It will then update its state of what constitutes the cluster,
and keep going (choose a node from the now-updated list of nodes, use it to connect to and answer queries).  If
no nodes are available, it'll re-query the cluster state (re-read the configuration file, re-query comdb2db).

For the three methods given above, the "update client configuration" step gets progressively easier.  If 
cluster information is passed to the API directly, it's up to the caller to discovered that database location
has changed, and to call the API with new information.  That's fine if your organization already uses some
service discovery framework.

If using configuration files for the database location, they will need to be redeployed in the "update client
configuration" step.  How that's done (some orchestration framework, copying to a hard-coded list of machines,
etc.) remains the responsibility of the application.  Again, this option is perfectly workable if your 
organization has a reliable way of distributing/generating configuration files.

If using comdb2db, the "update client configuration" step is reduced to "update comdb2db".  The API will query
comdb2db on any failure to contact known nodes and will learn the new configuration.
