---
title: Client setup
keywords:
tags:
sidebar: mydoc_sidebar
permalink: clients.html
---

Comdb2 was designed to run easily out of the box with minimal setup.  Databases just need to know
the names of their peers to form a cluster.  Applications just need to know the name of the database and
the application tier to connect.  This flexibility requires a little bit of configuration on the application
machine side.

Applications have 3 options for discovering where databases live.  In order of increasing complexity
and improved flexibility, they are:

  * Pass location information as part of the cdb2_open call
  * Configure database locations on a per-database basis
  * Set up a meta database that stores that information

This document covers all 3 options.  Experience of running databases at Bloomberg shows us that running
a database containing cluster setup information for other database, and setting up a DNS entry to bootstrap
the location of this meta-database is the most flexible and resilient option.

## Passing location information

The `type` argument to [cdb2_open](c_api.html#cdb2open) will treat any string that begins with '@' as a 
list of hostnames, not the name of a tier.  This makes it possible to use alternative means of looking up
database information in addition to configuration files and comdb2db.  The rest of the string as interpreted
as a comma-separated list of node specifications.  For example

@machine:port=123:dc=ZONE1,machine2:port=456:dc=ZONE2

This specifies 2 machines (machine and machine2).  Each machine optionally takes a colon separated list of 
options.  The only options currently supported are the port number (port=) and data center (dc=).  See 
[room affinity](#room) for how the dc option is used.  The port number, if omitted, will be queried 
from the specified machines.

## Client configuration files

### Destinations

The easiest option is to set up a configuration file that contains a list of machines where the database lives.
The downside is that moving databases to a different set of machines becomes a bit more difficult (see
[clustering](cluster.html) for more on that topic). 

The Comdb2 client API looks for configuration files in 2 places:

  * `$COMDB2_ROOT/etc/cdb2/config/comdb2db.cfg`
  * `$COMDB2_ROOT/etc/cdb2/config.d/$dbname.cfg`

`$COMDB2_ROOT` can bet set as an environment variable.  It defaults to `/opt/bb`.  The config file format
is simple:

    dbname machine1 machine2 machine3

    comdb2_config: option option_value
    comdb2_config: option2 option_value2
    ...

Empty lines and lines starting with '#' are ignored.

The configuration file can contain the name of the database, and which machines it lives on for the
current application tier (eg: beta, production).  Using `"default"` for `type` in the connect string passed to
[cdb2_open](c_api.html#cdb2open) will use these machines as the destination.  The database will query a machine
in the list for cluster information and learn what the rest of the machines are, so a partial list will work.
This comes in handy if a database ever needs to move.

In addition to database information, the config file can also contain settings that control how the API behaves. 
Each line containing a setting starts with `comdb2_config`.  The settings are detailed below.

### Application settings

#### default_type

This establishes the tier for the current machine.  If specified in the global config file (`$COMDB2_ROOT/etc/cdb2/config/comdb2db.cfg`)
this will establish a default value for all databases on this machine.  In a per-database config file (`COMDB2_ROOT/etc/cdb2/config.d/$dbname.cfg`)
it sets the default for the specified database.  The value is a freeform string.  Example values may be "dev", "alpha", 
"beta", "prod", etc.  Applications that use `"default"` as the `type` argument to `cdb2_open` will look for machine 
configuration for this tier (see [comdb2db](#comdb2db) for configuring multiple tiers).

#### room

The argument is a freeform string.  `cdb2_open` fetches cluster information from the database on connecting.  This information
includes which "room" (data center, availability zone, etc.) the machine is in.  `cdb2_open` will prefer to connect to 
database machines in the same room  as the current machine to reduce latency.  Room affinity of Comdb2 machines is
configured in [comdb2db](#comdb2db).

#### pmuxport

`cdb2_open` needs to learn what port the database listens on.  To do that, it talks to a service called `pmux` that runs on
database machines.  This configures what port `pmux` is listening on.  The default is 5105.

#### connect_timeout

This sets the timeout for connecting to databases.  The argument should be a number, in milliseconds. The default is 100ms.
When an attempt to connect to a node takes longer than this, the API will abort the attempt, and try another machine
in the cluster.

#### comdb2db_timeout

Similar to `connect_timeout`, but sets the timeout on querying comdb2db for cluster information.  The API will try
other available machines in turn if the cluster request fails.

#### comdb2dbname

This specifies the name of the meta database that contains information about locations of other databases.  The default
is comdb2db, and we'll call it comdb2db in the rest of this document.

#### tcpbufsz

Expects an integer argument.  This set the size of the receive buffer for database connections.  The default is unset
and will make the API use the OS default.

#### dnssuffix

As an alternative to specifying the location of comdb2db in a configuration file, it can be configured via DNS.  If the
location of comdb2db isn't present in config files, cdb2api will try to resolve the following names:

   * If no `default_type` is set, comdb2db.dnssuffix
   * If `default_type` is set, type-comdb2db.dnssuffix

For example, for this configuration file:

    comdb2_config: comdb2dbname metadb
    comdb2_config: dnssuffix dyndns.example.com
    comdb2_config: room prod

Comdb2 API will try to find the Comdb2 configuration database (called metadb) on machines returned by resolving the
hostname prod-metadb.dyndns.example.com.

## comdb2db

Comdb2db manages 3 types of resources: databases, machines, and clusters.  At the minimum, it should contains the
tables/fields listed below.  It can be extended with additional data.  For example, at Bloomberg, comdb2db contains
database size information and quotas, Comdb2 binaries for easy deployment, etc.

`cdb2_open` runs the following query to get a list of machines where a database runs:

``` SQL
   select M.name, D.dbnum, M.room 
      from 
        machines M join 
        databases D 
   where 
      M.cluster IN 
         (select cluster_machs
          from clusters 
          where 
            name=@dbname and 
            cluster_name=@cluster) and 
      D.name=@dbname 
   order by (room = @room) desc
```

Where `dbname` is the `name` argument to `cdb2_open`, `cluster` is the `type` argument, and `room` is the machine room 
(set with the `room` config file option).


### Databases

This table just contains a database name and number.  The number is only present for record keeping (it had a different
purpose in older iterations of Comdb2).  This table can be extended to contain whatever database specific information is 
relevant for your organization.

```
schema {
    cstring name[32]
    int     dbnum      dbstore=0
}

keys
{
    "KEY_NAME"  = name
}
```


### Machines

The machines table contains information on machine clusters set up to host Comdb2 databases.  A machine is a part of a
cluster.  Clusters have a name.  

```
schema
{
    // Name of machine 
    cstring     name[32]

    // Name of the comdb2 cluster that this machine is part of e.g. "C"
    cstring     cluster[32]

    // Name of the room that this machine is part of e.g. "NY"
    cstring     room[10]
}

keys
{
       "KEY_NAME"     = name
   dup "KEY_CLUSTER"  = cluster
}
```


### Clusters

The clusters table defines what cluster constitutes a given tier for a given database.  `cluster_machs` refers to
a group of machines (`cluster` field) in the machines table.  The `name` field is the name of the tier.

```
schema
{
    // Database name
    cstring     name[32]

    // A name for this tier.  Common choices are "test", "alpha", "beta",
    // "prod".
    cstring     cluster_name[32]

    // The cluster machines that this database lives on e.g. "beta" or "E"
    // (this refers to the actual machines and must exist in table machines)
    cstring     cluster_machs[32]
}

keys
{
    // Primary key
    "KEY_NAME_CLUSTER" = name + cluster_name

    // For "what databases are on this machine" type queries
    "KEY_MACHS_NAME_CLUSTER" = cluster_machs + name + cluster_name
}

```

## Example

Let's say you have a database called `somedb`.  You need to set up a development instance, a beta instance and a production
instance.  It's going to run on devdb1, devdb2, devdb3 for the dev tier, betadb1, betadb2, betadb3 for the beta
tier, and proddb1, proddb2, proddb3 for the production tier.  Let's say each machine is in one of three availability
zones (r1, r2, r3).

First, we'll set up the machines.  We are going to define 3 clusters, called prod1 (our first production cluster),
beta1 (our first beta cluster) and dev1 (our first development cluster).

``` 
insert into machines(name, cluster, room) values('proddb1', 'prod1', 'r1')
insert into machines(name, cluster, room) values('proddb2', 'prod1', 'r2')
insert into machines(name, cluster, room) values('proddb3', 'prod1', 'r3')

insert into machines(name, cluster, room) values('betadb1', 'beta1', 'r1')
insert into machines(name, cluster, room) values('betadb2', 'beta1', 'r2')
insert into machines(name, cluster, room) values('betadb3', 'beta1', 'r3')

insert into machines(name, cluster, room) values('devdb1', 'dev1', 'r1')
insert into machines(name, cluster, room) values('devdb2', 'dev1', 'r2')
insert into machines(name, cluster, room) values('devdb3', 'dev1', 'r3')
```

Now, we create the record for the database.

```
insert into databases(name) values('somedb')
```

Finally, we define the tiers for this database.

```
insert into clusters(name, cluster_name, cluster_machs) values('somedb', 'dev', 'dev1')
insert into clusters(name, cluster_name, cluster_machs) values('somedb', 'beta', 'beta1')
insert into clusters(name, cluster_name, cluster_machs) values('somedb', 'prod', 'prod1')
```

Now any machine that's configured for comdb2db access can call out to 'somedb'.  Configuring the `default_type` setting
on the machine will allow all applications to refer to the correct tier by passing "default" to `cdb2_open`.

## cdb2sockpool

cdb2sockpool is a connection pooler program.  If it's running, `cdb2_api` will get existing connections from
it instead of establishing a connection to the database.  `cdb2_close` will donate a connection to the socket pool.  
It listens on a UNIX socket for requests from applications.  It'll also read commands from /tmp/msgtrap.sockpool, which
can be used to query it for information or change settings.

### Commands

#### exit

Shuts down cdb2sockpool.  Running programs will not be able to donate connections back to the pool, but will
continue working.

#### stat clnt

Display statistics about connected applications.


#### stat pool

Display statistics about pooled connections.


#### stat port

Display statistics about cached database port information.

#### stat

Display general statistics.

#### closeall

Closes all available connections.


#### purgeports

Forgets all cached port information.

#### dumphints

Displays cached information.

#### sethint

Takes a "type string" (see output of dumphints for examples) and a port number.  Sets the port number remembered for
the given input string.
