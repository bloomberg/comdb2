---
title: Logs
keywords: code
sidebar: mydoc_sidebar
permalink: logs.html
---

## Setting the Log Location

The default comdb2 root directory is configured at build-time, but can be 
overridden at runtime via the `$COMDB2_ROOT` environmental variable. In the
absence of an override, the default logfile directory will be:

```
$COMDB2_ROOT/var/log/cdb2/<dbname>.<logname>
```

The logfile directory can be overridden in a database's lrl file via the 
`location logs /path/to/logs/` directive.

A simple example: consider a database named `customerdb`. If the `$COMDB2_ROOT`
environmental variable is set to `/opt/comdb2`, then the database will create
the following three logfiles upon starting:

```
/opt/comdb2/var/log/cdb2/customerdb.trc.c
/opt/comdb2/var/log/cdb2/customerdb.statreqs
/opt/comdb2/var/log/cdb2/customerdb.longreqs
```

If a user wishes to change the location of these logfiles to `/var/logs/comdb2`,
they should add the following directive to the customerdb database's lrl file:

```
location logs /var/logs/comdb2
```

This will create the following three logfiles:

```
/var/logs/comdb2/customerdb.trc.c
/var/logs/comdb2/customerdb.statreqs
/var/logs/comdb2/customerdb.longreqs
```

[//]: <> ## stdout/stderr
[//]: <> TODO - i don't know if we are going to redirect to stdout / stderr 
[//]: <> after opensource.

## Long Requests

Long requests are collected under comdb2's request-logging subsystem and 
reported in the database's `<dbname>.longreqs` file.  SQL statements which 
take longer than the threshold to complete are reported in the long-requests 
file. The long-requests file additionally reports other information which may 
have impacted the request. The default long-request reporting threshold is 5000
milliseconds. This value can be changed dynamically at runtime via the cdb2api,
or the cdb2sql command line utility:

```
cdb2sql customerdb local "exec procedure sys.cmd.send('reql longsqlrequest 0')"
```

Will set the long-request threshold to 0 milliseconds, which will have the 
effect of logging all of the incoming sql requests.

## Status Log

Comdb2 records global request status information for a database in its 
`<dbname>.statreqs` file.  Unlike the long requests file, which reports its 
information per-request, the statreqs file is updated every minute with 
global information (i.e., the number of new requests, the number of
additional log bytes, IO information, etc).


