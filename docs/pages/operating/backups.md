---
title: Backups
keywords: code
sidebar: mydoc_sidebar
permalink: backups.html
---

## copycomdb2

A very simple way to backup a database is to use the `copycomdb2 -b /path/to/db/lrl` flag.  `copycomdb2 -b` will compress the serialized database before sending it to stdout.
copycomdb2 -b may be used against a running or halted database.  To ensure a stable copy, running databases will halt logfile deletion while a copy is running against it.

```
copycomdb2 -b /db/comdb2/customerdb.lrl > /db/backups/customerdb.$(date +%Y%m%d%H%M%S).lz4
``` 
Saves a compressed copy of the database described by customerdb.lrl.

The copycomdb2 restore flag `copycomdb2 -r /desired/path/to/lrl /desired/data/dir` decompresses and unpacks a database which was previously packaged with `copycomdb2 -b`.

```
copycomdb2 -r /db/comdb2/lrl/ /db/comdb2/customerdb/ < /db/backups/customerdb.20170202083014.lz4
``` 
Restores customerdb, placing its lrl file in /db/comdb2/lrl/ and its data files in /db/comdb2/customerdb/.  


## comdb2ar

The comdb2ar utility is used by copycomdb2 to serialize and deserialize databases. 
To serialize, comdb2ar requires at minimum the lrl file of a database. 
The serialize mode produces a serialized stream which the user would typically redirect to a file or pipe to a compression utility. 
To deserialize, comdb2ar requires a serialized stream (redirected from a file or a pipe), the desired location of the database's lrl file and the desired location of the database's data files. 
The `-C strip` / `-C preserve` arguments are used to either strip or preserve the lrl's cluster directive. 
A user should specify `-C preserve` if the intention is to grow the cluster of an existing database instance. 
A user should specify `-C strip`  to create a decoupled, standalone instance of a database.  

```
lz4 -d stdin stdout < /db/backups/customerdb.20170202083014.lz4 | comdb2ar x /db/customerdb /db/customerdb
```

Deserializes `/db/backups/customerdb.20170202083014.lz4`, placing both the lrl files and data files in the
`/db/customerdb` directory.
