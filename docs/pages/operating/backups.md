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

## Incremental Backups

Operators can use the comdb2 archive utility (comdb2ar) to create a full "increment-mode" backup, and then subsequently, to create any number of incremental backups.
Incremental backups are overlaid in-order on top of the original full-backup to restore the database to its state as of the most recent increment.
The comdb2ar utility determines which pages to store in an incremental backup by creating and updating an increment-work directory.
The increment-work directory contains a per-btree list of page-checksums as of the most recent incremental-backup (or full-incremental-backup).
The comdb2ar utility compares this per-btree list of checksums against the running database to determine which btree pages an increment should contain.
An incremental-backup contains a stable set of btree pages, and all of the database logfiles accrued while the increment was being generated.

```
comdb2ar c -I create -b /backup/increment_work/userdb /usr/database/userdb/userdb.lrl > /backup/userdb/userdb.fullbackup.tar
```

This command saves a full increment-mode backup of userdb to /backup/userdb/userdb.fullbackup.tar, storing
userdb's page-checksum information in the increment-work directory "/backup/increment\_work/userdb".

```
comdb2ar c -I inc -b /backup/increment_work/userdb /usr/database/userdb/userdb.lrl > /backup/userdb/userdb.increment_1.tar
```

This command saves an incremental backup of userdb to /backup/userdb/userdb.increment\_1.tar.
It uses the page-checksum information in /backup/increment\_work/userdb to determine what btree-pages should be saved in this increment.
It additionally updates the page-checksum information in /backup/increment\_work/userdb to reflect the currently running database.

```
comdb2ar c -I inc -b /backup/increment_work/userdb /usr/database/userdb/userdb.lrl > /backup/userdb/userdb.increment_2.tar
```

This command creates an additional increment in the same manner as the first.
This increment will contain the pages which have changed since the first increment (userdb.increment\_1.tar) was produced.

```
cat /backup/userdb/userdb.fullbackup.tar /backup/userdb/userdb.increment_1.tar /backup/userdb/userdb.increment_2.tar | comdb2ar x -I restore /usr/restore/userdb/ /usr/restore/userdb/
```

This command restores userdb to its state as of the final increment (userdb.increment\_2.tar) to /usr/restore/userdb/.
