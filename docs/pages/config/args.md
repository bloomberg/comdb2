---
title: Command Line Arguments
tags:
sidebar: mydoc_sidebar
permalink: args.html
---

The general form of the Comdb2 command line is

```
      comdb2 [--lrl LRLFILE] [--recovertotime EPOCH]
             [--recovertolsn FILE:OFFSET]
             [--fullrecovery] NAME
  
      comdb2 --create [--lrl LRLFILE] [--dir PATH] NAME
  
       --lrl                      specify alternate lrl file
       --fullrecovery             runs full recovery after a hot copy
       --recovertolsn             recovers database to file:offset
       --recovertotime            recovers database to epochtime
       --create                   creates a new database
       --dir                      specify path to database directory
  
       NAME                       database name
       LRLFILE                    lrl configuration file
       FILE                       ID of a database file
       OFFSET                     offset within FILE
       EPOCH                      time in seconds since 1970
       PATH                       path to database directory
```
Options are optional

## Specifying the database directory

### Using --lrl

By default, the database will look in the standard install path for a database with the given name.  That defaults
to `${COMDB2_ROOT}/var/cdb2/${dbname}`.  If that directory contains a `${dbname}.lrl` file, the database will use 
it to determine its startup options.  The database will also look in the current directory for a `${dbname}.lrl` 

The database can be directed to use a specific lrl file with the `-lrl` option. It takes a path to the lrl file as
an argument.

### Using --dir

If the lrl file isn't found, and isn't specified, the database will try to start with default options, from the default
location.  That's still `${COMDB2_ROOT}/var/cdb2/${dbname}` followed by the current directory.  If these locations
contain a database, it'll try to start with default options.  The directory can be specified with the `-dir` option.
It takes a path to the database directory as an argument.

## Creating a new database

### Using --create

A new database must be created with the `--create` option.  If a directory is specified with `--dir`, 
the database will be created there.  If it isn't, Comdb2 will use the value of a `COMDB2_DB_DIR` environment
variable, if it's set.  Otherwise, it'll try to create the database in the default directory 
`${COMDB2_ROOT}/var/cdb2/${dbname}`.

If an lrl file is specified with the `--lrl` option, the database will apply options it.  Note that some options
can only be specified at [creation time](config_files.html#init-time-options). If no lrl file is specified, 
the database will create an `${dbname}.lrl` file in the database directory with the default options.  Running 
`comdb2 ${dbname}` will start this database (see [Specifying the database directory](#specifying-the-database-directory)).

The database has some options to guide recovery at startup time.

### --fullrecovery

Attempts to recover from the earliest available checkpoint.  Recovery normally starts at the latest checkpoint.

### --recovertotime

Takes an integer epoch time as an argument.  Unrolls all transactions that started later than this timestamps, even
if they committed.  This is useful for unrolling a database to a state as of a given timestamp.

### --recovertolsn

Takes an LSN in a file:offset format.  Unrolls all transactions that haven't committed as of that LSN.
