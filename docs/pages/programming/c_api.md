---
title: C API
keywords: code
sidebar: mydoc_sidebar
permalink: c_api.html
---

## Comdb2 SQL API

The Comdb2 C API (lovingly nicknamed *cdb2api*) is part of the Comdb2 [main repository](https://github.com/mponomar/comdb2/tree/master/cdb2api).  
See [Client setup](clients.html) for steps needed to set up client machines to talk to databases.  
All functions and constants in this document are defined in ```cdb2api.h```.   
This API implements the protocol detailed in the section on [Writing language bindings](client_protocol.html).  
Unless otherwise noted in a 'Return Values' table, all routines return an [integer return code](#errors).   
Applications can call [cdb2_errstr](#cdb2errstr) to obtain a human-readable string describing the error.  

## A word on thread safety

cdb2api calls are threadsafe in the sense that no user-supplied locking is necessary.  
However, `cdb2_hndl_tp` objects are not protected from concurrent changes.  
Multiple threads can use multiple `cdb2_hndl_tp` objects without interference, but two threads should not operate on the same `cdb2_hndl_tp` concurrently.  

## Connecting to databases


### cdb2_open
```
int cdb2_open(cdb2_hndl_tp **hndl, const char *dbname, const char *type, int flag)
```

Description:

This routine allocates a cdb2 handle to be used by subsequent calls to [cdb2_run_statement](#cdb2runstatement).  
Note that if this call fails you can use [cdb2_errstr](#cdb2errstr) on the returned handle to find the reason for the 
failure before freeing the handle object with [cdb2_close](#cdb2close).  
[cdb2_close](#cdb2close) should be called regardless of return code from [cdb2_open](#cdb2open).  

Parameters:

|Name|Type|Description|Notes|
|-|-|-|-|
|*hndl*| input/output | pointer to a cdb2 handle | A handle is allocated and a pointer to it is written into *hndl*
|*dbname*| input | database name  | The database name to be associated with the handle
|*type*| input | cluster type | The 'stage' to connect to.  If it's set to "default" it will use the value given in ```comdb2_config:default_type``` in comdb2db config - see the section on [configuring clients](clients.html). Use "local" if target db is running on same machine as the application.  In rarely needed cases, and explicit target can be set with, eg: "dev", "alpha", "beta", etc.  The stage must be registered in your [meta database](clients.html#comdb2db).  Alternatively you can pass a [list of machines](clients.html#passing-location-information).
|*flag*| input | alloc flags | The flags to be used to allocate handle, the values allowed are 0, ```CDB2_READ_INTRANS_RESULTS```, ```CDB2_RANDOM```, ```CDB2_RANDOMROOM``` , ```CDB2_ROOM```and ```CDB2_DIRECT_CPU``` 

|Flag Value|Description|
|---|---|
|```CDB2_READ_INTRANS_RESULTS``` | insert/update/deletes return rows (num changed) inside a transaction, thus disabling an optimization where server sends replies once per transaction - see [cdb2_get_effects](#cdb2_get_effects)|
|```CDB2_ROOM``` |  Queries are sent to one of the nodes in the same data center (see section on [comdb2db](clients.html#comdb2db) to see how this is configured) |
|```CDB2_RANDOMROOM``` |  Queries are sent to one of the randomly selected node of the same data center |
|```CDB2_RANDOM``` |  Queries are sent to one of the randomly selected node of the same or different data center |
|```CDB2_DIRECT_CPU``` |  Queries are sent to the hostname/ip given in the *type* argument |


### cdb2_close
```
int cdb2_close(cdb2_hndl_tp *hndl);
```
Description:

This routine closes the cdb2 handle and releases all associated memory. 
Depending on your [client setup](clients.html) making this call may donate the database connection to a system-wide connection pool.  Only connections that don't have pending transactions or query results will be donated to the connection pool.

Parameters:

|Name|Type|Description|Notes|
|---|----|---|---|
|*hndl*| input | cdb2 handle | This is a cdb2 handle that is ready to be closed.

## Running queries


### cdb2_run_statement
```
int cdb2_run_statement(cdb2_hndl_tp *hndl, const char *sql);
```

Description:

Executes the sql query.  The query stops at the first semicolon if present - this call cannot be used to execute multiple statements.  Anything past the 
first semicolon is ignored.  To run further statements using the same handle, user code MUST call [cdb2_next_record](#cdb2nextrecord) until it returns ```CDB2_OK_DONE```
(or an error). Running another statement before all records are retrieved is not supported.  If the application doesn't need the entire result set, it's still recommended
that it reads all the rows - see notes for [cdb2_close](#cdb2close) for rationale.  It is not necessary to call [cdb2_next_record](#cdb2nextrecord) for INSERT/UPDATE/DELETE statements.

The type of the resulting columns is determined by the database.  For more control over return types, see [cdb2_run_statement_typed](#cdb2runstatementtyped)

Parameters:

|Name|Type|Description|Notes
|-|-|-|-|
|*hndl*| input | CDB2 handle | A CDB2 handle previously allocated with [cdb2_open](#cdb2open)
|*sql*| input | sql statement | The SQL query to execute

### cdb2_run_statement_typed
```
int cdb2_run_statement_typed(cdb2_hndl_tp *hndl, const char *sql,int nparms, int *parms);
```

Description:

Executes the sql query. 
This is identical to [cdb2_run_statement](#cdb2runstatement), and all the same notes apply, **except** that the database will coerce
the types of the resulting columns to the types specified in the call. 
If the types aren't compatible, the database will return ```CDB2ERR_CONV_FAIL```.  The type constants accepted are the same constants that cdb2_column_type() can return.

Parameters:

|Name|Type|Description|Notes
|-|-|-|-|
|*hndl*| input | CDB2 handle | A CDB2 handle previously allocated with [cdb2_open](#cdb2open)
|*sql*| input | sql statement | The SQL query to execute
|*nparams*| input | #params| Number of output columns
|*parm*| input | output column types| Array of types of return columns

## Reading the result set

### cdb2_next_record
```
int cdb2_next_record(cdb2_hndl_tp *hndl);
```

Description:

This routine retrieves one record from the set referred to by hndl. 
Each call to this routine retrieves the next record in the set. 
The data that is returned from this call must be extracted with the [cdb2_column_value](#cdb2columnvalue) call.

Parameters:

|Name|Type|Description|Notes
|---|---|---|---|
|*hndl*| input | CDB2 handle | This is a CDB2 handle that was returned from a successful call to [cdb2_open](#cdb2open), and passed to a successfully returning [cdb2_run_statement](#cdb2runstatement) or [cdb2_run_statement_typed](#cdb2runstatementtyped) call.

Return Values:

|Value|Description|Notes
|---|---|---|
|```CDB2_OK```| Record returned properly | Data is ready for extraction and interrogation with ```cdb2_column_*``` calls.
|```CDB2_OK_DONE```| No more records | You've already gotten all the records or none matched the search criteria to begin with.
|Other| See [error codes](#errors)

### cdb2_numcolumns
```
int cdb2_numcolumns(cdb2_hndl_tp *hndl);
```

Description:

This routine returns the number of columns referred to by the hndl.  This will match the number of columns that were selected by your SELECT statement.

Parameters:

|Name|Type|Description|Notes
|-|-|-|-|
|*hndl*| input | cdb2 handle | This is a cdb2 handle that has already successfully called [cdb2_next_record](#cdb2nextrecord)

Return Values:

|Value|Description|Notes|
|---|----|----|
|0..N| Number of columns in a row of the returned data set | Other calls in this API use column number as input |

### cdb2_column_name
```
const char * cdb2_column_name(cdb2_hndl_tp *hndl, int col);
```

Description:

This routine returns the name of a column for a given row referred to by the hndl. 
The name remains valid while the handle is open until or the next query is run.

Parameters:

|Name|Type|Description|Notes|
|---|----|---|---|
|*hndl*| input | cdb2 handle | This is a cdb2 handle that has already successfully called [cdb2_next_record](#cdb2nextrecord) |
|*col*| input | column number | This is the number of the column to be interrogated. The first column is number 0. |

Return Values:

|Value|Description|Notes|
|---|----|---|
|name| string specifying the name of the column | |

### cdb2_column_size
```
int cdb2_column_size(cdb2_hndl_tp *hndl, int col);
```

Description:

This routine returns the size of the data of a column for a given row referred to by the hndl.

Parameters:

|Name|Type|Description|Notes|
|---|----|---|---|
|*hndl*| input | cdb2 handle | This is a cdb2 handle that has already called cdb2_next_record |
|*col*| input | column number | This is the number of the column to be interrogated.  The first column is number 0. |

Return Values:

|Value|Description|Notes|
|---|----|---|
|0-N| Size of the returned column in bytes | ```CDB2_INTEGER``` columns always return ```sizeof(int64_t)```.  ```CDB2_REAL``` columns always return ```sizeof(double)```.  ```CDB2T_CSTRING``` columns return the "strlen" of the string + 1.  ```CDB2_BLOB``` columns return the length of the blob. |
|-1| Column data is NULL | |

### cdb2_errstr
```
const char * cdb2_errstr(cdb2_hndl_tp *hndl);
```

Description:

This routine returns a printable string for more information about the last error encountered by hndl. 
The string is valid until the next API call.

Parameters:

|Name|Type|Description|Notes|
|---|----|---|---|
|*hndl*| input | cdb2 handle | a cdb2 handle that has encountered an error

Return Values:

|Value|Description|Notes|
|---|----|---|
|error string| Printable string| Tells you more about what went wrong

### cdb2_column_value
```
void * cdb2_column_value(cdb2_hndl_tp *hndl, int col);
```

Description:

This routine returns a pointer to the data of a column for a given row referred to by the hndl.  The pointer is valid only until the next call to [cdb2_next_record](#cdb2nextrecord). 
This will return a NULL pointer if the column contains NULL.

A non-NULL pointer can be dereferenced to fetch the data after being cast to the correct type. 
The types are:

|Type|C type|Example cast|
|-|-|-|
CDB2_INTEGER|int64_t|```*(int64_t*)```
CDB2_REAL|double|```*(double*)```
CDB2_CSTRING|char*|```(char*)```
CDB2_BLOB|void*|```none```
CDB2_DATETIME|cdb2_client_datetime_t|```*(cdb2_client_datetime_t*)```
CDB2_DATETIMEUS|cdb2_client_datetimeus_t|```*(cdb2_client_datetimeus_t*)```
CDB2_INTERVALYM|cdb2_client_intv_ym_t|```*(cdb2_client_intv_ym_t*)```
CDB2_INTERVALDS|cdb2_client_intv_ym_t|```*(cdb2_client_intv_ym_t*)```
CDB2_INTERVALDSUS|cdb2_client_intv_dsus_t|```*(cdb2_client_intv_dsus_t*)```


Parameters:

|Name|Type|Description|Notes
|-|-|-|-|
|*hndl*| input | cdb2 handle | This is a cdb2 handle that has already successfully called [cdb2_next_record](#cdb2nextrecord)
|*col*| input | column number | This is the number of the column to be interrogated. The first column is number 0.

### cdb2_column_type
```
int cdb2_column_type(cdb2_hndl_tp *hndl, int col);
```

Description:

This routine returns the datatype of a column for a given row referred to by the hndl. 

Parameters:

|Name|Type|Description|Notes|
|---|---|---|---|
|*hndl*| input | cdb2_handle | This is a cdb2 handle that has already called [cdb2_next_record](#cdb2nextrecord) |
|*col*| input | column number | This is the number of the column to be interrogated. The first column is number 0. |

Return Values:

|Value|Description|Notes|
|---|---|---|
|`CDB2_INTEGER`, `CDB2_REAL`, `CDB2_CSTRING`, `CDB2_BLOB`, `CDB2_DATETIME`, `CDB2_DATETIMEUS`, `CDB2T_INTERVALYM`, `CDB2_INTERVALDS`, `CDB2_INTERVALDSUS`| The datatype of the numbered column | Numeric data is always promoted to its largest natural form, eg: a `short` field in the schema will come back from the db as an `int64_t`, see [cdb2_column_value](#cdb2columnvalue).|


### cdb2_bind_param
```
int cdb2_bind_param(cdb2_hndl_tp *hndl, const char *name, int type, const void *varaddr, int length);
```

Description:

This routine is used to bind value pointers with the names in cdb2 handle. The name should be present in the query as replaceable parameters. 
For example:

```c 
char *sql = "INSERT INTO t1(a, b) values(@a, @b)"

char *a = "foo";
int64_t b = "bar";

/* return code checks omitted for brevity */
cdb2_bind_param(db, "a", CDB2_CSTRING, a, strlen(a));
cdb2_bind_param(db, "b", CDB2_INTEGER, &b, sizeof(int64_t));
cdb2_run_statement(db, sql);
```

There are two very important things to remember about bound parameters:

  1. We're passing addresses of variables.  The values are copied and sent to the database at [cdb2_run_statement](#cdb2runstatement) time - it's important that these values don't go out of scope between when the binding is established and when the values are fetched.
  1. The bindings remain after the call is made.  You can populate the addresses with new values and call [cdb2_run_statement](#cdb2runstatement) again without redoing the bindings.  This is useful if you're running lots of identical statements with similar values in a loop.  Bindings should be cleared with [cdb2_clearbindings](#cdb2clearbindings) if you need to run a different statement.

It is good practice to have the names of bound parameters correspond to the columns they represent, but it is not required.

Parameters:

|Name|Type|Description|Notes|
|---|---|---|--|
|*hndl*| input | cdb2 handle | A previously allocated CDB2 handle |
|*name*| input | The name of replaceable param, max 31 characters | The value associated with this pointer should not change between bind and [cdb2_run_statement](#cdb2runstatement) |
|*type*| input | The type of replaceable param | |
|*valueaddr*| input | The value pointer of replaceable param | The value associated with this pointer should not change between bind and [cdb2_run_statement](#cdb2runstatement), and for numeric types must be signed. |
|*length*| input | The length of replaceable param | This should be the sizeof(valueaddr's original type), so 1 if it's a char, 4 for float... |

### cdb2_bind_index
```
int cdb2_bind_index(cdb2_hndl_tp *hndl, int index, int type, const void *varaddr, int length);
```

Description:

This routine is used to bind value pointers to named or unnamed replaceable params in sql statement. The index starts from 1, and increases for every new parameter in the statement. This version of cdb2_bind_* is faster than cdb2_bind_param.

For example:

```c
char *sql = "INSERT INTO t1(a, b) values(?, ?)”

char *a = "foo";
int64_t b = "bar";

cdb2_bind_index(db, 1, CDB2_CSTRING, a, strlen(a));
cdb2_bind_index((db, 2, CDB2_INTEGER, &b, sizeof(int64_t));
cdb2_run_statement(db, sql);
```

Parameters:

|Name|Type|Description|Notes|
|---|---|---|--|
|*hndl*| input | cdb2 handle | A previously allocated CDB2 handle |
| index | input | The index of replaceable param | The value associated with this pointer (valueaddr  arg) should not change between bind and [cdb2_run_statement](#cdb2runstatement) |
|*type*| input | The type of replaceable param | |
|*valueaddr*| input | The value pointer of replaceable param | The value associated with this pointer should not change between bind and [cdb2_run_statement](#cdb2runstatement), and for numeric types must be signed. |
|*length*| input | The length of replaceable param | This should be the sizeof(valueaddr's original type), so 1 if it's a char, 4 for float... |

### cdb2_get_effects
```
int cdb2_get_effects(cdb2_hndl_tp *hndl, cdb2_effects_tp *effects);
```

Description:

This routine allows the caller to to get the number rows affected, selected, updated, deleted and inserted in the last query, (or until last query from the start of the transaction.). This routine initialize the data members of the ```cdb2_effects_tp``` structure (see below), the address of which is supplied as an argument. Number of rows affected is a sum of number of rows updated, deleted and inserted.
These values only make sense after COMMIT, since the transaction may get replayed and the values for each individual statements may change.  The replay can be turned off by running ```"SET VERIFYRETRY OFF"```, after which you can call ```cdb2_get_effects``` in the middle of a transaction. 
For rationale, see the [Comdb2 transaction model](transaction_model.html)
section.

Parameters:

|Name|Type|Description|Notes |
|---|---|---|---|
|*hndl*| input | cdb2 handle | A previously allocated CDB2 handle |
|*effects*| input | The pointer to effects structure | | 

```c
typedef struct cdb2_effects_type {
    int num_affected;
    int num_selected;
    int num_updated;
    int num_deleted;
    int num_inserted;
} cdb2_effects_tp;
```

### cdb2_clearbindings
```
int cdb2_clearbindings(cdb2_hndl_tp *hndl);
```

Description:

This routine is to clear the bindings done to the handle.  See [cdb2_bind_param](#cdb2bindparam).

Parameters:

|Name|Type|Description|Notes |
|---|---|---|---|
|*hndl*| input | cdb2 handle | A previously allocated CDB2 handle |

### cdb2_use_hint
```
int cdb2_use_hint(cdb2_hndl_tp *hndl);
```

Description:

This routine enables a per-handle setting that makes the database cache statements, and allows the client code to only
send a short unique id instead of the entire SQL string. 
This may give you a small performance boost for very long SQL strings.

Parameters:

|Name|Type|Description|Notes |
|---|---|---|---|
|*hndl*| input | cdb2 handle | A previously allocated CDB2 handle |

### cdb2_set_comdb2db_config
```
int cdb2_set_comdb2db_config(char *cfg_file);
```

Description:

This function sets location of a config file the API uses to discover databases. 
See the [Client setup](clients.html) section for more details.

Parameters:

|Name|Type|Description|Notes |
|---|---|---|---|
|*cfg_file*| input | cfg_file | configuration file path |

### cdb2_set_comdb2db_info
```
int cdb2_set_comdb2db_info(char *cfg_info);
```
Description:

This functions is like [cdb2_set_comdb2db_config](#cdb2setcomdb2dbconfig), but passes the contents of the configuration instead of its location. 
This may be useful for programs that fetch database location information from other systems start databases dynamically.

Parameters:

|Name|Type|Description|Notes |
|---|---|---|---|
|*cfg_info*| input | cfg_info | configuration info buffer |


### cdb2_init_ssl
 
```c
int cdb2_init_ssl(int init_libssl, int init_libcrypto)
```

Description:

The function initializes `libssl` and `libcrypto`.  
Generally you don't need to call the function by yourself.
However if `libssl` and `libcrypto` are initialized by your application, you need to call the function with (0, 0) to prevent libcdb2api from initializing these libraries again.

Parameters:

|Name|Type|Description|Notes |
|---|---|---|---|
|*init_libssl*| input | boolean flag | Set to non-zero to let cdb2api initialize libssl. Set to 0 if libssl is initialized by the application |
|*init_libcrypto*| input | boolean flag | Set to non-zero to let cdb2api initialize libcrypto. Set to 0 if libcrypto is initialized by the application |

### cdb2_is_ssl_encrypted

```c
int cdb2_is_ssl_encrypted(cdb2_hndl_tp *hndl)
```

Description:

The function returns 1 if the handle is protected by SSL, 0 otherwise. This is useful only if the SSL mode is `ALLOW`.

Parameters:

|Name|Type|Description|Notes |
|---|---|---|---|
|*hndl*| input | cdb2 handle | A previously allocated CDB2 handle |



## Errors

### Comdb2 Return Codes

These return codes can be found in ```cdb2api.h```

### cdb2api errors

|Code    |Constant |Description
|--------|---------|-----------
| 0    |```CDB2_OK``` | <a id="CDB2_OK"/>Success. 
| 1    |```CDB2_OK_DONE``` | <a id="CDB2_OK_DONE"/>Returned by ```cdb2_next_record()``` if there are no more records in the stream. 
| -1   |```CDB2ERR_CONNECT_ERROR``` | <a id="CDB2ERR_CONNECT_ERROR"/>Unable to open TCP connection to the database.  Make sure that the database is running. 
| -2   |```CDB2ERR_NOTCONNECTED``` | <a id="CDB2ERR_NOTCONNECTED"/>Not connected to the database. 
| -3   |```CDB2ERR_PREPARE_ERROR``` | <a id="CDB2ERR_PREPARE_ERROR"/>SQLite rejected your SQL query. 
| -4   |```CDB2ERR_IO_ERROR``` | <a id="CDB2ERR_IO_ERROR"/>IO error.  This can happen if your request is cut off (the database node you are connected to may have come down for turning, or the database may have timed out your connection).  Make sure that you read records back in a timely manner without long pauses between reads. 
| -5   |```CDB2ERR_INTERNAL``` | <a id="CDB2ERR_INTERNAL"/>Internal SQL error. 
| -6   |```CDB2ERR_NOSTATEMENT ``` | <a id="CDB2ERR_NOSTATEMENT"/>Returned if you call any of the sql stream query functions (such as ```cdb2_numcolumns```) but you have not yet used ```cdb2_next_record()``` to move on to a valid record. 
| -7   |```CDB2ERR_BADCOLUMN``` | <a id="CDB2ERR_BADCOLUMN"/>Returned if you give an out of range column index to the sql api. 
| -8   |```CDB2ERR_BADSTATE ``` | <a id="CDB2ERR_BADSTATE"/>The sql handle is in an incorrect state for the operation requested e.g. ```cdb2_column_size()``` was called after all the records have been read. 
| -15  |```CDB2ERR_REJECTED``` | <a id="CDB2ERR_REJECTED"/>Database was not able to read a request from the client, or was not able to dispatch it. 
| -16  |```CDB2ERR_STOPPED``` | <a id="CDB2ERR_STOPPED"/>Database is stopped operationally and isn't accepting requests. 
| -17  |```CDB2ERR_BADREQ``` |  <a id="CDB2ERR_BADREQ"/>A request is malformed. 
| -20  |```CDB2ERR_THREADPOOL_INTERNAL``` | <a id="CDB2ERR_THREADPOOL_INTERNAL"/>Some error in threadpool code. 
| -21  |```CDB2ERR_READONLY``` | <a id="CDB2ERR_READONLY"/>Database is readonly (possible because a schema change operation is in progress). 
| -101 |```CDB2ERR_NOMASTER``` | <a id="CDB2ERR_NOMASTER"/>Database has no master node - try again later. 
| -103 |```CDB2ERR_CONSTRAINTS``` | <a id="CDB2ERR_CONSTRAINTS"/>Some constraint violation. 
| 203  |```CDB2ERR_DEADLOCK``` | <a id="CDB2ERR_DEADLOCK"/>Deadlock detected. 
| -105 |```CDB2ERR_TRAN_IO_ERROR``` | <a id="CDB2ERR_TRAN_IO_ERROR"/>I/O error. 
| -106 |```CDB2ERR_ACCESS``` | <a id="CDB2ERR_ACCESS"/>Access denied. 
| -107 |```CDB2ERR_TRAN_MODE_UNSUPPORTED``` | <a id="CDB2ERR_TRAN_MODE_UNSUPPORTED"/>Transaction mode is unsupported. 
| 2    |```CDB2ERR_VERIFY_ERROR``` | <a id="CDB2ERR_VERIFY_ERROR"/>An update failed because the record being updated was changed more recently than it was first read.  This could happen if a transaction attempts to update the same record twice, or it could happen if two concurrent transactions are trying to update the same record (one will win, and the other will lose). 
| 3    |```CDB2ERR_FKEY_VIOLATION``` | <a id="CDB2ERR_FKEY_VIOLATION"/>Foreign key violation. 
| 4    |```CDB2ERR_NULL_CONSTRAINT``` | <a id="CDB2ERR_NULL_CONSTRAINT"/>Null constraint violation. 
| 113  |```CDB2ERR_CONV_FAIL``` | <a id="CDB2ERR_CONV_FAIL"/>Data could not be inserted/updated because the provided data could not be converted to the type/size in the table definition. 
| 115  |```CDB2ERR_MALLOC``` | <a id="CDB2ERR_MALLOC"/>Malloc failed.  Check your code for memory leaks or heap corruption! 
| 116  |```CDB2ERR_NOTSUPPORTED``` | <a id="CDB2ERR_NOTSUPPORTED"/>Operation not supported. 
| 299  |```CDB2ERR_DUPLICATE``` | <a id="CDB2ERR_DUPLICATE"/>Transaction would have inserted a duplicate in a key that does not allow it. 
| 401  |```CDB2ERR_TZNAME_FAIL``` | <a id="CDB2ERR_TZNAME_FAIL"/>Invalid timezone name. 
| 300  |```CDB2ERR_UNKNOWN``` | <a id="CDB2ERR_UNKNOWN"/>Unknown error. 
| Other| Any |  Any other return code should be treated as an internal error.

### Error handling.

An application that gets an error while in a transaction (after running "```BEGIN```", before any call to "```COMMIT```" or "```ROLLBACK```") should roll back the transaction (by calling "```ROLLBACK```"). 
Any error returned by "```COMMIT```" will roll back the transaction automatically. 
A handle that has finished reading a result set, and has no active transaction can be used to run another query/statement. 
It is an error to begin a statement before the entire result from the previous statement has been read. 

"Logical" errors including ```CDB2ERR_DUPLICATE``` / ```CDB2ERR_FKEY_VIOLATION``` / ```CDB2ERR_NULL_CONSTRAINT``` / ```CDB2ERR_ACCESS``` / ```CDB2ERR_CONV_FAIL``` / 
```CDB2ERR_TZNAME_FAIL``` / ```CDB2ERR_READONLY``` / ```CDB2ERR_PREPARE_ERROR``` / ```CDB2ERR_MALLOC``` are application errors (or expected by the application).  
Any other error is a (usually transient) condition, and the database should log the error. 
See the section on [examining logs](/logs.html) for more information.

## Debugging

An application can set the `$CDB2_LOG_CALLS` environment variable to have cdb2api log all calls to stderr.
