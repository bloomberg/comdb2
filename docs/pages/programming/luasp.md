---
title: Stored procedures
keywords: code
sidebar: mydoc_sidebar
permalink: storedprocs.html
---

## Lua Stored Procedure API

The stored procedure language in Comdb2 is a heavily extended dialect of the
Lua 5.1 programming language. Documentation for the base language can be found
at [http://www.lua.org](http://www.lua.org).  Comdb2 extensions include additional base data types,
a strongly typed dynamic type system, a pthreads-like threading model, and
more.

Stored procedures run with implicitly set [```SET VERIFYRETRY off```](sql.html#set-verifyretry)

## Stored procedure basics

### Invoking a Stored Procedure

Run SQL query ```EXEC PROCEDURE name(args...)``` as a regular SQL query. Consider the following invocation of SP named `foo`:

```
exec procedure foo(1, @bound_arg, 'hello')
```

```lua
local function main(a, b, c)
end
```

`main()` will receive 3 arguments, `a` (Lua number with value 1), `b` (type and value of bound parameter) and `c` (Lua string containing "hello").


## Stored Procedure entry point function: main()

Stored procedure execution begins at the ```main()``` function. The main function (like other Lua functions) can take any number of arguments. Because Comdb2 SP disallows globals, these must be declared local. Following is an example of `main()` taking 2 arguments:

```lua
local function main(name, value)
    db:column_name(name, 1)
    db:emit(value)
end
```

If this is `main()` for SP named ex1, it can be executed by:

```
$ cdb2sql testdb default "exec procedure ex1('mycol', 'myval')"
(mycol='myval')
```


## Function argument types

Function definitions can specify argument types. The system takes care of doing the necessary type conversions 
when the function is called. The type can be any of the Comdb2 types. Failure to perform type conversion is a 
runtime error and the SP execution is terminated.

```lua
local function func(i int)
    print("In func")
    print(i, type(i))
end
```

```lua
local function main(x int, y real, z cstring)
    print("In main")
    print(x, type(x))
    print(y, type(y))
    print(z, type(z))
    func(y) -- real -> int
end
```

We will see the following output if we execute the SP as:

```
$ cdb2sql testdb default "exec procedure foo('10', '3.14', 'hello')"
In main
10      int
3.14    real
hello   cstring
In func
3       int
```

If type conversion cannot be performed:

```
$ cdb2sql testdb default "exec procedure foo ('abc', '3.14', 'hello')"

[exec procedure foo ('abc', '3.14', 'hello')] failed with rc -3 conversion to: int failed from: abc
```

## The `declare` statement

Use the `declare` keyword to declare variable types. Use the `:=` operator to assign values while preserving 
type. On a `:=` assignment, the system will convert the value to the declared type.

Multiple variables can be declared to be of the same type in one statement, separated by commas. A declared 
variable is initialized with NULL.

```lua
local function assign(x, y)
    x := y
    print(x, type(x), y, type(y))
end

local function main()
    local str = '10'
    local num = 5
    local pi = 3.14
    declare i 'int'

    assign(i, str)
    assign(i, num)
    assign(i, pi)           -- truncates to 3

    i = 'hello'             -- regular assignment
    print(i, type(i))       -- loses type

    declare a, b, c "int"   -- multiple local int variables
    print(b, type(b))       -- initialized to NULL
end
```

This stored procedure will output the following:

```
10      int     10      string
5       int     5       number
3       int     3.14    number
hello   string
NULL    int
```

The type specified as part of a declare statement can be any expression which evaluates to a valid comdb2 type. All of the following are valid:

``` lua
local function gettype()
    return 'cstring'
end

local function main(argval, argtype)
    print(argval, type(argval))

    declare v argtype
    v := argval
    print(v, type(v))

    declare x gettype()
    x := v
    print(x, type(x))
end
```

We will see the following output if we execute the SP as:

```
$ comdb2sql testdb "exec procedure foo('int', '3.14')"

3.14    string
3       int
3       cstring
```


## Comdb2 types in Lua tables

We can broadly classify the commonly used datatypes as:

| Type Class | Includes
|------------|----------------------------------------------------
|  Numeric   | Lua number, Comdb2 int, Comdb2 real, Comdb2 decimal
|  String    | Lua string, Comdb2 cstring
|  Others    | Lua tables, Other Comdb2 types

Keys in a table will be successfully found using keys of another type if it has the same type-class and 
the same value. For example, `t[1]` can be found by any Numeric key as long as its value is equal to `1`. 
Similarly, Lua strings will find Comdb2 cstring keys and vice versa. Example:

```
local function main()
    local d2s = {}
    table.insert(d2s, 'Sun')
    table.insert(d2s, 'Mon')
    table.insert(d2s, 'Tue')
    table.insert(d2s, 'Wed')
    table.insert(d2s, 'Thu')
    table.insert(d2s, 'Fri')
    table.insert(d2s, 'Sat')

    -- Generate some ints and reals
    local stmt = db:exec([[
      select 1 as 'd' union
      select 2 as 'd' union
      select 3 as 'd' union
      select 4 as 'd' union
      select 5 as 'd' union
      select 6.0 as 'd' union
      select 7.0 as 'd' ]])

    local row = stmt:fetch()
    while row do
        print(d2s[row.d])
        row = stmt:fetch()
    end 
end
```

This will print all the days of the week from Sun - Sat. In the above example the table `d2s` is an array (indexed 
by Lua numbers 1-7). In the while loop we're able to find the correct entries even though we search using 
Comdb2 `int`s and `real`s.

In the following example, observe how the type of keys change when updated using keys of the same type class and same value:

```
local function main()
    local t = {}
    local hello = 'hello'
    local ten = 10
    declare c 'cstring'
    c := hello
    declare i 'int'
    i := ten

    print('Store using Lua types')
    t[ten] = 99
    t[hello] = 'world'
    for k, v in pairs(t) do
        print(k, type(k), ' > ', v, type(v))
    end

    print('\nUpdate using Comdb2 types')
    t[i] = 999
    t[c] = 'world!'
    for k, v in pairs(t) do
        print(k, type(k), ' > ', v, type(v))
    end
end
```

```
Store using Lua types
10      number   >    99      number
hello   string   >    world   string

Update using Comdb2 types
10      int      >    999     number
hello   cstring  >    world!  string
```

When a Lua table is used as an array the keys are not saved. They are simply an index into an array of values. 
When iterating using `ipairs` the keys will be Lua numbers even if they are saved/updated using any other 
Numeric type. Example:

```
local function main()
    local t = {}

    declare i 'int'
    i := 0

    for _ = 1, 3 do
        i = i + 1
        t[i] = i
    end

    for k, v in ipairs(t) do
        print(k, type(k), ' > ', v, type(v))
    end
end
```

```
1       number   -->    1       int
2       number   -->    2       int
3       number   -->    3       int
```


### dbtable

Description:

This is a reference to an SQL table. A dbtable can represent a "base table" that exists in your schema, or a temporary SQL table.
* A dbtable from base tables is produced by the `db:table()`. A dbtable provides various methods documented in [Methods on dbtable](#operating-on-a-dbstmt). A dbtable is the only way to pass SQL temp tables from main thread to children threads. These can then be worked on concurrently.
* `db:table("t")` produces a dbtable on underlying Comdb2 table "t".
* `db:table("tmp", {<schema>})` produces a dbtable on a temporary SQL table.


### dbstmt

Description:

A `dbstmt` is a handle to an SQL statement. Use the `dbstmt` object to bind values to the underlying prepared statement (produced by `db:prepare()`) and for running the statement and fetching query result (`dbrow`s). `db:exec` also returns a `dbstmt` which can then be used to step through the result set, similar to a prepared statement.


### dbrow

Description:

This is a Lua "table" consisting of the result of the dbtable:fetch method.
It corresponds directly to one "row" from the underlying relational table.  As a lua table,
columns "a" "b" "c" in resultrow "r" would be accessed as "r.a" "r.b" "r.c"
* A dbrow can be formed manually as a Lua table as such: {'column1='abc','column2='123'}


### dbthread

Description:

This is used to identify the thread created by `db:create_thread` method. The only method available to this is
join which returns the values returned by function passed in `db:create_thread` method.


### dbconsumer

Description:
A dbconsumer object is handle to the underlying consumer. The only way to obtain this object is by calling `db:consumer()` in a Lua consumer. This is not available in any other stored procedures. Use this object to consume events from the queue as well as perform a blocking emit to calling client.

## Creating threads

### db:create_thread 

```
dbthread = db:create_thread(func, ...)
```

Description:

This method is used to create a new thread by invoking the function given as the first parameter, and rest of 
the parameters as the parameter to the function.  The result is a dbthread which supports join method.

Return Values:

|Name          | Description
|--------------|------------------------------------------------------------
|*dbthread*    | This datatype contains information about the thread created

Parameters:

|Name                | Description
|--------------------|-----------------------------------------------------
|  *func*            | The function to be run in new thread
|  *args* (multiple) | The variable number of args to be passed in function


### dbthread:join

```
ret1,ret2,... = dbthread:join()
```

Description:

This method suspends execution of calling thread until target thread is terminated.

Return Values:

|Name                | Description
|--------------------|--------------------------------------------------------
|*ret*  (multiple)   | The return from the function passed in `create_thread`

```
local function myfunc(a)
  local result, rc = db:exec("insert into table1 values ("..a..")")
  return
end

local function main(t)
  local th = db:create_thread(myfunc, 0)
  local th1 = db:create_thread(myfunc, 1)
  local th2 = db:create_thread(myfunc, 2)
  local th3 = db:create_thread(myfunc, 3)
  th:join()
  th1:join()
  th2:join()
  th3:join()
  return 0
end
```




## Executing SQL to produce a dbstmt

### db:exec

```
db:table, rc = db:exec(query)
```

Description:

This method creates an anonymous dbtable backed by the dynamic SQL query specified.  The resulting dbtable
is fully equivalent to a dbtable referencing a base table, supporting all of the same methods.  Use of DDL
statements is not allowed.

Return Values:

|Name                     | Description                                                   |  Notes 
|-------------------------|---------------------------------------------------------------|-------------------------------------------
|*dbtable*                | anonymous table of rows returned from query                   | The relational table produced by the query   
|*rc*                     | non zero is failure                                           | The return code                              

Parameters:

|  Name        | Description
|--------------|------------
|  *query*     | The SQL string


### db:prepare

```
dbtable, rc = db:prepare(query)
```

Description:

This method creates an anonymous dbtable backed by the SQL query specified.  The resulting dbtable
is the same as dbtable referencing a base table, supporting all of the same methods.
This method differs from db:exec in that the query plan of the specified query is automatically cached, 
and replaceable parameters are used with the <dbtable>:bind call.  The syntax of the SQL uses the *?* character
as a placeholder for a replaceable parameter. Maximum of 2048 parameters are allowed in a query.

Return Values:

|Name           | Description                                  |  Notes
|---------------|----------------------------------------------|-------------------------------------------
|*dbtable*      | anonymous table of rows returned from query  | The relational table produced by the query   
|*rc*           | non zero is failure                          | The return code                              


Parameters:

|  Name          |  Description               |   Notes
|----------------|----------------------------|------------------------------------------------------------
|  *name*        |  name of the column        |                                                 
|  *query*       | The SQL string             |   *?* used to specify placeholder for replaceable parameter              


## Operating on a dbstmt

### dbstmt:bind by name

```
rc = dbstmt:bind(varname, value)
```

Description:

This method binds a Lua variable to an SQL variable specified by the *@varname* in the SQL string
specified to the ```db:prepare``` method.

Return Values:

|Name           | Value   | Description
|---------------|---------|------------------------
|*rc*           | non zero| failed to bind variable
 
Parameters:

|  Name      | Description
|------------|------------------------------------------------------------
|  *varname* | name of the parameter to bind to
|  *value*   | A Lua variable containing the value to be used in the query


### dbstmt:bind by position

```
rc = dbstmt:bind(position, value)
```

Description:

This method binds a Lua variable to an SQL variable specified by the *?* character present in the SQL string
specified to the db:prepare method.

Return Values:

|Name           | Value   | Description
|---------------|---------|------------------------
|*rc*           | non zero| failed to bind variable
 
Parameters:

|  Name       | Description                                                 | Notes
|-------------|-------------------------------------------------------------|--------
|  *position* | which parameter to bind to                                  | 1 based  
|  *value*    | A Lua variable containing the value to be used in the query |   


### dbstmt:exec

```
rc = dbstmt:exec()
```

Description:

This method executes the sql insert, delete and update query, prepared by db:prepare method.

Return Values:

|Name|Value|Description
|----|-----|-----------
|*rc*| non zero| failed to execute sql query 

An example illustrating how to use `db:prepare()`

```
local t
t = db:prepare("INSERT INTO t(c1, c2) values(?,?)")
t:bind(1, 1)
t:bind(2, 2)
t:exec()

t = db:prepare("INSERT INTO t(c1, c2) values(@i, @j)")
t:bind(i, 3)
t:bind(j, 4)
t:exec()
```

This will insert two rows `(1, 2)` and `(3, 4)` into table `t` with 2 columns `i` and `j`.


### dbstmt:emit

```
rc = dbstmt:emit()
```

Description:

This method emits the rows returned by the underlying sql statement.

|Name                | Description
|--------------------|------------
| *rc*          | non zero is failure


### dbstmt:fetch

```
dbrow = dbstmt:fetch()
```

Description:

A call to this method will produce a dbrow from the dbtable. When all rows of the dbtable are consumed, `nil` is returned. Before executing a new sql statement either all the rows must be consumed  by calling `fetch()` in a loop, or calling `emit()`, or calling `dbtable:close()`.

Return Values:

|Name                | Description
|--------------------|----------------------------
|*dbrow*        | the row returned from the dbtable
 
Parameters:
none


### dbstmt:close

```
dbrow = dbstmt:close()
```

Description:

Closes dbstmt's underlying sql statement.


### dbstmt:rows_changed

```
num = dbstmt:rows_changed
```

Description:

This method give the number of rows changed by a query.

Return Values:

|Name                | Description                       
|--------------------| -----------------------------------
|*num*        | number of rows change              

Parameters:
none


### dbstmt:column_count

```
num = dbstmt:column_count()
```

Description:

This method returns the number of columns in the result set (`dbrow`) returned by the dbstmt.

Return Values:

|Name                | Description
|--------------------| -----------------------------------
|*num*        | number of columns returned by dbstmt

Parameters:
none


### dbstmt:column_name

```
name = dbstmt:column_name(col)
```

Description:

This method returns the name assigned to a particular column in the result set of a dbstmt.

Return Values:

|Name                | Description
|--------------------| -----------------------------------
|*name*        | name of column returned by dbstmt

Parameters:

|  Name               |  Description
|---------------------|---------------------------------------
|  *col*        |  column number (leftmost column number is 1)


### dbstmt:column_origin_name

```
name = dbstmt:column_origin_name(col)
```

Description:

This method returns the table column that is the origin of a particular column in dbstmt. This does not work if column is an expression or a function.

Return Values:

|Name                | Description
|--------------------| -----------------------------------
|*name*        | Originating table-column of column returned by dbstmt

Parameters:

|  Name               |  Description
|---------------------|---------------------------------------
|  *col*        |  column number (leftmost column number is 1)


### dbstmt:column_table_name

```
name = dbstmt:column_table_name(col)
```

Description:

This method returns the table that is the origin of a particular column in dbstmt. This does not work if column is an expression or a function.

Return Values:

|Name                | Description
|--------------------| -----------------------------------
|*name*        | Originating table-name of column returned by dbstmt

Parameters:

|  Name               |  Description
|---------------------|---------------------------------------
|  *col*        |  column number (leftmost column number is 1)


## Operating on a dbtable

### db:table with name

```
dbtable, rc = db:table(name)
```

Description:

This method produces a dbtable which references an already existing SQL base table.

Return Values:

|Name                | Description                                |Notes
|--------------------|--------------------------------------------|-----
|*dbtable*      | the dbtable referencing the SQL base table |              
|*rc*           | non zero                                   | failed to produce dbtable
 

Parameters:

|  Name               |  Description          
|---------------------|-----------------------
|  *name*        |  name of the column                                                         


### db:table with name and schema

```
dbtable, rc = db:table(name, {schema})
```

Description:

This method creates a new SQL temporary table. The schema for this table is specified by passing in a Lua array which has one element for each column in the temp table. Each of these is itself an array of two elements which specifies column name and column type. `{ {'name1', 'type1'}, {'name2', 'type2'}, ...}`. The type can be any of the SQL types listed <a href="#lua-data-types">here</a>. The lifespan is the scope of the store procedure execution.

The temporary tables are not transactional - `db:begin()`, `db:commit()`, `db:rollback()` do not apply to operations performed on temporary tables. These allow concurrent access and are effective way to transfer data between threads in a stored procedure. The `main` thread creates temporary tables, and passes the `dbtable` handle to several threads. The threads perform concurrent operations on the temporary tables and will see each others' side effects (as will the main thread.)

Return Values:

|Name                | Description                       |Notes
|--------------------|-----------------------------------|-----
|*dbtable*        | the dbtable referencing the SQL base table|
|*rc*           | non zero| failed to produce dbtable

Parameters:

|  Name               |  Description               |                           Notes
|---------------------|----------------------------|--------------------------------
|  *name*        |  name of the column        |
|  *schema array*    |  schema as a Lua array |  array of arrays

Example:

```
local tt = db:table('tmpname', { {'id', 'integer'}, {'j', 'integer'}})
tt:insert({id=5, j=55})
tt:emit()
```

Running this yields:

```
$ cdb2sql testdb default "exec procedure temp()"
(isss=5, j=55)
```


### dbtable:insert

```
rc = dbtable:insert(dbrow)
```

Description:

This method inserts a single dbrow into a dbtable.

Return Values:

|Name           |     Description  |Notes                     
|---------------|------------------|--------------------------
|*rc*           | non zero         | failed to insert row     
 
Parameters:

|  Name            |  Type
|------------------|------
|  *dbrow*         | input


### dbtable:copyfrom

```
rc = dbtable:copyfrom(dbtable, whereclause)
```

Description:

This method appends the table constructed by the where clause on the input dbtable to the calling dbtable.

Return Values:

|Name           | Value                              | Description 
|---------------|------------------------------------|------------
|*rc*           | non zero                           | failed to insert row
 
Parameters:

|  Name          |         Description                |                     Notes
|----------------|------------------------------------|--------------------------
|  *dbtable*     | This dbtable is used as the source |                          
| *where clause* | optional input                     | An SQL where clause used as a filter 


### dbtable:name

```
name = dbtable:name()
```

Description:

This method returns a string consisting of the name of a dbtable.  For base SQL tables, the name of the
dbtable is the name of the SQL base table.  For dbtables constructed from the output of running SQL, the
name is the SQL string that generated the dbtable.

Return Values:

|Name           |      Description                  |                        Notes                     
|---------------|-----------------------------------|--------------------------------------------------
|*name*         |  a Lua string                     | The name of the dbtable in the SQL namespace     
 

### dbtable:emit

```
rc = dbtable:emit()
```

Description:

This method emits the contents of a dbtable to the calling SQL client.  

Return Values:

|Name                | Description
|--------------------|------------
| *rc*          | non zero is failure


### dbtable:where

dbtable, rc  = dbtable:where(where clause)

Description:

This method produces a dbtable consisting of the contents of the invoking dbtable filtered through an SQL where clause.

Return Values:

|Name                | Description  
|--------------------|--------------
|  *dbtable*    | The dbtable produced
| *rc*          | non zero is failure 

Parameters:

|Name             | Description      |           Notes             
|-----------------|------------------|------------------------------------------
|*where clause*   | optional input   |      An SQL where clause used as a filter         


## Lua Data Types

| Lua  Data Type       |  SQL Data Type |     CSC2 Data Type
|----------------------|----------------|--------------------------------------
| real                 |  real          |  float, double                         
| int                  |  integer       |  short, u_short, int, u_int, longlong  
| cstring              |  text          |  cstring, vutf8                        
| blob                 |  blob          |  byte[], blob                          
| decimal              |  decimal       |  decimal32, decimal64, decimal64       
| datetime             |  datetime      |  datetime, datetimeus                              
| intervalym           |  intervalym    |  intervalym                            
| intervalds           |  intervalds    |  intervalds, intervaldsus                            


### datetime

`datetime` objects have the following member functions:

|Function         | Purpose
|-----------------|--------
|`change_timezone`| change timezone of the datetime object
|`to_table`       | convert to a Lua date table (<https://www.lua.org/pil/22.1.html>)


`datetime` objects have the following properties:

|Property   | Description
|-----------|--------------
|`year`     |
|`month`    |
|`day`      |
|`yday`     | day of year
|`wday`     | day of week
|`hour`     |
|`min`      |
|`sec`      |
|`msec`     |
|`usec`     |
|`isdst`    | bool
|`timezone` |


Following sample show using datetimes

    local function main()
        declare n 'datetime'
        n := "2016-01-01 America/New_York"
        print(n.year, n.month, n.day, n.yday, n.wday, n.hour, n.min, n.sec, n.msec, n.isdst, n.timezone)
        n:change_timezone('Europe/London')
        print (n, n.year, n.month, n.day, n.yday, n.wday, n.hour, n.min, n.sec, n.msec, n.isdst, n.timezone)
    end

    2016-01-01T000000.000 America/New_York  2016    1       1       1       6       0       0       0       0       false   America/New_York
    2016-01-01T050000.000 Europe/London     2016    1       1       1       6       5       0       0       0       false   Europe/London


### type
```
typename = type(object)
```

Description:

This Lua built in is extended to return type names for Comdb2 data types as well as base Lua data types.
This string is suitable for use as input to the `db:cast()` method.

Return Values:

|Name         |   Description             |Notes
|-------------|---------------------------|-----
|*type string*  |  a string representing the type   | suitable for input to db:cast 

Parameters:

|  Name      |   Description|Notes
|------------|--------------|-----
|  *object*| Any Lua object| handles both base Lua types, and extended Comdb2 Lua types  


### db:cast
```
objectout = db:cast(objectin, type string)
```

Description:

Produce a new object which is a type cast version of the input object to the specified type.

Return Values:

|Name         | 
|-------------|
|  *objectout*|
 
Parameters:

|  Name      |   Description|Notes
|------------|--------------|-----
|  *objectin*| Any Lua object| handles both base Lua types, and extended Comdb2 Lua types  
|*type string*  |  a string representing the type   | e.g. "int" 


### db:copyrow

```
local rowout = db:copyrow(rowin)
```

Description:

Produce a new row which is deep copy of input row.

Return Values:

|Name      |   
|----------|
|  *rowout* |
 
Parameters:

|  Name      |   Description
|------------|--------------
|  *rowin*| Any dbtable row 


### db:isnull

```
boolean db:isnull(dbtype)
```

Description:

Checks if a comdb2 datatype object is null

Return Values:

|Name         |   Description
|-------------|-------------
|*type boolean*  |  true or false


### db:setnull

```
db:setnull(dbtype)
```

Description:

Set the comdb2 datatype object to null.


## Managing Transactions

By default, the entire stored procedure executes as one transaction. All changes performed by the procedure are 
committed when `main` returns.  The stored procedure can be part of larger transaction which executes other 
SQL statements and other stored procedures. All changes will commit when the client program calls commit. This 
would look something like the following pseudo code snippet --

```
    cdb2_run_statement('begin')
    cdb2_run_statement('exec procedure tran1()')
    cdb2_run_statement('insert stmt')
    cdb2_run_statement('update stmt')
    cdb2_run_statement('exec procedure tran2()')
    cdb2_run_statement('commit')
```

Couple of limitations when running inside a client transaction (like the one shown above):

1. Stored procedure cannot start threads.
2. Stored procedure cannot explicitly control transactions (by calling `db:begin`, `db:commit` and `db:rollback`).


### db:get_trans

```
intrans, isolation string = db:get_trans()
```

Description:

This method determines at runtime if the stored procedure has been invoked within the scope of a larger
transaction by the calling SQL session.  As transactions cannot be nested in Comdb2, this routine can
be used to determine if it is safe to begin a transaction inside the stored procedure if one is needed.
The isolation level of a transaction in Comdb2 can not be changed within the scope of a running transaction,
so a defensive practice would be to query for the minimum isolation level required by the stored procedure
code and abort if the requirements have not been met.

Return Values:

|Name         |   Description             |Notes
|-------------|---------------------------|-----
|*intrans*  |   boolean     | True if in a transaction 
|*isolation string*  |   string     | one of "BLOCKSQL", "READ COMMITTED", "SNAPSHOT ISOLATION"  
 

### db:begin

```
rc = db:begin()
```

Description:

This method begins a transaction inside the stored procedure.  It runs in the isolation level inherited
by the invoking SQL session. It is an error to begin a transaction when the invoking SQL session
has invoked the stored procedure inside of a transaction.

Return Values:

|Name         |   Description         
|--------------|--------------
|*rc*  |   non zero is failure    
 

### db:commit

```
rc = db:commit()
```

Description:

This method commits a transaction that was started inside the same stored procedure with the db:begin method.  It is an error to attempt to commit any transaction not begun in that manner. db:error() should give the failure message on commit.

Return Values:

|Name    |   Description
|--------|----------------
|*rc*    |   non zero is failure

Compare `rc` with the following named values to determine why `db:commit` failed:


| Lua value            | Error |
|----------------------|-------|
|db.err_dup            | Unique key constraint violation (duplicate key)
db.err_verify          | Verify failure - transaction tried to modify a record that was modified by another transaction
db.err_fkey            | Foreign key constraint violation
db.err_null_constraint | Null constraint violation
db.err_selectv         | Records touched by ```SELECTV``` modified by other transactions

Parameters:
none


### db:rollback

```
rc = db:rollback()
```

Description:

This method aborts a transaction that was started inside the same stored procedure with the
db:begin method.  It is an error to attempt to commit any transaction not begun in that manner.

Return Values:

|Name         |   Description
|-------------|--------------
|*rc*  |   non zero is failure

Parameters:
none

## Building result sets

### db:num_columns

```
db:num_columns(num)
```

Description:

This method specifies the number of columns to be output to the calling SQL client when either the "emit"
method is invoked, or a table is returned from the stored procedure.  It is an optional method - The
stored procedure will determine the number of columns in the output stream at runtime if not given this
information.  Best practices suggest using this method to rigidly enforce the output format of a stored procedure.

Return Values:

|Name  | Description         |Notes
|------|---------------------|-----------------
|*rc*  | non zero is failure | failed to set number of columns 

Parameters:

|  Name      |   Description
|------------|-------------------
|  *num*     | number of columns 


### db:column_type

```
db:column_type(type, index)
```

Description:

This method specifies the data type of a single column identified by numeric position to be output to the calling SQL client when either the "emit" method is invoked, or a table is returned from the stored procedure.  The first position is 1. It is an optional method - The stored procedure will determine the datatypes of each column in the output stream at runtime if not given this information. Best practices suggest using this method to rigidly enforce the output format of a stored procedure. The type is specified as a string, being one of real, int, text, blob or date.

Return Values:

|Name         |           Description  |Notes
|-------------|------------------------|-----
|*rc*  |  non zero is failure             | failed to set data type for column

Parameters:

|  Name         |    Description                   |Notes
|---------------|----------------------------------|----------
|  *type*  |  name of the data type                |real, int, text, blob, date
|  *index* |  which column is being defined        |Columns start at 1


### db:column_name

```
rc = db:column_name(name, index)
```

Description:

This method specifies the name of a single column identified by numeric position to be output to the calling SQL 
client when either the "emit" method is invoked, or a table is returned from the stored procedure.  The first position is 1.
It is an optional method - The
stored procedure will name the columns "column1" "column2" etc by default if this information is not given.
Best practices suggest using this method to rigidly enforce the output format of a stored procedure.  Defining the name of
a column explicitly offers a level of decoupling from the underlying database.

Return Values:

|Name         | Description         |        Notes                  
|-------------|----------------------|------------------------------
| *rc*   | non zero is failure | failed to set name for column 
 
Parameters:

|  Name        | Description                   |         Notes   
|--------------|--------------------------|----------------------
|  *name* | name of the column            |                  
|  *index*| which column is being defined |         1 based  


### db:emit

```
db:emit(dbtable | dbrow)
```

Description:

This method emits an item to the calling SQL client.  When invoked with a dbtable (the result of the "db:exec" method)
it will send each dbrow contained in the dbtable (the Lua table corresponding to each row returned by the query) to the client using the format
specified by optional calls to db:column_type, db:column_name, db:num_columns.
When invoked with a dbrow (the result of the dbtable:fetch method) the corresponding database "row" (Lua "table")
will be sent to the client.


### Lua return statement

Description:

The stored procedure can pass back data to the invoking SQL session by simply returning data from the Lua function. Following types can be 
returned:

* dbtable --  The entire dbtable will be sent back to the invoking SQL session, 1 row at a time.
* dbstmt --   All rows produced by sql statement will be sent back to the invoking SQL session, 1 row at a time.
* number --  The namespace for return codes is -200 to -299 and 0.
* string -- Calling application can access error message by calling `cdb2_errstr()`


## Limiting runtime

### db:setmaxinstructions

```
db:setmaxinstructions(num)
```

Description:

Stored procedure execution is halted after its quota of instructions is exhausted. By default the quota is set 
to 10000 Lua instruction. This can be changed to any number in the range [100, 1000000000]. Use 
`db:getinstructioncount` to determine how many instructions are used by your procedure and tune this number 
accordingly. This can be set to 20000 by calling `db:setmaxinstructions(20000)`

This can also be changed for all procedures in a database by adding the following to the lrl file

```
max_lua_instructions 20000
```

Parameters:

|Name|Description|Notes
|----|-----------|-----
|*num*|Set the quota of instructions to *num*|Must be in the range [100, 1000000000]


### db:getinstructioncount

```
num = db:getinstructioncount()
```

Description:

Returns the number of instructions executed by the stored procedure.

Return Values:

|Name|Description
|----|-----------
|*num*|See above


## Datetime functions

### db:now()

`db:now()` is a function to obtain current datetime from the system. Result is equivalent to running `select now()` in SQL.


### db:settimezone()

`db:settimezone('America/New_York')` has the effect of running `SET TIMEZONE America/New_York` in SQL. Example:

```
local function now_in_london()
    db:settimezone("Europe/London")
    return db:now()
end

local function main()
    print(db:now())
    print(now_in_london())
end
```

This stored procedure will output:

```
2013-11-13T112434.628 America/New_York
2013-11-13T162434.628 Europe/London
```


## Error information

### db:error()
```
errstr = db:error()
```

Obtain error string if any. Error may have been produced by running SQL or by other SP operations.

Return Values:

|Name|Description
|----|-----------
|*errstr*|string error message


## Reserved Keywords

In addition to all Lua reserved keywords, all comdb2 type names are reserved keywords and cannot be used as identifiers.

* `blob`
* `cstring`
* `datetime`
* `decimal`
* `declare`
* `int`
* `intervalds`
* `intervalym`
* `real`


## Convenience methods to pass objects/arrays to and from Stored Procedure

Database can parse JSON/CSV strings and produce corresponding Lua tables. This may be useful when passing JSON serialized objects or array of values as CSV.

### db:csv_to_table()

```
lua-array = db:csv_to_table(csv-string)
```

Parses input csv string and produces a Lua array (Lua table indexed by integers). The default separator is ','. Parsing is compatible with RFC 4180 (<https://tools.ietf.org/html/rfc4180>). Consider following pseudo-code:

```
string csv_str = a,b,"hello, world!","is ""quoted"" back there"
bind(@csv, csv_str)
run_statement("exec procedure csvdemo(@csv)")
```

`csvdemo.lua:`
```
local function main(c)
    local array = db:csv_to_table(c)
    print(#array) -- array length
    print(array[1]) -- customary in Lua to start arrays with index 1
    print(array[4])
end
```

This will output:
```
4
a
is "quoted" back there
```

Per the RFC, each record is located on a separate line. Input string above was a single record (row). Following 
is another example showing Lua array for input with multiple records:

```
string csv_str =
    1,2,3
    4,5,6
bind(@csv, csv_str)
run_statement("exec procedure csvdemo2(@csv)")
```

`csvdemo2.lua:`
```
local function main(c)
    local array = db:csv_to_table(c)
    print(#array)
    print(#array[1])
    print(array[1][1])
    print(array[2][3])
end
```

This will output:
```
2
3
1
6
```

In all examples above, the resulting Lua array contain strings.

### db:table_to_json()

```
json-string, rc = db:table_to_json(x, y)
    x: Lua table to convert to JSON string
    y: Optional Lua table to govern conversion
   rc: 0 if conversion was successful without truncation or hex-encoding of strings
       non-zero otherwise
```

Converts input Lua table to a UTF-8 encoded JSON string. Blobs convert to hex encoded strings. Datetime, interval and decimal types convert to strings. To distinguish between strings derived from different types, enable type annotation.
```
y.type_annotate = true | false (default)
```
`true`: Every element is replaced with an object which has `type` and `value` attributes. `type` can be can be one of: `object, array, bool, number, string, integer, double, decimal, cstring, hexstring, datetime, intervalym, intervalds, blob`

`false`: All non-numeric types are converted to respective string forms

Lua `string` and `cstring` may be not always be convertible into UTF-8 as they are simply null terminated byte arrays. To convert such input, use one of the following options:
```
y.invalid_utf8 = 'fail' | 'truncate' | 'hex' | nil (default)
```

`'fail'`: Return `nil` if any input cstring contains invalid UTF-8

`'truncate'`: Truncate cstring at first invalid UTF-8 byte

`'hex'`: Encode cstring as hex (including terminating null)

`nil`: Invalid UTF-8 in cstring will cause SP to fail

Examples:

```
local function main()
    local t = {}
    t.ans = 42
    t.pi = 3.14
    t.arr = {"first", "second"}
    t.obj = {firstName="John", lastName="Doe"}
    t.now = db:now()
    t.greeting = "hello, world"
    local json = db:table_to_json(t)
    print(json)
end
```

Output:
```
{"arr":["first","second"],"now":"2016-03-14T165305.197 America/New_York","obj":{"firstName":"John","lastName":"Doe"},"greeting":"hello, world","pi":3.14,"ans":42}
```

```
local function main()
    local x = {a = hello, world, b = x'deadbeef'}
    local json0 = db:table_to_json(x)
    local json1 = db:table_to_json(x, {type_annotate=true})
    print(json0)
    print(json1)
```

Output:
```
    {
      "a": "hello, world",
      "b": "deadbeef"
    }

    {
      "type": "object",
      "value": {
        "a": {
          "type": "string",
          "value": "hello, world"
        },
        "b": {
          "type": "blob",
          "value": "deadbeef"
        }
      }
    }
```

### db:json_to_table()

```
lua-table = db:csv_to_table(json-string, optional lua-table)
```

Parse input json string and produce corresponding Lua table. Supports objects, arrays, null, bool, integer, double and string. There is no representation for undefined, NaN or Infinity. Additionally, JSON does not have encoding for datetime, interval or decimal values. If required, these types can be encoded as strings or numbers and converted to desired type in the procedure. Pass in a lua-table with `type_annotate=true` to process JSON string which has type hints (as produced by `db:table_to_json`)
Example pseudo-code:

```
string json_str =
    {"employees":[
        {"firstName":"John", "lastName":"Doe"},
        {"firstName":"Anna", "lastName":"Smith"},
        {"firstName":"Peter", "lastName":"Jones"}
    ]}
bind(@json, json_str)
run_statement("exec procedure jsondemo(@json)")
```

`jsondemo.lua:`
```
local function main(j)
    local tbl = db:json_to_table(j)
    print(tbl.employees[3].lastName)
end
```

This will output:
```
Jones
```

## db:sp

```
lua-func = db:sp("spname")
lua-func = db:sp("spname", spversion)
```

It is possible to call one stored procedure from another. `db:sp()` returns a Lua function which is a reference to `main` in the target stored procedure. When spversion is omitted, the default version is used. The stored procedure is loaded in a new Lua chunk and has its own local variables and functions. Names will not clash with the calling stored procedure as shown in the following example:

`sp1.lua:`
```
local function func()
    print("sp1")
end
local function main()
    func()
end
```


`sp2.lua:`
```
local function func()
	print("sp2")
end
local function main()
    local sp1 = db:sp("sp1")
    sp1()
    func()
end
```


`exec procedure sp2()` will output:

```
sp1
sp2
```


## SQL Lua Functions

Custom SQL functions can be implemented as stored procedures.

Couple of limitations when using stored procedures as lua functions:

1. Lua functions cannot start threads.
2. Lua functions cannot explicitly control transactions (by calling `db:begin`, `db:commit` and `db:rollback`).

Two categories of functions can be defined:

### Scalar function

The `CREATE LUA SCALAR FUNCTION func-name` statement exposes the named stored procedure as a scalar function 
in SQL. The stored procedure should already have been added to the database before running this statement.

These functions return a single value on every call, for example `upper`, `round`. Any number of arguments can 
be passed to the stored procedure and this depends on the business logic it implements.

The stored procedure name must match the SQL function name. Additionally, it must implement a function 
which matches the SQL function name. The stored procedure is not required to have a `main`. This is convenient 
during development; stored procedure can be executed as usual and `main` can be used to simulate calls SQL 
engine would have performed.

```
local function greet_name(s, f, l)
    if s == 'm' then
        return "Mr. " .. f .. " " .. l
    elseif s == 'f' then
        return "Ms. " .. f ..  " " ..l
    else
        return f ..  " " ..l
    end
end
```

Example runs:

```
cdb2sql> insert into persons values("m", "John", "Doe"),("f", "Jane", "Roe")
(rows inserted=2)
cdb2sql> select greet_name(sex, firstName, lastName) as hi from persons
[select greet_name(sex, firstName, lastName) as hi from persons] failed with rc -3 no such function: greet_name
cdb2sql> create lua scalar function greet_name
cdb2sql> select greet_name(sex, firstName, lastName) as hi from persons`
(hi="Mr. John Doe")
(hi="Ms. Jane Roe")
```

The `DROP LUA SCALAR FUNCTION func-name` statement disassociates the stored procedure from SQL function. 
The stored procedure still exists, but it's not callable as a function from SQL.

### Aggregate function

The `CREATE LUA AGGREGATE FUNCTION func-name` statement exposes the named stored procedure as an aggregate 
function in SQL. The stored procedure should already have been added to the database before running this statement.

These functions return a single value after performing computation on a set of values, for example `avg`, `max`. 
Any number of arguments can be passed to the stored procedure and this depends on the business logic it implements.

The stored procedure name must match the SQL function name. Additionally, it must implement two functions 
named `step` and `final`. SQL engine will call `step` once per matching row and `final` at the end of processing 
all rows. `final` returns a single value which is the result of aggregate logic. Any return value from `step` is 
ignored. Stored procedure doesn't need to have `main`. Stored procedure will need to maintain state during 
calls to `step` and `final`. The sever will ensure that same Lua VM is provided during these calls so state 
can be maintained. This is illustrated in the example below where we provide a simplistic implementation for `median`.

```
// Table definition
schema {
    int i
}
```

```
local nums = {} -- to save state during various step, final calls

local function step(num)
    table.insert(nums, num)
end

local function final()
    table.sort(nums)
    local total = #nums
    if total % 2 ~= 0 then
        total = total + 1
        return nums[total/2]
    end
    local a = total / 2
    local b = a + 1
    return (nums[a] + nums[b]) / 2
end
```

Example runs:
```
cdb2sql> select median(i) from t;
[select median(i) from t] failed with rc -3 no such function: median
cdb2sql> create lua aggregate function median
[create lua aggregate function median] rc 0
cdb2sql> insert into t values(1)
(rows inserted=1)
cdb2sql> select median(i) from t;
(median(i)=1)
cdb2sql> insert into t values(4),(3),(2);
(rows inserted=3)
cdb2sql> select median(i) from t;
(median(i)=2.5)
cdb2sql> insert into t values(5);
(rows inserted=1)
cdb2sql> select median(i) from t;
(median(i)=3)
```

The `DROP LUA AGGREGATE FUNCTION func-name` statement disassociates the stored procedure from SQL function. The stored procedure still exists, but it's not callable as a function from SQL.
