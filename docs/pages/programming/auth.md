---
title: Comdb2 authentication
sidebar: mydoc_sidebar
permalink: auth.html
---

## Authenticating comdb2 session

### Password-based Authentication

A comdb2 session can be authenticated by setting username and password using [set user](sql.html#set-user) and [set password](sql.html#set-password), just after opening the connection.

```sql
set user 'foo_user'
set password 'foo_password'
``` 

### Certificate-based Authentication

A comdb2 session can also be authenticated by setting the client certificate/key using [set ssl_cert](sql.html#set-ssl_cert) and [set ssl_key](sql.html#set-ssl_key), just after opening the connection.

```sql
set ssl_cert /path/to/certificate
set ssl_key /path/to/key
``` 

## Adding/deleting users to database
Users can be added and deleted through the [put password](sql.html#put) statement. Once authentication is enabled only users with OP credentials can add or delete users.

To add a new user or update the password of an existing user:
```put password '<password>' for <user>```

```sql
put password 'foo_password' for 'foo_user'
put password 'op_password' for 'op_user'
``` 

To delete a user:
```put password off for <user>```

```sql
put password off for 'foo_user'
``` 

Users can be granted OP credentials using [grant OP](sql.html#grant-and-revoke).  Only OP users can run this statement after authentication is enabled.
```sql
grant OP to 'op_user'
``` 

Existing users can be seen by performing a query on the ```comdb2_users``` table, the result set will give user names along with their OP status.

```sql
select * from comdb2_users
``` 

```
select * from comdb2_users
(username='foo_user', isOP='N')
(username='op_user', isOP='Y')
```
This information is part of the database's metadata, therefore copying the database will preserve it.

## Turning on Authentication
The following SQL statement will turn on authentication

```sql
put authentication on
``` 

It can be turned off by running

```sql
put authentication off
``` 
Only users which have OP credentials can turn on/off authentication.

As a convenience, if ```create_default_user``` tunable is turned on, turning on authentication would automatically create a new user with name 'default' and empty password if it doesn't already exist. This newly created user doesn't have any privileges to any objects, but can be granted any privilege by an OP user. It will be used for every unauthenticated session.

## Granting/Revoking table privileges to users

Comdb2 defines the following privileges on a table:

|Privilege|Description|
|---|---|
|READ|Can read records of table|
|WRITE|Can read/write records to table|
|DDL|Can read/write records and alter schema of table|

OP users can grant table privileges using [grant](sql.html#grant-and-revoke)

```sql
grant read on t1 to 'foo_user'
``` 

privileges can be revoked using the [revoke](sql.html#grant-and-revoke) statement

```sql
revoke read on t1 to 'foo_user'
``` 

Existing privileges can be seen by running a query on ```comdb2_tablepermissions```
```sql
select * from comdb2_tablepermissions
``` 

```
testdb> set user foo_user
[set user foo_user] rc 0
testdb> set password foo_password
[set password foo_password] rc 0
testdb> select * from comdb2_tablepermissions /* Can only see tables on which user has read access */
(tablename='t1', username='default', READ='N', WRITE='N', DDL='N')
(tablename='t1', username='foo_user', READ='Y', WRITE='N', DDL='N')
(tablename='t1', username='op_user', READ='Y', WRITE='Y', DDL='Y')
[select * from comdb2_tablepermissions] rc 0
testdb> set user op_user
[set user op_user] rc 0
testdb> set password op_password
[set password op_password] rc 0
testdb> select * from comdb2_tablepermissions 
(tablename='sqlite_stat1', username='default', READ='N', WRITE='N', DDL='N')
(tablename='sqlite_stat1', username='foo_user', READ='N', WRITE='N', DDL='N')
(tablename='sqlite_stat1', username='op_user', READ='Y', WRITE='Y', DDL='Y')
(tablename='sqlite_stat4', username='default', READ='N', WRITE='N', DDL='N')
(tablename='sqlite_stat4', username='foo_user', READ='N', WRITE='N', DDL='N')
(tablename='sqlite_stat4', username='op_user', READ='Y', WRITE='Y', DDL='Y')
(tablename='t1', username='default', READ='N', WRITE='N', DDL='N')
(tablename='t1', username='foo_user', READ='Y', WRITE='N', DDL='N')
(tablename='t1', username='op_user', READ='Y', WRITE='Y', DDL='Y')
(tablename='t2', username='default', READ='N', WRITE='N', DDL='N')
(tablename='t2', username='foo_user', READ='N', WRITE='N', DDL='N')
(tablename='t2', username='op_user', READ='Y', WRITE='Y', DDL='Y')
[select * from comdb2_tablepermissions] rc 0
```

## User Schemas
Comdb2 supports tables in user's namespace. This allows multiple users to have tables with same name.

To enable it, this lrl option needs to be enabled
```
allow_user_schema
```

The following example will create multiple users and separate table (with same name) for each user.
Querying comdb2_tables from op user account will show all the tables.

```sql
put password 'user' for 'user'
put password 'user1' for 'user1'
put password 'user2' for 'user2'
grant op to user

set user user
set password user
put authentication on /* Only op can turn on the authentication. */

select * from comdb2_users
(username='user', isOP='Y')
(username='user1', isOP='N')
(username='user2', isOP='N')
[select * from comdb2_users] rc 0

set user user1
set password user1
create table test { schema {int t} keys { "T1" = t}}$$
insert into test values(1)
(rows inserted=1)

set user user2
set password user2
create table test { schema {int t} keys { "T1" = t}}$$
insert into test values(2)
(rows inserted=1)

set user user1
set password user1
select * from test
(t=1)

set user user2
set password user2
select * from test
(t=2)

set user user
set password user
select * from comdb2_tables
(tablename='sqlite_stat1')
(tablename='sqlite_stat2')
(tablename='sqlite_stat4')
(tablename='test@user1')
(tablename='test@user2')
```
