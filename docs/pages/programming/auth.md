---
title: Comdb2 authentication
sidebar: mydoc_sidebar
permalink: auth.html
---

## Authenticating comdb2 session
Comdb2 session can be authenticated by setting username and password using [set user](sql.html#set-user) and [set password](sql.html#set-password) statements, just after opening the connection.

```sql
set user 'foo_user'
set password 'foo_password'
``` 

## Adding/deleting users to database
Users can be added and deleted by [put password](sql.html#put) statement. Once authentication is enabled only users with OP credentials can add or delete users.

To add new user or change password of existing user:
```put password '<password>' for <user>```

```sql
put password 'foo_password' for 'foo_user'
put password 'op_password' for 'op_user'
``` 

To delete user:
```put password off for <user>```

```sql
put password off for 'foo_user'
``` 

Users can be granted OP credentials by [grant OP](sql.html#grant-and-revoke) statement.  Only OP users can run this statement after authentication is enabled.
```sql
grant OP to 'op_user'
``` 

Existing users can be seen by query on ```comdb2_users``` table, the resulting data will give usernames along with their OP credentials.

```sql
select * from comdb2_users
``` 

```
select * from comdb2_users
(username='default', isOP='N')
(username='foo_user', isOP='N')
(username='op_user', isOP='Y')
```
This info is part of database metadata, therefore copying the database will preserve it.

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

Turning on authentication will add a new user with username 'default', if it doesn't already exist. This user will be used for every unauthenticated session.
The newly created 'default' user doesn't have any permission, but can be granted any permission by an OP user.

## Granting/Revoking table permissions to users

Comdb2 allows following permissions on table

|Permission|Description|
|---|---|
|READ|Can read records of table|
|WRITE|Can read/write records to table|
|DDL|Can read/write records and alter schema of table|

OP users can grant table permissions using [grant](sql.html#grant-and-revoke) statement

```sql
grant read on t1 to 'foo_user'
``` 

permissions can be revoked using [revoke](sql.html#grant-and-revoke) statement

```sql
revoke read on t1 to 'foo_user'
``` 

Existing permissions can be seen by query on ```comdb2_tablepermissions```
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
