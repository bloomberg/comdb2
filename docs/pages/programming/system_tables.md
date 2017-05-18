---
title: Info Tables
keywords: code
sidebar: mydoc_sidebar
permalink: system_tables.html
---

## comdb2_tables

This is a table of all the existing tables in the database.

    comdb2_tables(tablename)

* `tablename` - Name of the table.

## comdb2_columns

Describes all the columns for all of the tables in the database.

    comdb2_columns(tablename, columnnname, type, size, sqltype,
    varinlinesize, defaultvalue, dbload, isnullable)

* `tablename` - Name of the table.
* `columnname` - Name of the column.
* `size` - The storage size of the column.
* `sqltype` - The type as recognized by sql.
* `varinlinesize` - The size of the data stored in the column when inlined.
* `defaultvalue` - The default value for this column.
* `dbload` - The default value for this column loaded by the server.
* `isnullable` - `Y` if this column can hold nulls.

## comdb2_keys

Describes all of the keys in the database.

    comdb2_keys(tablename, keyname, keynumber, isunique, isdatacopy,
    isrecnum, condition)

* `tablename` - Name of the table.
* `keyname` - Name of the key.
* `isunique` - `Y` if this column is unique.
* `isdatacopy` - `Y` if the data is inlined with this key.
* `isrecnum` - `Y` if this key has recnums.
* `condition` - Where condition for this index.

## comdb2_keycomponents

Describe all the components of the keys.

    comdb2_keycomponents(tablename, keyname, columnnumber, columnname,
    isdescending)

* `tablename` - Name of the table.
* `keyname` - Name of the key.
* `columnnumber` - Number of a the column `keyname` comprises.
* `columnname` - Name of a column that `keyname` comprises.
* `isdescending` - `Y` if this key is descending.

## comdb2_constraints

Shows all foreign key constraints on tables in the database.

    comdb2_constraints(tablename, keyname, foreigntablename,
    foreignkeyname, iscascadingdelete, iscascadingupdate)

* `tablename` - Name of the table.
* `keyname` - Name of the key.
* `foreigntablename` - Name of the foreign table.
* `foreignkeyname` - Name of the foreign key.
* `iscascadingdelete` - `Y` if this is a cascading delete.
* `iscascadingupdate` - `Y` if this is a cascading update.

## comdb2_tablesizes

Shows the sizes on disk of the tables.

    comdb2_tablesizes(tablename, bytes)

* `tablename` - Name of the table.
* `bytes` - Size of the table in bytes.

## comdb2_users

Table of users for the database that do or do not have operator access.

    comdb2_users(username, isOP)

* `username` - Name of the user.
* `isOP` - `Y` if `username` has operator access.

## comdb2_tablepermissions

Table of permissions for tables in the database.

    comdb2_tablepermissions(tablename, username, READ, WRITE, OP)

* `tablename` - Name of the table.
* `username` - Name of the user.
* `READ` - `Y` if `username` has read access to `tablename`.
* `WRITE` - `Y` if `username` has write access to `tablename`.
* `OP` - `Y` if `username` can modify `tablename` schema.

## comdb2_triggers

Table of triggers in the database.

    comdb2_triggers(name, type, tbl_name, event, col)

* `name` - Name of the trigger.
* `type` - Type of the trigger.
* `tbl_name` - Name of the table.
* `event` - Event to trigger on.
* `col` - Column to trigger on.
