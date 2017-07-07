---
title: Bi-Temporal Tables
keywords: code
sidebar: mydoc_sidebar
permalink: bitemporal.html
---

One of the main features introduced in SQL:2011 was "SQL Support for Time-Related Information".

In *ISO/IEC TR 19075-2:2015*, it defines three different types of time-related tables:

* System-Versioned Tables (with system versioning period)
* Application/Business-Versioned Table (with application/business time period)
* Bi-Temporal Table (with both types of periods)

Comdb2 has native support for all three types of time-related tables.

Reference works:

* SQL:2011 Standard [ISO/IEC TR 19075-2:2015](http://standards.iso.org/ittf/PubliclyAvailableStandards/c060394_ISO_IEC_TR_19075-2_2015.zip) (Free download)
* [Temporal features in SQL:2011](http://cs.ulb.ac.be/public/_media/teaching/infoh415/tempfeaturessql2011.pdf)

## System-Versioned Temporal Tables

System-versioned tables are intended for meeting the requirements of applications that must maintain an accurate history of data changes either for business reasons, legal reasons, or both. In order to have an accurate history of data changes, any update or delete of rows would automatically preserve old state of the rows. Another important requirement to system-versioned tables is that the system, rather than the user, maintains the start and end times of the system periods of the rows, and that users be unable to modify the content of historical rows or the periods associated with any of the rows.

### Creating a table with system time

```
create table t1 {
	schema {
		int id
		int coverage
		datetimeus sys_start null=yes
		datetimeus sys_end null=yes
	}
	keys {
		"ID" = id
	}
	periods {
		SYSTEM(sys_start, sys_end)
	}
}
```

![periods](images/period-section.gif)

Remarks:

* `SYSTEM` time period must be defined under the `periods` section in the csc2 file like above. A system-versioned temporal table must have exactly one period named `SYSTEM` defined in the `periods` section.
* `SYSTEM` time period consumes two `datetimeus` columns with the first one being the system start time and the second one being the system end time.
* In Comdb2, the two system period columns MUST be nullable. (This differs from the period defnition in *[ISO/IEC TR 19075-2:2015*.) `NULL` in the system start time column means "less than all `datetimeus` values" and `NULL` in the system end time column means "greater than all `datetimeus` values".
* Comdb2 will automatically generate period values (system start time and end time) on INSERT, UPDATE, and DELETE using the transaction start time. Users are not allowed to assign or change values of these two columns.
* Comdb2 uses an inclusive-exclusive (close-open) approach for the system time period `(sys_start, sys_end)`, meaning that a row is valid from (and including) `sys_start` to (but not including) `sys_end`.
* An history table named `[tablename]_history` is automatically created in the same schema as current table. `[tablename]` is the table name of the current table in the `CREATE TABLE`/`ALTER TABLE` statement.
* The history table created is a READ ONLY table. Users are not allowed to INSERT/UPDATE/DELETE any rows in history table.
* On UPDATE and DELETE, Comdb2 automatically inserts rows into history table for every current system row that is updated or deleted, marking `sys_end` to be the transaction start time of the update or delete.
* Only instance schema change is allowed on system-versioned temporal tables.

### Inserting data into a table with system time
Traditional [`INSERT`](sql.html#insert) statements can be used to insert data into system-versioned tables. Imagine that we started two transactions at "2017-05-10T100000" and "2017-05-10T110000" to insert the following two rows respectively.
```
insert into t1(id, coverage) values (1, 100)
insert into t1(id, coverage) values (2, 200)
```
Now, t1 looks like:

```
(id=1, coverage=100, sys_start="2017-05-10T100000.000000 America/New_York", sys_end=NULL)
(id=2, coverage=200, sys_start="2017-05-10T110000.000000 America/New_York", sys_end=NULL)
[select * from t1] rc 0
```

When inserting each row into the current table, Comdb2 generates `datetimeus` values for system time columns with system start time being the transaction start time and system end time being `NULL`. Even if these two columns were referenced in the `INSERT` statement, the values of these two columns would still be overwritten as transaction start time and `NULL`.

### Updating data in a table with system time
Traditional [`UPDATE`](sql.html#update) statements without the `FOR PORTION OF BUSINESS_TIME` clause can be used to update data in system-versioned tables. Imagine that we started a transaction at "2017-05-10T120000" to update the following row.

```
(rows updated=1)
[update t1 set coverage = 1000 where id = 1] rc 0
```

Now, `t1` looks like (with `coverage` and `sys_start` columns updated for `id = 1`):

```
(id=1, coverage=1000, sys_start="2017-05-10T120000.000000 America/New_York", sys_end=NULL)
(id=2, coverage=200, sys_start="2017-05-10T110000.000000 America/New_York", sys_end=NULL)
[select * from t1] rc 0
```

`t1_history` looks like (with old copy of the updated row inserted to history table):

```
(id=1, coverage=100, sys_start="2017-05-10T100000.000000 America/New_York", sys_end="2017-05-10T120000.000000 America/New_York")
[select * from t1_history] rc 0
```

`UPDATE` on system-versioned tables only operate on current system rows. During the execution of the above `UPDATE` statement, Comdb2 updates the value of the row in the current table. In addition, within the same transaction, it moves a copy of the old row to the corresponding history table. Comdb2 also generates values for both `sys_start` and `sys_end` columns for both (current and history) tables. In particular, `sys_start` in the current table and `sys_end` in the history table are set to the start time of the update transaction.

### Deleting data in a table with system time
Traditional [`DELETE`](sql.html#delete) statements without the `FOR PORTION OF BUSINESS_TIME` clause can be used to delete data from system-versioned tables. Imagine that we started a transaction at "2017-05-10T130000" to delete the following row (where id = 2).

```
(rows deleted=1)
[delete from t1 where id = 2] rc 0
```

Now, `t1` looks like (with one row with `id = 2` deleted):

```
(id=1, coverage=1000, sys_start="2017-05-10T120000.000000 America/New_York", sys_end=NULL)
[select * from t1] rc 0
```

And `t1_history` looks like (with old copy of the deleted row inserted to history table):

```
(id=1, coverage=100, sys_start="2017-05-10T100000.000000 America/New_York", sys_end="2017-05-10T120000.000000 America/New_York")
(id=2, coverage=200, sys_start="2017-05-10T110000.000000 America/New_York", sys_end="2017-05-10T130000.000000 America/New_York")
[select * from t1_history] rc 0
```

`Delete` on system-versioned tables only operate on current system rows. When you delete a row from current table, Comdb2 automatically removes the row from the current table but maintains an old version of the row in the history table. Comdb2 sets the end time of the (deleted) row in the history table to the transaction start time of the `DELETE` statement.

### Querying a table with system time
Traditional [`SELECT`](sql.html#select-statement) statements are still allowed in system-versioned tables. In addition, one of the following supported system time period specifications can be used in the [`temporal-clause`](sql.html#temporal-clause) of your `SELECT` query:

|System Time Queries|Qualifying Rows|Description|
|---|---|---|
|FOR SYSTEM_TIME AS OF <datetime>|sys_start <= datetime && sys_end > datetime|return data as of a certain point in time|
|FOR SYSTEM_TIME FROM <datetime_start> TO <datetime_end>|sys_start < datetime_end && sys_end > datetime_start|return data for all versions that were valid within the specified (close-open) range period starting from `<datetime_start>` up to (but not including) `<datetime_end>`|
|FOR SYSTEM_TIME BETWEEN <datetime_start> AND <datetime_end>|sys_start <= datetime_end && sys_end > datetime_start|return data for all versions that were valid within the specified (close-close) range period starting from `<datetime_start>` up to (and including) `<datetime_end>`|
|FOR SYSTEM_TIME ALL|All rows|return union of rows that belong to the current and history table|

If we want data as of "2017-05-10T113000", we could do:

```
(id=1, coverage=100, sys_start="2017-05-10T100000.000000 America/New_York", sys_end="2017-05-10T120000.000000 America/New_York")
(id=2, coverage=200, sys_start="2017-05-10T110000.000000 America/New_York", sys_end="2017-05-10T130000.000000 America/New_York")
[select * from t1 for system_time as of '2017-05-10T113000' order by id] rc 0
```

Another example with `FOR SYSTEM_TIME ALL`:

```
(id=1, coverage=100, sys_start="2017-05-10T100000.000000 America/New_York", sys_end="2017-05-10T120000.000000 America/New_York")
(id=1, coverage=1000, sys_start="2017-05-10T120000.000000 America/New_York", sys_end=NULL)
(id=2, coverage=200, sys_start="2017-05-10T110000.000000 America/New_York", sys_end="2017-05-10T130000.000000 America/New_York")
[select * from t1 for system_time all order by id, sys_start] rc 0
```

Remarks:

* `<datetime>` specified in the `FOR SYSTEM_TIME` clauses can be any [expressions](sql.html#expr), even subqueries, whose result can be convertible to a single `datetimeus` datatype value.
* `<datetime_start>` must be strictly less than `<datetime_end>` in `FOR SYSTEM_TIME FROM ... TO ...` and `FOR SYSTEM_TIME BETWEEN ... AND ...` clauses.

## Business-Versioned Temporal Tables

As defined in SQL:2011 Standard, Business-versioned tables are intended for meeting the requirements of applications that are interested in capturing time periods during which the data is believed to be valid in the real world. A typical example of such applications is an insurance application, where it is necessary to keep track of the specific policy details of a given customer that are in effect at any given point in time.

### Creating a table with business time

```
create table t1 {
	schema {
		int id
		int coverage
		datetimeus bus_start null=yes
		datetimeus bus_end null=yes
	}
	keys {
		"PK" = id + bus_start + bus_end
	}
	periods {
		BUSINESS(bus_start, bus_end)
	}
	constraints {
		no_overlap "PK" -> bus_start : bus_end
	}
}
```

![periods](images/period-section.gif)

Remarks:

* `BUSINESS` time period must be defined under the `periods` section in the csc2 file like above. A business-versioned temporal table must have exactly one period named `BUSINESS` defined in the `periods` section.
* `BUSINESS` time period consumes two `datetimeus` columns with the first one being the business start time and the second one being the business end time.
* The two period columns must be nullable. (This differs from the period defnition in *ISO/IEC TR 19075-2:2015*.) `NULL` in the business start time column means "less than all `datetimeus` values" and `NULL` in the business end time column means "greater than all `datetimeus` values".
* Comdb2 uses an inclusive-exclusive (close-open) approach for the busiess time period `(bus_start, bus_end)`, meaning that a row is effective from (and including) `bus_start` to (but not including) `bus_end`.
* Comdb2 generates implicit constraint to enforce that `bus_start` is strictly less than `bus_end` for every row in the business-versioned table.
* Optional extended [`no_overlap` constraints](table_schema.html#no-overlapping-constraints) can be defined for keys that contain both the business period columns `bus_start` and `bus_end`, meaning that there cannot be more than one versions/rows with identical values on other key fields being effective at the same time (i.e. having overlapping business period).

### Inserting data into a table with business time
Following the SQL:2011 standard, rows can be inserted into tables containing an business-time period in exactly the same way as inserting rows into any tables. The only difference is that there is an automatically-generated constraint for such tables that ensures that the value of the end column of the business-time period is greater than the value of the start column of the business-time period for every row being inserted.

```
insert into t1(id, coverage, bus_start, bus_end) values (1, 100, '2015-01-01', '2016-01-01')
insert into t1(id, coverage, bus_start, bus_end) values (1, 110, '2016-01-01', '2017-01-01')
insert into t1(id, coverage, bus_start, bus_end) values (2, 200, NULL, '2017-01-01')
insert into t1(id, coverage, bus_start, bus_end) values (3, 300, '2017-01-01', NULL)
```

Now, `t1` looks like:

```
(id=1, coverage=100, bus_start="2015-01-01T000000.000000 America/New_York", bus_end="2016-01-01T000000.000000 America/New_York")
(id=1, coverage=110, bus_start="2016-01-01T000000.000000 America/New_York", bus_end="2017-01-01T000000.000000 America/New_York")
(id=2, coverage=200, bus_start=NULL, bus_end="2017-01-01T000000.000000 America/New_York")
(id=3, coverage=300, bus_start="2017-01-01T000000.000000 America/New_York", bus_end=NULL)
[select * from t1 order by id] rc 0
```

Inserting overlapping rows would fail:

```
[insert into t1(id, coverage, bus_start, bus_end) values (1, 111, '2015-06-01', '2016-06-01')] failed with rc 3 OP #3 BLOCK2_SEQV2(824): verify key constraint cannot resolve constraint no_overlap(bus_start : bus_end)
[insert into t1(id, coverage, bus_start, bus_end) values (2, 222, '2016-01-01', '2018-01-01')] failed with rc 3 OP #3 BLOCK2_SEQV2(824): verify key constraint cannot resolve constraint no_overlap(bus_start : bus_end)
[insert into t1(id, coverage, bus_start, bus_end) values (3, 333, '2018-01-01', '2019-01-01')] failed with rc 3 OP #3 BLOCK2_SEQV2(824): verify key constraint cannot resolve constraint no_overlap(bus_start : bus_end)
```

### Updating data in a table with business time

[`UPDATE`](sql.html#update) statements are used to update rows in tables with business time. Using traditional `UPDATE` statement without the `FOR PORTION OF BUSINESS_TIME` clause on business-versioned tables would behave exactly like the update operations on any regular tables. In addition, with business-versioned tables, a `FOR PORTION OF BUSINESS_TIME FROM ... TO ...` clause can be used to restrict the update to a specific business time period. This specified updates apply only to those rows whose business periods overlap or are contained in the given period. If your update impacts data in a row that isn't fully contained within the time period specified, Comdb2 will update the row range specified by the period clause and insert additional rows to record the old values for the period not included in the update operation.

In particular, Comdb2 follows SQL:2011's Standard defined in *ISO/IEC TR 19075-2:2015* for this type of `UPDATE` statement. Quoting the standard behavior as follow:

* Let FT be the first value and ST be the second value specified in the `FOR PORTION OF` clause.
* For each row R in the table that qualifies for update and whose business-time period overlaps with the period formed by FT and ST, let BPS be its business-time period start value, and let BPE be its business time period end value.
* If BPS < FT and BPE > FT, then a copy of R with its business-time period end value set to FT is inserted.
* If BPS < ST and BPE > ST, then a copy of R with its business-time period start value set to ST is inserted.
* R is updated with its business-period start value set to the maximum of BPS and FT and the business-time end value set to the minimum of BPE and ST.

```
(rows updated=2)
[update t1 for portion of business_time from '2015-06-01' to '2016-06-01' set coverage = 111 where id = 1] rc 0
```

Now, rows with `id = 1` in `t1` are:

```
(id=1, coverage=100, bus_start="2015-01-01T000000.000000 America/New_York", bus_end="2015-06-01T000000.000000 America/New_York")
(id=1, coverage=111, bus_start="2015-06-01T000000.000000 America/New_York", bus_end="2016-01-01T000000.000000 America/New_York")
(id=1, coverage=111, bus_start="2016-01-01T000000.000000 America/New_York", bus_end="2016-06-01T000000.000000 America/New_York")
(id=1, coverage=110, bus_start="2016-06-01T000000.000000 America/New_York", bus_end="2017-01-01T000000.000000 America/New_York")
[select * from t1 where id = 1] rc 0
```

### Deleting data in a table with business time
[`DELETE`](sql.html#delete) statements are used to delete rows in tables with business time. Using traditional `DELETE` statement without the `FOR PORTION OF BUSINESS_TIME` clause on business-versioned tables would behave exactly like the update operations on any regular tables. In addition to regular `DELETE` statement, with business-versioned tables, a `FOR PORTION OF BUSINESS_TIME FROM ... TO ...` clause can be used to restrict the delete to a specific business time period. This specified deletes apply only to those rows whose business periods overlap or are contained in the given period. If rows being deleted have data that isn't fully contained within the time period specified, Comdb2 will preserve the portion outside the specified range.

In particular, Comdb2 follows SQL:2011's Standard defined in *ISO/IEC TR 19075-2:2015* for this type of `DELETE` statement. Quoting the standard behavior as follow:

* Let FT be the first value and ST be the second value specified in the FOR PORTION OF clause.
* For each row R in the table that qualifies for deletion and whose business-time period overlaps with the period formed by FT and ST, let BPS be its business-time period start value, and let BPE be its business-time period end value.
  * If BPS < FT and BPE > FT, then a copy of R with its business-time period end value set to FT is inserted.
  * If BPS < ST and BPE > ST, then a copy of R with its business-time period start value set to ST is inserted.
  * R is deleted.

```
(rows deleted=1)
[delete from t1 for portion of business_time from '2015-01-01' to '2016-01-01' where id = 2] rc 0
```

Now, rows with `id = 2` in `t1` are:

```
(id=2, coverage=200, bus_start=NULL, bus_end="2015-01-01T000000.000000 America/New_York")
(id=2, coverage=200, bus_start="2016-01-01T000000.000000 America/New_York", bus_end="2017-01-01T000000.000000 America/New_York")
[select * from t1 where id = 2] rc 0
```

Remarks:

* In clause `FOR PORTION OF BUSINESS_TIME FROM <datetime_start> TO <datetime_end>`, `<datetime_start>` and `<datetime_end>` can be any [expressions](sql.html#expr), even subqueries, whose result can be convertible to a single `datetimeus` datatype value.

### Querying a table with business time
Traditional [`SELECT`](sql.html#select-statement) statements are still allowed in business-versioned tables. In addition, one of the following supported business time period specifications can be used in the [`temporal-clause`](sql.html#temporal-clause) of your `SELECT` query:

|Business Time Queries|Qualifying Rows|Description|
|---|---|---|
|FOR BUSINESS_TIME AS OF <datetime>|bus_start <= datetime && bus_end > datetime|return data effective at `<datetime>`|
|FOR BUSINESS_TIME FROM <datetime_start> TO <datetime_end>|bus_start < datetime_end && bus_end > datetime_start|return data for all versions that were effective within the specified (close-open) range period starting from `<datetime_start>` up to (but not including) `<datetime_end>`|
|FOR BUSINESS_TIME BETWEEN <datetime_start> AND <datetime_end>|bus_start <= datetime_end && bus_end > datetime_start|return data for all versions that were effective within the specified (close-close) range period starting from `<datetime_start>` up to (and including) `<datetime_end>`|
|FOR BUSINESS_TIME ALL|All rows|return all rows satisfying the `WHERE` clause of the `SELECT` statement|

After the modifications above, now `t1` looks like:

```
(id=1, coverage=100, bus_start="2015-01-01T000000.000000 America/New_York", bus_end="2015-06-01T000000.000000 America/New_York")
(id=1, coverage=111, bus_start="2015-06-01T000000.000000 America/New_York", bus_end="2016-01-01T000000.000000 America/New_York")
(id=1, coverage=111, bus_start="2016-01-01T000000.000000 America/New_York", bus_end="2016-06-01T000000.000000 America/New_York")
(id=1, coverage=110, bus_start="2016-06-01T000000.000000 America/New_York", bus_end="2017-01-01T000000.000000 America/New_York")
(id=2, coverage=200, bus_start=NULL, bus_end="2015-01-01T000000.000000 America/New_York")
(id=2, coverage=200, bus_start="2016-01-01T000000.000000 America/New_York", bus_end="2017-01-01T000000.000000 America/New_York")
(id=3, coverage=300, bus_start="2017-01-01T000000.000000 America/New_York", bus_end=NULL)
[select * from t1 order by id] rc 0
```

If we want data effective on "2016-06-01", we could do:

```
(id=1, coverage=111, bus_start="2016-01-01T000000.000000 America/New_York", bus_end="2016-06-01T000000.000000 America/New_York")
(id=2, coverage=200, bus_start="2016-01-01T000000.000000 America/New_York", bus_end="2017-01-01T000000.000000 America/New_York")
[select * from t1 for business_time as of '2016-01-01' order by id] rc 0
```

Remarks:

* `<datetime>` specified in the `FOR BUSINESS_TIME` clauses can be any [expressions](sql.html#expr), even subqueries, whose result can be convertible to a single `datetimeus` datatype value.
* `<datetime_start>` must be strictly less than `<datetime_end>` in `FOR BUSINESS_TIME FROM ... TO ...` and `FOR BUSINESS_TIME BETWEEN ... AND ...` clauses.

## Bi-Temporal Tables
A table may be both a system-versioned table and an business-time period table. Bitemporal tables combine the capabilities of both system-versioned and business-time period tables.

### Managing data in a bi-temporal table

Creating a bi-temporal table

```
create table t1 {
    schema {
        int id
        cstring company[20]
        int coverage
        datetimeus bus_start null=yes
        datetimeus bus_end null=yes
        datetimeus sys_start null=yes
        datetimeus sys_end null=yes
    }
    keys {
        "PK" = id + bus_start + bus_end
    }
    periods {
        BUSINESS(bus_start, bus_end)
        SYSTEM(sys_start, sys_end)
    }
    constraints {
        no_overlap "PK" -> bus_start : bus_end
    }
}
```

![periods](images/period-section.gif)

Imagine that on "2016-06-01", we inserted two rows into the table:

```
(rows inserted=2)
[insert into t1(id, company, coverage, bus_start, bus_end) values(1, 'Allstate', 20000, '2016-01-01', '2017-01-01'), (1, 'State Farm', 30000, '2017-01-01', '2018-01-01')] rc 0
```

If our business applications were interested in the effective value on "2017-05-01", we would see `coverage=30000`:
```
(id=1, company='State Farm', coverage=30000, bus_start="2017-01-01T000000.000000 America/New_York", bus_end="2018-01-01T000000.000000 America/New_York", sys_start="2016-01-01T000000.000000 America/New_York", sys_end=NULL)
[select * from t1 for business_time as of '2017-05-01' where id = 1] rc 0
```

Imagine that on "2016-08-01", we did the following update:

```
(rows updated=2)
[update t1 for portion of business_time from '2016-06-01' to '2017-06-01' set coverage = 10000 where id = 1] rc 0
```

After "2016-08-01", `t1` and `t1_hostory` looks like:

```
(id=1, company='Allstate', coverage=20000, bus_start="2016-01-01T000000.000000 America/New_York", bus_end="2016-06-01T000000.000000 America/New_York", sys_start="2016-08-01T000000.000000 America/New_York", sys_end=NULL)
(id=1, company='Allstate', coverage=10000, bus_start="2016-06-01T000000.000000 America/New_York", bus_end="2017-01-01T000000.000000 America/New_York", sys_start="2016-08-01T000000.000000 America/New_York", sys_end=NULL)
(id=1, company='State Farm', coverage=10000, bus_start="2017-01-01T000000.000000 America/New_York", bus_end="2017-06-01T000000.000000 America/New_York", sys_start="2016-08-01T000000.000000 America/New_York", sys_end=NULL)
(id=1, company='State Farm', coverage=30000, bus_start="2017-06-01T000000.000000 America/New_York", bus_end="2018-01-01T000000.000000 America/New_York", sys_start="2016-08-01T000000.000000 America/New_York", sys_end=NULL)
[select * from t1 order by id, bus_start] rc 0
(id=1, company='Allstate', coverage=20000, bus_start="2016-01-01T000000.000000 America/New_York", bus_end="2017-01-01T000000.000000 America/New_York", sys_start="2016-01-01T000000.000000 America/New_York", sys_end="2016-08-01T000000.000000 America/New_York")
(id=1, company='State Farm', coverage=30000, bus_start="2017-01-01T000000.000000 America/New_York", bus_end="2018-01-01T000000.000000 America/New_York", sys_start="2016-01-01T000000.000000 America/New_York", sys_end="2016-08-01T000000.000000 America/New_York")
[select * from t1_history order by id, bus_start] rc 0
```

After "2016-08-01", if our business applications were interested in the effective value on "2017-05-01", we would see the updated value with `coverage=10000`:

```
(id=1, company='State Farm', coverage=10000, bus_start="2017-01-01T000000.000000 America/New_York", bus_end="2017-06-01T000000.000000 America/New_York", sys_start="2016-08-01T000000.000000 America/New_York", sys_end=NULL)
[select * from t1 for business_time as of '2017-05-01' where id = 1] rc 0
```

### Querying data in a bi-temporal table

After "2016-08-01", if we want to run our business applications against dataset at a point in time on "2016-03-01" for business time "2017-05-01", we can use the following bi-temporal query:

```
(id=1, company='State Farm', coverage=30000, bus_start="2017-01-01T000000.000000 America/New_York", bus_end="2018-01-01T000000.000000 America/New_York", sys_start="2016-01-01T000000.000000 America/New_York", sys_end="2016-08-01T000000.000000 America/New_York")
[select * from t1 for system_time as of '2016-03-01T000000.000000', business_time as of '2017-05-01' where id = 1] rc 0
```

Remarks:

* The [`temporal-clause`](sql.html#temporal-clause) support any combinations of `SYSTEM_TIME` and `BUSINESS_TIME` specifications. For examples, both `FOR SYSTEM_TIME AS OF ..., BUSINESS_TIME FROM ... TO ...` and `FOR SYSTEM_TIME FROM ... TO ..., BUSINESS_TIME AS OF ...` are valid temporal `FOR` clauses.

### Setting temporal registers
Use [`SET` statements](sql.html#set-statements) to set temporal period specifications in registers to avoid typing the long [`temporal-clause`](sql.html#temporal-clause). This feature enables you to run existing applications against data from a certain point in time or a range of time  without changing the application itself. To do this, simply run `SET TEMPORAL` followed by any `SYSTEM_TIME` or `BUSINESS_TIME` specifications. Use `DISABLE` keyword to erase previous temporal settings.

For example,

```
[set temporal system_time as of '2016-03-01T000000.000000'] rc 0
(id=1, company='State Farm', coverage=30000, bus_start="2017-01-01T000000.000000 America/New_York", bus_end="2018-01-01T000000.000000 America/New_York", sys_start="2016-01-01T000000.000000 America/New_York", sys_end="2016-08-01T000000.000000 America/New_York")
[select * from t1 for business_time as of '2017-05-01' where id = 1] rc 0
[set temporal business_time as of '2017-05-01'] rc 0
(id=1, company='State Farm', coverage=30000, bus_start="2017-01-01T000000.000000 America/New_York", bus_end="2018-01-01T000000.000000 America/New_York", sys_start="2016-01-01T000000.000000 America/New_York", sys_end="2016-08-01T000000.000000 America/New_York")
[select * from t1 where id = 1] rc 0
```

Remarks:

* Unlike that all [expressions](sql.html#expr) and subqueries are supported in the [`temporal-clause`](sql.html#temporal-clause), time specifications in `SET TEMPORAL` statements	can only be literal datetime strings that are convertible to `datetimeus` datatype.
