---
title: Constraints
keywords: code
sidebar: mydoc_sidebar
permalink: constraints.html
---

## Comdb2 Constraints

In a database system constraints are assertions that must hold true of the data within the database. This page documents the behavior of the Comdb2 constraints engine.

### Constraint types

The following constraint types are supported by Comdb2:

#### Unique constraints
Uniqueness constraints are implemented in Comdb2 as unique keys. These constraints forbid duplicate entries from being added to the table.

In a table defined using the [Schema definition](table_schema.html) language, keys are unique by default unless specified otherwise using the `dup` keyword.  Historically Comdb2 does not consider `NULL` values as unique. That is, it would not allow multiple `NULL` values in a unique key. This behaviour could be changed by adding `uniqnulls` keyword (introduced in `7.0`) to the key definition.  With `uniqnulls`, `NULL` values are considered unique and, thus, be allowed multiple times in a unique key.

In standard data definition language (DDL), `UNIQUE` keyword must be used to create unique keys via [CREATE TABLE](#create-table-ddl) or [CREATE INDEX](#create-index) commands. `NULL` values are always considered unique.

#### NULL constraint
NULL constraints check the use of NULL values.

A column defined in [Schema definition](table_schema.html) language, by default, may not contain `NULL` values unless explicitly allowed to by the `null=yes` directive.

In standard data definition language (DDL), where columns are nullable by default, `NOT NULL` can be added to the column definition to forbid `NULL` values.

#### Foreign key constraint
Foreign key constraints ensure that a given key value in a (child) table exists in a corresponding key in another (parent) table.

In [Schema definition](table_schema.html) language, foreign keys can be specified in the `constraints` section.

#### Check constraint
Check constraints allow arbitrary expressions to be added to the table definition to constrain the values being added. Following is an example demonstrating check constraints:

```SQL
testdb> CREATE TABLE grades(id INT UNIQUE, name VARCHAR(60), grade VARCHAR(2), CONSTRAINT valid_grade_check CHECK (LOWER(grade) in ('a', 'b', 'c', 'd', 'e', 'f')))$$
[CREATE TABLE grades(id INT UNIQUE, name VARCHAR(60), grade VARCHAR(2), CONSTRAINT valid_grade_check CHECK (LOWER(grade) in ('a', 'b', 'c', 'd', 'e', 'f')))] rc 0

testdb> INSERT INTO grades VALUES(1, 'foo', 'Z')
[INSERT INTO grades VALUES(1, 'foo', 'Z')] failed with rc 403 CHECK constraint violation CHECK constraint failed for 'valid_grade_check' unable to add record rc = 320

testdb> INSERT INTO grades VALUES(1, 'foo', 'B')
+---------------+
| rows inserted |
+---------------+
| 1             |
+---------------+
[INSERT INTO grades VALUES(1, 'foo', 'B')] rc 0

```

The following objects are not allowed in the check constraint expressions:
* Subqueries
* Parameters
* Aggregate functions

### When constraints are checked

Comdb2 defers constraint checks until just before a transaction is committed.  This makes sense if you view constraints as assertions that must hold true for committed data. In other words, they need not hold true for uncommitted data part of the way through a transaction. There's a short example in the [constraints](transaction_model.html#constraints) section.

### When cascade effects are applied

Comdb2 applies all cascade effects at the end of a transaction at the time at which constraints are checked. Comdb2 only tries to apply cascade effects to repair a broken foreign key constraint. This has the following implications:

* If you delete a primary key which is referred to by an `on delete cascade` foreign key constraint, and then in the same transaction re-insert the same primary key value, then any records referring to that primary key value will not be cascade deleted (as the constraint holds at commit time).

* If you update two primary key values to effectively swap them, and those primary keys are referred to by an `on delete cascade` foreign key constraint, then the records referring to the primary keys will not be updated as at commit time the constraints are seen to be held.
