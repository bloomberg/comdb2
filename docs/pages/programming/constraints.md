---
title: Constraints
keywords: code
sidebar: mydoc_sidebar
permalink: constraints.html
---

## Comdb2 Constraints

This page documents the behavior of the Comdb2 constraints engine.

### Defining constraints

In a database system constraints are assertions that must hold true of the data within the database.  Examples of constraints are:

* Uniqueness constraints.
* Check constraints.
* Foreign key constraints.

Uniqueness constraints are implemented in Comdb2 as unique keys.  Keys are unique by default unless specified otherwise using the `dup` keyword table definition.

Check constraints enforce certain properties of the data stored in a record.  In Comdb2 the only kind of check constraints supported are `NULL` constraints.  Columns may not contain nulls unless explicitly allowed to by the csc2 directive `null=yes`.

Foreign key constraints ensure that a given key value in a table exists in a corresponding key in another table.  Foreign keys can be specified in the `constraints` section of your csc2 file.

### When constraints are checked

Comdb2 defers constraint checks until just before a transaction is committed.  This makes sense if you view constraints as assertions that must hold true for committed data.  In other words, they need not hold true for uncommitted data part of the way through a transaction.  There's a short example in the
[constraints](transaction_model.html#constraints) section.

### When cascade effects are applied

Comdb2 applies all cascade effects at the end of a transaction at the time at which constraints are checked.  Comdb2 only tries to apply cascade effects to repair a broken foreign key constraint.  This has the following implications:

* If you delete a primary key which is referred to by an `on delete cascade` foreign key constraint, and then in the same transaction re-insert the same primary key value, then any records referring to that primary key value will not be cascade deleted (as the constraint holds at commit time).

* If you update two primary key values to effectively swap them, and those primary keys are referred to by an `on delete cascade` foreign key constraint, then the records referring to the primary keys will not be updated as at commit time the constraints are seen to be held.
