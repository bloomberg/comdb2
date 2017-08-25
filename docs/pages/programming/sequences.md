---
title: Sequence Generators
keywords: code
sidebar: mydoc_sidebar
permalink: sequences.html
---

## Comdb2 Sequence Generators

This page documents the behavior of the Comdb2 Sequence Generators.

### Defining Sequence Generators

Sequence generators are defined using SQL using the grammar defined [here](sql.html/#create-sequence). Note that altering sequence parameters may cause the sequence to redispensed values if the new parameters allow for dispensing previously dispensed values. For example, it is obvious that if an increasing sequence is changed to have a negative increment, the sequence values dispensed may overlap with previously dispensed values.

A list of all defined sequences and their parameters can be listed by querying the `comdb2_sequences` virtual table.

### Sequence Access Functions

Sequence values are accessed using the following functions. These functions can be incorportated where ever SQL functions can be used (SELECT, INSERT, etc.).

#### nextval(*name*)

The `nextval()` function returns the next value of the sequence generator specified by the *name* argument. 

#### currval(*name*)

The `currval()` function returns the last value distributed of the sequence generator specified by the *name* argument in the scope of the current transaction. 

### Master-only vs. Replicant Value Distribution

Sequence generators in Comdb2 can be configured to dispense sequence values in one of two ways. Master-only distribution goes to the cluster master to get a sequence value each time a client requests for a value through the `nextval()` function. Replicant distribution allows replicants to allocate a range of values (number of values allocated is controlled with the `CHUNK` parameter) from the master from which it will distribute to clients, removing a round trip to the master node.

Master-Only | Replicant
--- | ---
Strictly non-decreasing values dispensed | Possibly decreasing values dispensed
Bottlenecked by always going to master | Fast number generation


For applications that require sequence values to be unique AND monotonic, use master-only disribution. Applications that only require unique values, can use replicant distribution for faster value acquisition. Master-only distibution is enabled by default and reolicant distribution can be enabled by setting the `sequence_replicant_distribution` LRL option to `1`.
