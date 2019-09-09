---
title: Ruleset files
tags:
sidebar: mydoc_sidebar
permalink: ruleset.html
---

## Ruleset files

Comdb2 ruleset files have a .ruleset extension.  Ruleset files are optional.
If no ruleset files are loaded, the database will assume all SQL queries have
equal priority.  A ruleset file consists of optional blank lines, optional
comment lines, a required file header, and optional rule definition lines.

Blank lines are skipped.  Lines beginning with `#` are treated as comments and
skipped.

The file header line must be the first non-blank, non-comment line in the
file.  Currently, it must consist of the literal string `version 1`.

### Rule syntax

The syntax for rule definition lines is:

    rule <ruleNo> [propName1 propValue1] ... [propNameN propValueN]

A rule definition consists of an integer rule number and its associated
matching criteria.  The rule matching criteria consist of zero or more
property names and values.  The rule number must be an integer with a
value between one (1) and one thousand (1000).  All property names and
values are optional; a line without at least one property name and value
does nothing except verify the rule number.  When a property name is
specified its value must be specified as well.  Each property name may
appear more than once for a particular rule, even on the same line.
Only the last property value encountered for each combination of rule
number and property name will be retained.  Currently, the number of
rules is limited to one thousand (1000).  Property values are interpreted
based on the match mode configured for the rule.

### Property names and values

Property names are always delimited by whitespace.  All property values
except `flags`, `mode`, and `sql` are also delimited by whitespace.  The
special `flags`, `mode`, and `sql` property values are delimited by
semicolon because their values may contain whitespace.

The supported set of property names and their required formats is:

| Property Name | Property Value Format |
|---------------|------------------------|
|action         | One of `NONE`, `REJECT`, `UNREJECT`, `LOW_PRIO`, or `HIGH_PRIO`. |
|adjustment     | An integer between zero (0) and one million (1000000). |
|flags          | One or more of `NONE` and `STOP`, see [flags syntax](#flags-syntax). |
|mode           | One or more of `NONE`, `EXACT`, `GLOB`, `REGEXP`, and `NOCASE`, see [flags syntax](#flags-syntax). |
|originHost     | Any pattern string suitable for match mode.  May not contain whitespace. |
|originTask     | Any Pattern string suitable for match mode.  May not contain whitespace. |
|user           | Any Pattern string suitable for match mode.  May not contain whitespace. |
|sql            | Any Pattern string suitable for match mode.  May contain whitespace. |
|fingerprint    | SQLite compatible BLOB as string literal, e.g. `x'0123ABCD'`. |

### Flags syntax

For property values that represent a set of flags, e.g. for the `flags` and
`mode` properties, multiple choices from the set of possible values may be
specified, delimited by whitespace or commas.  The semicolon must be used to
delimit the end of these property values.

### Rule actions and adjustments

If all criteria specified for a rule are matched an action is taken.  If the
action is `NONE`, nothing is done.  This can be useful if the server has been
configured to emit a log message when a ruleset is matched, e.g. this allows
rulesets to effectively monitor activity without otherwise impacting it.  If
the action is `REJECT`, the SQL query will be marked as rejected.  This mark
of rejection can be removed by subsequently matching a rule with an action of
`UNREJECT`.  If the SQL query is marked as rejected and there are no further
rules to process, the SQL query will be rejected and an error will be emitted
to the client.  If the action is `LOW_PRIO`, the relative priority of the SQL
query will be decreased by the amount specified by the associated `adjustment`
property value.  If the action is `HIGH_PRIO`, the relative priority of the SQL
query will be increased by the amount specified by the associated `adjustment`
property value.

### Rule flags

The rule flag `NONE` has no effect.  If all criteria specified for a rule are
matched and the rule flag `STOP` is present, further rule matching is skipped
and the current ruleset result is returned.

### Match modes

The match mode `NONE` has no effect.  If a match mode of `NONE` is specified
and any rule criteria are present, the results are undefined.  The match mode
`EXACT`, as the name suggests, treats the property value as a string literal
to be matched exactly.  The match mode `GLOB` treats the property value as a
pattern compatible with the [SQLite GLOB operator syntax](https://www.sqlite.org/lang_expr.html#glob).
The match mode `REGEXP` treats the property value as a [regular expression](https://en.wikipedia.org/wiki/Regular_expression)
compatible with the [SQLite regular expression extension](https://www.sqlite.org/src/artifact?ci=trunk&filename=ext/misc/regexp.c).
The match mode `NOCASE` may be combined with another match mode to enable
case-insensitive matching.

### Example #1

```
version 1

rule 1 action REJECT
rule 1 fingerprint X'a9c8b6ddb5b9e55ee41b7f5a46ec4e45'
rule 1 flags STOP

rule 2 action LOW_PRIO
rule 2 adjustment 1000
rule 2 user Robert

rule 3 action HIGH_PRIO
rule 3 adjustment 1000
rule 3 mode GLOB
rule 3 originTask */cdb2sql

rule 4 action NONE
rule 4 mode REGEXP, NOCASE
rule 4 sql ^CREATE +.*$
```
