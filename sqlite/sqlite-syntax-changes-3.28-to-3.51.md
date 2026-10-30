# SQL Syntax Changes in Stock SQLite: 3.28.0 → 3.51.2

An audit of **user-facing SQL syntax** added or changed in upstream SQLite between
3.28.0 (2019-04-16) and 3.51.2 (2026-01-09). This is a stock-SQLite audit only — it
does **not** consider comdb2 patches, comdb2 grammar, or which of these SQLite chooses
to expose. C-API, build-system, WASM, query-planner, and pure performance/bugfix
changes are excluded except where they change what a user can type or observe from SQL.

## How to read this report

* Every construct listed here was confirmed present in the stock `sqlite-3.51.2/`
  checkout and absent from the stock `sqlite-3.28.0/` checkout (grammar via
  `src/parse.y` / `tool/mkkeywordhash.c` / `src/tokenize.c`; functions via
  `src/func.c`, `src/date.c`, `src/json.c`, `src/window.c`), and cross-checked against
  the upstream change log / feature docs.
* **8 new grammar keywords** appear in 3.51.2 that were not keywords in 3.28.0:
  `ALWAYS`, `FIRST`, `GENERATED`, `LAST`, `MATERIALIZED`, `NULLS`, `RETURNING`, `WITHIN`.
  (No keywords were removed. `FILTER`, `OVER`, `WINDOW`, `RIGHT`, `FULL`, `OUTER`,
  `VIRTUAL`, `WITHOUT` were already keywords in 3.28.0.)

### Compile-time gating (relevant to reachability)

Several features exist in the 3.51.2 source but are only active under a build flag.
Whether they are reachable in a given build depends on how it was compiled:

| Feature | Gate | Default in stock amalgamation |
|---|---|---|
| Math functions (`sqrt`, `pow`, …) | `SQLITE_ENABLE_MATH_FUNCTIONS` | off in the raw amalgamation; on in the CLI and most distros |
| JSON / JSONB functions & `->`/`->>` | not `SQLITE_OMIT_JSON` | **on by default since 3.38.0** |
| `WITHIN GROUP (ORDER BY …)` ordered-set syntax | `SQLITE_ENABLE_ORDERED_SET_AGGREGATES` | off |
| `percentile()` / `median()` family | `SQLITE_ENABLE_PERCENTILE` | off (source built-in since 3.51.0) |
| Generated columns | not `SQLITE_OMIT_GENERATED_COLUMNS` | on |

### Feature-to-version quick index

| Feature | Introduced |
|---|---|
| `FILTER` clause on aggregates/windows | 3.30.0 |
| `NULLS FIRST` / `NULLS LAST` | 3.30.0 |
| Generated columns (`GENERATED ALWAYS AS … VIRTUAL/STORED`) | 3.31.0 |
| JSON path `$[#-N]` end-relative index | 3.31.0 |
| `iif()` (3-arg) | 3.32.0 |
| `LIKE … ESCAPE` wildcard-override (semantics) | 3.32.0 |
| `UPDATE … FROM` | 3.33.0 |
| Recursive CTE with multiple recursive terms | 3.34.0 |
| `substring()` alias | 3.34.0 |
| `RETURNING` clause | 3.35.0 |
| `ALTER TABLE … DROP COLUMN` | 3.35.0 |
| Built-in math functions (incl. `sign()`) | 3.35.0 |
| `MATERIALIZED` / `NOT MATERIALIZED` CTE hints | 3.35.0 |
| UPSERT: multiple `ON CONFLICT` clauses | 3.35.0 |
| `rowid` of a VIEW/subquery is now an error (semantics) | 3.36.0 |
| `STRICT` tables | 3.37.0 |
| JSON `->` and `->>` operators; JSON built-in by default | 3.38.0 |
| `format()` (rename of `printf()`) | 3.38.0 |
| `unixepoch()` + `auto`/`julianday` date modifiers | 3.38.0 |
| `RIGHT JOIN` / `FULL OUTER JOIN` | 3.39.0 |
| `IS [NOT] DISTINCT FROM` | 3.39.0 |
| `HAVING` without `GROUP BY` | 3.39.0 |
| `unhex()` | 3.41.0 |
| Excess parens on RHS of `IN` (semantics) | 3.41.0 |
| JSON5 input; `subsec` date modifier | 3.42.0 |
| `octet_length()`, `timediff()`, time-shift & date-shift modifiers | 3.43.0 |
| `ORDER BY` inside aggregate arguments | 3.44.0 |
| `concat()`, `concat_ws()`, `string_agg()` | 3.44.0 |
| new `strftime()` conversion letters | 3.44.0, 3.46.0 |
| JSONB format + `jsonb_*()` functions | 3.45.0 |
| `json_pretty()` | 3.46.0 |
| Underscore digit separators in numeric literals | 3.46.0 |
| `ceiling`/`floor` date modifiers | 3.46.0 |
| `->>` negative array index | 3.47.0 |
| Expression as 2nd arg to `RAISE()` | 3.47.0 |
| `group_concat()` empty-input returns `''` (semantics) | 3.47.0 |
| `iif()`/`if()` 2-arg form and `if()` alias | 3.48.0 |
| `iif()`/`if()` variadic (CASE-like) form | 3.49.0 |
| `unistr()` / `unistr_quote()` | 3.50.0 |
| `printf`/`format` `%#q`/`%#Q` alternate form | 3.50.0 |
| `concat_ws()` keeps empty strings (semantics) | 3.50.2 |
| `WITHIN GROUP` ordered-set aggregates; `percentile*`/`median` built-in | 3.51.0 |
| `jsonb_each()` / `jsonb_tree()` | 3.51.0 |

---

# 1. Statements, clauses, and operators

## `FILTER` (conditional aggregation) — *3.30.0*

Appends a `FILTER (WHERE <expr>)` clause to an aggregate or window-function call so that
only rows satisfying `<expr>` feed that particular aggregate. This lets you compute
several differently-filtered aggregates in one pass instead of using correlated
subqueries or `CASE` tricks. It sits between the argument list and any `OVER` clause.

Links:

* https://sqlite.org/lang_aggfunc.html#aggfilter
* https://sqlite.org/syntax/filter-clause.html

```sql
-- Total, plus only-shipped and only-paid counts, in a single scan
SELECT
  count(*)                                    AS total,
  count(*)    FILTER (WHERE status='shipped') AS shipped,
  sum(amount) FILTER (WHERE status='paid')    AS paid_total
FROM orders;
```

## `NULLS FIRST` / `NULLS LAST` (null ordering) — *3.30.0*

An `ORDER BY` ordering term may end with `NULLS FIRST` or `NULLS LAST` to control where
NULLs sort relative to non-NULLs, independent of `ASC`/`DESC`. If omitted, the historical
default is preserved (NULLs sort first for `ASC`, last for `DESC`).

Links:

* https://sqlite.org/lang_select.html#the_order_by_clause
* https://sqlite.org/syntax/ordering-term.html

```sql
-- Highest priority first, but push NULL priorities to the very bottom
SELECT id, priority FROM tasks
ORDER BY priority DESC NULLS LAST;
```

## `GENERATED ALWAYS AS` / `VIRTUAL` / `STORED` (generated columns) — *3.31.0*

A `CREATE TABLE` column may compute its value from other columns of the same row using
`[GENERATED ALWAYS] AS (<expr>)`, optionally suffixed with `VIRTUAL` (recomputed on read;
the default, stores nothing) or `STORED` (computed and persisted on write). Generated
columns are read-only — you populate them by writing their source columns. The expression
must be deterministic and reference only same-row columns; such a column cannot be part of
the `PRIMARY KEY` or have a `DEFAULT`, and only `VIRTUAL` ones can be added later via
`ALTER TABLE … ADD COLUMN`.

Links:

* https://sqlite.org/gencol.html
* https://sqlite.org/syntax/column-def.html

```sql
CREATE TABLE t1(
  a INTEGER PRIMARY KEY,
  b INT,
  c TEXT,
  d INT  GENERATED ALWAYS AS (a * abs(b)) VIRTUAL,       -- computed on read
  e TEXT GENERATED ALWAYS AS (substr(c, b, b+1)) STORED  -- persisted on write
);
INSERT INTO t1(a,b,c) VALUES (2, -5, 'hello world');     -- d, e are auto-derived
```

## `UPDATE … FROM` (join-driven update) — *3.33.0*

An `UPDATE` may carry a `FROM` clause after `SET`, joining the target table to other
tables/subqueries whose columns can then be used in `SET` expressions and the `WHERE`
condition (PostgreSQL-style). The target table should not itself appear in `FROM` (except
as a differently-aliased self-join). If the join yields multiple source rows per target
row, one is chosen arbitrarily.

Links:

* https://sqlite.org/lang_update.html#update_from
* https://sqlite.org/syntax/update-stmt.html

```sql
-- Decrement inventory by each item's total daily sales
UPDATE inventory
   SET quantity = quantity - daily.amt
  FROM (SELECT itemId, sum(quantity) AS amt FROM sales GROUP BY itemId) AS daily
 WHERE inventory.itemId = daily.itemId;
```

## `RETURNING` (return affected rows) — *3.35.0*

`DELETE`, `INSERT`, and `UPDATE` may end with a `RETURNING` clause that produces one result
row per affected database row, so a client reads back generated values (autoincrement
rowids, `DEFAULT`s, computed columns) without a second query. It accepts a comma-separated
list of expressions, `*`, or `expr AS alias`. For `INSERT`/`UPDATE` the values are
post-change; for `DELETE`, pre-change. Restrictions: not on virtual-table `UPDATE`/`DELETE`,
not inside triggers, cannot be a subquery, output order is arbitrary, and no top-level
aggregate/window functions.

Links:

* https://sqlite.org/lang_returning.html
* https://sqlite.org/syntax/returning-clause.html

```sql
INSERT INTO log(msg) VALUES('hello') RETURNING id, created_at;   -- read back generated cols
DELETE FROM sessions WHERE expires < unixepoch() RETURNING *;    -- all cols of deleted rows
UPDATE account SET balance = balance - 10 WHERE id = 42
  RETURNING balance AS new_balance;
```

## `ALTER TABLE … DROP COLUMN` (remove a column) — *3.35.0*

Removes an existing column and rewrites the table to purge its data. The column cannot be
dropped if it is part of a `PRIMARY KEY`, is `UNIQUE` or indexed (incl. partial indexes),
appears in a `CHECK`/foreign-key/generated-column constraint, or is referenced by a trigger
or view. Complements `RENAME COLUMN` (added earlier, in 3.25.0).

Links:

* https://sqlite.org/lang_altertable.html#altertabdropcol
* https://sqlite.org/syntax/alter-table-stmt.html

```sql
ALTER TABLE employees DROP COLUMN middle_name;   -- 'COLUMN' keyword optional
```

## UPSERT: multiple `ON CONFLICT` clauses — *3.35.0*

Generalizes UPSERT (originally 3.24.0) so one `INSERT` may carry several `ON CONFLICT`
clauses, evaluated in order — the first whose conflict target matches fires for a given
row. The **final** clause may omit its conflict target and still use `DO UPDATE`, acting as
a catch-all for any uniqueness violation not handled earlier.

Links:

* https://sqlite.org/lang_upsert.html
* https://sqlite.org/syntax/upsert-clause.html

```sql
INSERT INTO t(a,b,c) VALUES(1,2,3)
  ON CONFLICT(a) DO UPDATE SET c = excluded.c   -- conflict on a
  ON CONFLICT(b) DO UPDATE SET c = excluded.c   -- else conflict on b
  ON CONFLICT     DO NOTHING;                    -- catch-all, no target
```

## `MATERIALIZED` / `NOT MATERIALIZED` (CTE hints) — *3.35.0*

Optional planner hints on a `WITH` common table expression, placed between `AS` and the
parenthesized body: `MATERIALIZED` forces the CTE result into a transient table computed
once; `NOT MATERIALIZED` forces it to be inlined into the enclosing query. Without a hint,
SQLite chooses heuristically.

Links:

* https://sqlite.org/lang_with.html#materialized_and_not_materialized_common_table_expressions
* https://sqlite.org/syntax/common-table-expression.html

```sql
WITH big AS MATERIALIZED (SELECT * FROM huge WHERE flag=1)   -- compute once, reuse
SELECT * FROM big a JOIN big b ON a.k = b.k;

WITH sub AS NOT MATERIALIZED (SELECT id FROM t WHERE x>0)    -- inline into outer query
SELECT * FROM sub;
```

## Recursive CTE with multiple recursive terms — *3.34.0*

The recursive part of a recursive CTE may be a compound of two or more recursive
`SELECT`s, not just one. All arms of that compound must use the same connector — all
`UNION` or all `UNION ALL` — matching the operator between the initial and recursive parts.
This makes multi-branch graph traversals easier to express.

Links:

* https://sqlite.org/lang_with.html
* https://sqlite.org/syntax/recursive-cte.html

```sql
-- Walk edges in both directions using two recursive terms
WITH RECURSIVE nodes(id) AS (
  VALUES(1)
  UNION
  SELECT edge.dst FROM edge, nodes WHERE edge.src = nodes.id   -- forward
  UNION
  SELECT edge.src FROM edge, nodes WHERE edge.dst = nodes.id   -- backward
)
SELECT DISTINCT id FROM nodes ORDER BY id;
```

## `STRICT` (rigid table typing) — *3.37.0*

Add the `STRICT` keyword after the closing `)` of `CREATE TABLE` to enable rigid column
typing instead of SQLite's flexible affinity. Every column must declare a type, and it must
be exactly one of `INT`, `INTEGER`, `REAL`, `TEXT`, `BLOB`, or the special `ANY`. Values
that are not NULL or the declared type (or losslessly convertible) raise
`SQLITE_CONSTRAINT_DATATYPE`. `ANY` stores values verbatim with no coercion. `STRICT` may
be combined with `WITHOUT ROWID` as comma-separated table options.

Links:

* https://sqlite.org/stricttables.html
* https://sqlite.org/syntax/table-options.html

```sql
CREATE TABLE t1(
  a INTEGER PRIMARY KEY,
  b TEXT NOT NULL,
  c ANY                 -- stored verbatim, no numeric coercion
) STRICT;

CREATE TABLE t2(id INTEGER PRIMARY KEY, v TEXT) STRICT, WITHOUT ROWID;
INSERT INTO t1(a) VALUES('xyz');   -- SQLITE_CONSTRAINT_DATATYPE: 'xyz' is not INTEGER
```

## `->` and `->>` (JSON extract operators) — *3.38.0* (negative index *3.47.0*)

Two infix operators extract a subcomponent from JSON on their left side. The right operand
is either a full JSON path (`'$.a.b[2]'`) or, PostgreSQL-style, a bare object label or an
integer array index. `->` returns a **JSON** representation of the selected element; `->>`
returns a plain **SQL value** (TEXT/INTEGER/REAL/NULL). Since 3.47.0, a negative integer
index counts from the end of an array (`-1` = last). This release also made JSON built-in
and enabled by default (previously the optional JSON1 extension).

Links:

* https://sqlite.org/json1.html#the_and_operators
* https://sqlite.org/lang_expr.html#the_like_glob_regexp_match_and_extract_operators

Given the document `'{"a":2,"c":[4,5,{"f":7}]}'`:

| Expression | Result |
|---|---|
| `doc -> '$.c[2].f'` | `'7'`  (JSON) |
| `doc ->> '$.c[2].f'` | `7`  (SQL integer) |
| `'{"a":"xyz"}' -> '$.a'` | `'"xyz"'`  (quoted JSON) |
| `'{"a":"xyz"}' ->> '$.a'` | `xyz`  (SQL text) |
| `'{"a":"xyz"}' ->> 'a'` | `xyz`  (bare label, PG-style) |
| `'[10,20,30]' ->> -1` | `30`  (last element; 3.47.0+) |

## `RIGHT JOIN` / `FULL OUTER JOIN` (outer joins) — *3.39.0*

Adds `RIGHT [OUTER] JOIN` and `FULL [OUTER] JOIN` to the join grammar (previously only
`LEFT`/`INNER`/`CROSS`/natural joins were supported). `RIGHT JOIN` keeps all rows of the
right table; `FULL OUTER JOIN` keeps unmatched rows from both sides, NULL-filling missing
columns. (Note: the keywords `RIGHT`/`FULL`/`OUTER` already tokenized in 3.28.0 but were
rejected at code-generation; 3.39.0 implements them.)

Links:

* https://sqlite.org/lang_select.html#strange_join_names
* https://sqlite.org/syntax/join-operator.html

```sql
SELECT * FROM a RIGHT JOIN b       ON a.id = b.a_id;   -- all rows of b
SELECT * FROM a FULL OUTER JOIN b  ON a.id = b.a_id;   -- all rows of both
```

## `IS [NOT] DISTINCT FROM` (null-aware comparison) — *3.39.0*

Two NULL-aware comparison operators for cross-dialect compatibility.
`x IS NOT DISTINCT FROM y` is an alternate spelling of SQLite's `x IS y` (true when equal or
both NULL); `x IS DISTINCT FROM y` is an alternate spelling of `x IS NOT y`. Unlike
`=`/`<>`, these never return NULL — always 0 or 1.

Links:

* https://sqlite.org/lang_expr.html#isisnot

```sql
SELECT NULL IS NOT DISTINCT FROM NULL;  -- 1  (same as NULL IS NULL)
SELECT 1    IS DISTINCT FROM NULL;      -- 1  (same as 1 IS NOT NULL)
```

## `HAVING` without `GROUP BY` — *3.39.0* (grammar relaxation)

`HAVING` may now be used on an aggregate query that has no `GROUP BY` (i.e. the whole table
aggregated into one implicit group). Previously this was rejected.

Links:

* https://sqlite.org/lang_select.html#the_having_clause

```sql
SELECT count(*) FROM t HAVING count(*) > 10;   -- now legal without GROUP BY
```

## `WITHIN GROUP (ORDER BY …)` (ordered-set aggregates) — *3.51.0*

Introduces the SQL-standard ordered-set aggregate syntax
`percentile_cont(P) WITHIN GROUP (ORDER BY Y)` /
`percentile_disc(P) WITHIN GROUP (ORDER BY Y)`, as an alternative to the legacy positional
form `percentile_cont(Y,P)`. **This syntax is only compiled in when built with
`-DSQLITE_ENABLE_ORDERED_SET_AGGREGATES=1`** (the `WITHIN` keyword was added to the grammar
in this release). The percentile/median functions themselves are covered in §2.5.

Links:

* https://sqlite.org/percentile.html
* https://sqlite.org/lang_aggfunc.html

```sql
-- Median salary per department, SQL-standard ordered-set form
SELECT dept,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY salary) AS median_salary
FROM employees
GROUP BY dept;
```

## `ORDER BY` inside aggregate arguments (ordered aggregation) — *3.44.0*

An aggregate function call may include an `ORDER BY` clause after its last argument,
controlling the order rows are fed to the aggregate. This matters for order-sensitive
aggregates such as `group_concat()`/`string_agg()` and `json_group_array()`.

Links:

* https://sqlite.org/lang_aggfunc.html
* https://sqlite.org/lang_aggfunc.html#group_concat

```sql
SELECT string_agg(name, ', ' ORDER BY name DESC) FROM t;   -- concat in descending order
SELECT json_group_array(x ORDER BY x) FROM t;              -- ordered JSON array
```

## Underscore digit separators in numeric literals — *3.46.0*

A single `_` may appear between two digits of any numeric literal (integer, real, hex) to
aid readability. It may not be leading, trailing, doubled, or adjacent to `.`, an exponent
marker, or the `0x` boundary.

Links:

* https://sqlite.org/lang_expr.html#literal_values_constants_
* https://sqlite.org/syntax/numeric-literal.html

```sql
SELECT 1_000_000;   -- 1000000
SELECT 0xFF_FF;     -- 65535
SELECT 3.141_592;   -- 3.141592
SELECT 1__0;        -- error: only one underscore, and only between two digits
```

---

# 2. New and renamed SQL functions

## 2.1 Math functions — *3.35.0*

A full set of scalar math functions was added (built into the amalgamation, active with
`-DSQLITE_ENABLE_MATH_FUNCTIONS`; on in the CLI and most distros):

`acos, acosh, asin, asinh, atan, atan2, atanh, ceil, ceiling, cos, cosh, degrees, exp,
floor, ln, log, log2, log10, mod, pi, pow, power, radians, sign, sin, sinh, sqrt, tan,
tanh, trunc`

`log(X)` is base-10; `log(B,X)` is base-B. `pow`/`power` and `ceil`/`ceiling` are aliases.
`sign(X)` returns -1/0/+1 for negative/zero/positive `X` (documented with the core
functions rather than the math page).

Links:

* https://sqlite.org/lang_mathfunc.html
* https://sqlite.org/lang_corefunc.html#sign

```sql
SELECT sqrt(2), pow(2,10), pi();   -- 1.4142135623731 | 1024.0 | 3.14159265358979
SELECT log(1000), log(2,8);        -- 3.0 (base 10) | 3.0 (base 2)
SELECT sign(-4), sign(0), sign(9); -- -1 | 0 | 1
```

## 2.2 String / miscellaneous scalar functions

New scalar functions added across the range:

| Function | Version | What it does |
|---|---|---|
| `iif(X,Y,Z)` | 3.32.0 | `Y` if `X` true else `Z` (compact `CASE`) |
| `iif(X,Y)` / `if(…)` alias | 3.48.0 | 2-arg form (else = NULL); `if()` is an alias for `iif()` |
| `iif(C1,V1,C2,V2,…[,ELSE])` | 3.49.0 | variadic CASE-like form |
| `substring(X,Y[,Z])` | 3.34.0 | alias for `substr()` |
| `format(…)` | 3.38.0 | rename of `printf()` (`printf` kept as permanent alias) |
| `concat(X,…)` | 3.44.0 | concatenate args as text, skipping NULLs |
| `concat_ws(SEP,X,…)` | 3.44.0 | concatenate with separator, skipping NULL args |
| `octet_length(X)` | 3.43.0 | number of bytes in the stored encoding of `X` |
| `unhex(X[,Y])` | 3.41.0 | decode hex string to BLOB (`Y` = ignorable chars) |
| `unistr(X)` | 3.50.0 | decode `\uXXXX` / `\UXXXXXXXX` / `\\` escapes |
| `unistr_quote(X)` | 3.50.0 | render `X` as an SQL literal with `\u`/`\U` escapes |
| `sign(X)` | 3.35.0 | see §2.1 |
| `subtype(X)` | added post-3.28.0 (exact release **uncertain**; present in 3.51.2) | returns the integer subtype tag of expression `X` (niche; used with JSON/pointer subtypes) |

Links:

* https://sqlite.org/lang_corefunc.html#iif
* https://sqlite.org/lang_corefunc.html#concat
* https://sqlite.org/lang_corefunc.html#unhex
* https://sqlite.org/lang_corefunc.html#unistr
* https://sqlite.org/printf.html

```sql
SELECT iif(bal < 0, 'overdrawn', 'ok');        -- 3-arg (3.32)
SELECT iif(score >= 60, 'pass');               -- 2-arg -> NULL if false (3.48)
SELECT iif(x<0,'neg', x=0,'zero', 'pos');      -- variadic CASE (3.49)
SELECT concat('a', NULL, 'b', 3);              -- 'ab3'  (NULL skipped)
SELECT 'a' || NULL || 'b';                     -- NULL   (contrast with concat)
SELECT concat_ws('-', 2023, 5, 16);            -- '2023-5-16'
SELECT length('café'), octet_length('café');   -- 4 chars | 5 UTF-8 bytes
SELECT unhex('41 42 43', ' ');                 -- x'414243'  (spaces ignored)
SELECT unistr('Aé\U0001F600');            -- 'Aé😀'
```

## 2.3 Aggregate functions

| Function | Version | What it does |
|---|---|---|
| `string_agg(X,Y)` | 3.44.0 | PG/SQL-Server alias for `group_concat(X,Y)` |
| `median(Y)` | 3.51.0 | median of non-NULL `Y` (= `percentile_cont(Y,0.5)`) |
| `percentile(Y,P)` | 3.51.0 | value at percentile `P` in 0.0–100.0 |
| `percentile_cont(Y,P)` | 3.51.0 | interpolated value at fraction `P` in 0.0–1.0 |
| `percentile_disc(Y,P)` | 3.51.0 | nearest actual input value at fraction `P` |

The `percentile*`/`median` family is built into the source since 3.51.0 but **requires
`-DSQLITE_ENABLE_PERCENTILE`**; the SQL-standard `WITHIN GROUP` form additionally requires
`-DSQLITE_ENABLE_ORDERED_SET_AGGREGATES` (see §1). Combine `string_agg` with the in-aggregate
`ORDER BY` (§1) for deterministic output.

Links:

* https://sqlite.org/lang_aggfunc.html#group_concat
* https://sqlite.org/percentile.html

```sql
SELECT string_agg(city, '; ' ORDER BY city) FROM customers;
SELECT median(response_ms), percentile(response_ms, 95) FROM requests;   -- p50 and p95
```

## 2.4 Date / time functions and modifiers

New date/time **functions**:

| Function | Version | What it does |
|---|---|---|
| `unixepoch(…)` | 3.38.0 | time value as integer Unix seconds |
| `timediff(A,B)` | 3.43.0 | signed `A−B` as ISO-ish interval `±YYYY-MM-DD HH:MM:SS.SSS` |

New date/time **modifiers** (usable in `date()`/`time()`/`datetime()`/`strftime()`/`unixepoch()`):

| Modifier | Version | Effect |
|---|---|---|
| `auto` | 3.38.0 | infer Julian-day vs Unix-timestamp from magnitude |
| `julianday` | 3.38.0 | force numeric argument to be read as a Julian day |
| `subsec` / `subsecond` | 3.42.0 | include fractional seconds (ms) in output |
| `±YYYY-MM-DD HH:MM:SS.SSS` | 3.43.0 | shift by a full offset given in one modifier |
| `floor` | 3.46.0 | month/year overflow rounds down to last valid day |
| `ceiling` | 3.46.0 | month/year overflow rolls into the next month |

New `strftime()` conversion letters: `%e %F %I %k %l %p %P %R %T %u` (3.44.0);
`%G %g %U %V` ISO-week substitutions (3.46.0).

Links:

* https://sqlite.org/lang_datefunc.html#the_unixepoch_function
* https://sqlite.org/lang_datefunc.html#modifiers

```sql
SELECT unixepoch('2022-02-22 12:00:00');            -- 1645531200
SELECT timediff('2023-06-01', '2023-05-16');        -- +0000-00-16 00:00:00.000
SELECT datetime('2023-01-01', '+0001-02-03');       -- 2024-03-04 00:00:00
SELECT date('2023-01-31', '+1 month', 'floor');     -- 2023-02-28
SELECT date('2023-01-31', '+1 month', 'ceiling');   -- 2023-03-03
SELECT strftime('%F %T', '2023-11-01 09:05:03');    -- 2023-11-01 09:05:03
```

## 2.5 JSON and JSONB functions

JSON became a **core, on-by-default** feature in 3.38.0 (previously the optional JSON1
extension). Beyond the functions carried over from JSON1, these were added:

| Function / feature | Version | What it does |
|---|---|---|
| JSON path `$[#-N]` | 3.31.0 | address arrays from the end; `$[#]` appends |
| `json_error_position(X)` | 3.42.0 | byte offset of first JSON parse error, else 0 |
| JSON5 input | 3.42.0 | accept lenient JSON5 (unquoted keys, comments, hex, trailing commas, `Infinity`/`NaN`) — output stays strict JSON |
| `json_pretty(X[,IND])` | 3.46.0 | indented multi-line rendering (`IND` = indent string) |
| JSONB blob format + `jsonb_*()` | 3.45.0 | binary on-disk JSON; each text-returning JSON fn has a `jsonb_`-prefixed twin (`jsonb`, `jsonb_array`, `jsonb_extract`, `jsonb_insert`, `jsonb_set`, `jsonb_replace`, `jsonb_remove`, `jsonb_object`, `jsonb_patch`, `jsonb_group_array`, `jsonb_group_object`) |
| `jsonb_each()` / `jsonb_tree()` | 3.51.0 | like `json_each`/`json_tree` but emit JSONB for array/object values |

Links:

* https://sqlite.org/json1.html#jsonb
* https://sqlite.org/json1.html#json5_extensions
* https://sqlite.org/json1.html#jpretty
* https://sqlite.org/json1.html#jsonpath

```sql
SELECT json_extract('[10,20,30]', '$[#-1]');       -- 30  (last element)
SELECT json('{ a:''hi'', b:0x1F, /* c5 */ }');     -- JSON5 in -> {"a":"hi","b":31}
SELECT json_pretty('{"a":1,"b":[2,3]}');           -- indented multi-line JSON
INSERT INTO doc(b) VALUES (jsonb('{"a":1,"b":[2,3]}'));  -- store as JSONB blob
SELECT json_extract(b, '$.b[1]') FROM doc;         -- 3  (reads JSONB directly)
```

---

# 3. SQL-visible semantic changes (no new syntax)

These change what an existing statement does or accepts, without adding syntax — worth
flagging because behavior can differ from 3.28.0.

## `LIKE … ESCAPE` overrides wildcards — *3.32.0*

The `ESCAPE` character now always makes a following `%` or `_` a literal, matching
PostgreSQL — previously an escaped wildcard could still act as a wildcard in some contexts.

Links: https://sqlite.org/lang_expr.html#the_like_glob_regexp_match_and_extract_operators

```sql
SELECT * FROM items WHERE label LIKE '100\% cotton' ESCAPE '\';  -- '\%' = literal '%'
```

## `rowid` of a VIEW or subquery is now an error — *3.36.0*

Referencing `rowid`/`_rowid_`/`oid` of a VIEW or subquery previously returned NULL
silently; it now raises an error at prepare time.

Links: https://sqlite.org/rowidtable.html

```sql
CREATE VIEW v AS SELECT a,b FROM t;
SELECT rowid FROM v;   -- error in 3.36.0+ (previously NULL)
```

## Excess parentheses on RHS of `IN` are ignored — *3.41.0*

Redundant parentheses around a subquery on the right of `IN` now parse the same as one set,
so `x IN ((SELECT …))` behaves like `x IN (SELECT …)`.

Links: https://sqlite.org/lang_expr.html#the_in_and_not_in_operators

```sql
SELECT * FROM t WHERE id IN ((SELECT id FROM s));   -- accepted, same as single parens
```

## `group_concat()` empty-input result — *3.47.0*

`group_concat()` now returns an empty string (not NULL) when its single input value is an
empty string.

Links: https://sqlite.org/lang_aggfunc.html#group_concat

## `RAISE()` accepts an expression — *3.47.0*

The second argument of `RAISE(ABORT|FAIL|ROLLBACK, …)` in triggers may now be any
expression, not just a string literal, so error messages can be computed.

Links: https://sqlite.org/lang_createtrigger.html#the_raise_function

```sql
CREATE TRIGGER t BEFORE INSERT ON accounts BEGIN
  SELECT RAISE(ABORT, 'balance too low: ' || NEW.balance) WHERE NEW.balance < 0;
END;
```

## `printf`/`format` `%#q` / `%#Q` alternate form — *3.50.0*

The alternate-form flag `#` on the `%q`/`%Q` conversions makes control characters emit as
backslash escapes rather than raw bytes, for safer quoted-SQL output.

Links: https://sqlite.org/printf.html

```sql
SELECT format('%#q', 'line1' || char(10) || 'line2');   -- newline rendered as an escape
```

## `concat_ws()` keeps empty-string arguments — *3.50.2*

`concat_ws()` now includes `''` arguments in the output (with surrounding separators);
only NULL arguments are skipped. This changes results for callers passing empty strings.

Links: https://sqlite.org/lang_corefunc.html#concat_ws

```sql
SELECT concat_ws('-', 'a', '', 'b');   -- 'a--b'  (was 'a-b' before 3.50.2)
```

## `printf`/`format` negative-float formatting — *3.51.0*

`printf()`/`format()` now omit a leading `-` from negative floating-point values that round
to zero at the requested precision (e.g. `format('%.1f', -0.01)` no longer yields `-0.0`).

Links: https://sqlite.org/printf.html

## `generate_series()` constraint pushdown — *3.47.0*

The built-in `generate_series()` table-valued function now honors constraints on its `value`
column (e.g. `WHERE value <= N`) and can be used with fewer explicit arguments.

Links: https://sqlite.org/series.html

```sql
SELECT value FROM generate_series(1) WHERE value <= 5;   -- 1,2,3,4,5
```

---

# 4. Notes, exclusions, and uncertainties

* **`subtype()`** — confirmed present in stock 3.51.2 and absent in 3.28.0, but its exact
  introducing release was not confirmed from the change log (the small function is not
  prominently documented). Treat the version as **uncertain**; presence is certain.
* **`sign()`** — grouped with the 3.35.0 math-functions batch (source confirms it as new
  since 3.28.0 and gated with the math functions); the 3.35.0 release note names the batch
  collectively rather than each function, so the exact-release attribution is inferred, not
  quoted.
* **Not in range (checked and excluded):** `ALTER TABLE … ALTER COLUMN … SET/DROP NOT NULL`
  is **3.53.0 (2026-04-09)**, after 3.51.2. No `REINDEX EXPRESSIONS`, no TEMP-trigger
  main-schema change, and no `VACUUM INTO` URI syntax exist in this range (these surfaced
  only as hallucinations during research and were rejected).
* **Baseline confirmations (already in 3.28.0, not new):** window functions (`OVER`,
  `WINDOW`, frame specs incl. `GROUPS`/`EXCLUDE`/`TIES`), single-clause UPSERT,
  `ALTER TABLE … RENAME COLUMN` (3.25.0), `WITHOUT ROWID`, hexadecimal integer literals
  (`0x…`, 3.8.6).
* **PRAGMAs** (configuration/introspection, not query grammar) added in range include
  `PRAGMA trusted_schema`, `PRAGMA hard_heap_limit`, `PRAGMA analysis_limit`,
  `PRAGMA table_list`, and `PRAGMA function_list`/`module_list`/`pragma_list` compiled in by
  default. Listed for completeness; see https://sqlite.org/pragma.html.
