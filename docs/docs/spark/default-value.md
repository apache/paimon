---
title: "Default Values"
sidebar_position: 8
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Default Values

Column default values require **Spark 3.4 or later**. Define defaults when creating a table or
change the default of an existing column with `ALTER TABLE`.

Paimon applies a configured default when a write supplies no value for that column. It also
replaces an explicit `NULL` in a nullable column with the configured default during writing.
This applies to writes from SQL and the DataFrame API. `NOT NULL` constraints are checked before
Paimon's write-time default substitution, so a null value in a `NOT NULL` column is rejected even
if the column has a default.

## Create Table

Defaults can be scalar values or values of complex types:

```sql
CREATE TABLE my_table (
    a BIGINT,
    b STRING DEFAULT 'my_value',
    c INT DEFAULT 5,
    numbers ARRAY<INT> DEFAULT ARRAY(1, 2, 3),
    scores MAP<INT, INT> DEFAULT MAP(1, 10, 2, 20),
    nested STRUCT<x: INT, y: INT> DEFAULT STRUCT(42, 7)
);
```

## Insert Table

Omit columns from an explicit column list to use their defaults:

```sql
INSERT INTO my_table (a) VALUES (1), (2);

SELECT a, b, c FROM my_table ORDER BY a;
-- 1  my_value  5
-- 2  my_value  5
```

`DEFAULT` requests the column default explicitly. A written `NULL` in the nullable column `c`
is also replaced by its default:

```sql
INSERT INTO my_table (a, b, c) VALUES (3, DEFAULT, NULL);

SELECT a, b, c FROM my_table WHERE a = 3;
-- 3  my_value  5
```

The complex columns receive the defaults declared above. Inspect individual fields instead
of printing an entire nested row:

```sql
SELECT numbers, scores[1], nested.x FROM my_table WHERE a = 3;
-- [1, 2, 3]  10  42
```

If a column has no default, an omitted or null value remains null, subject to the column's
nullability constraints. For write-path-specific handling of missing columns, see
[Schema Evolution on Write](./schema-evolution#column-alignment-by-write-path).

## Alter Default Value

A changed default is used by subsequent writes. Previously stored non-null values are preserved:

```sql
CREATE TABLE default_example (a INT, b INT DEFAULT 2);
INSERT INTO default_example (a) VALUES (1);

ALTER TABLE default_example ALTER COLUMN b SET DEFAULT 3;
INSERT INTO default_example (a) VALUES (2);

SELECT * FROM default_example ORDER BY a;
-- 1  2
-- 2  3
```

Change complex defaults with the same statement:

```sql
ALTER TABLE my_table ALTER COLUMN numbers SET DEFAULT ARRAY(4, 5);
ALTER TABLE my_table ALTER COLUMN scores SET DEFAULT MAP(3, 30);
INSERT INTO my_table (a) VALUES (4);

SELECT a, numbers, scores[3] FROM my_table WHERE a = 4;
-- 4  [4, 5]  30
```

## Limitation

- Complex default expressions currently have a string-quoting limitation: single quotes in
  `ARRAY`, `MAP`, and `STRUCT` string elements can be retained in the stored values. For example,
  `MAP('key1', 'value1')` stores a key containing the quote characters, so looking up `['key1']`
  returns `NULL`. The examples above use numeric elements to avoid this limitation. Supply
  complex values containing strings explicitly in the incoming query or DataFrame instead.
- `ALTER TABLE ADD COLUMN` cannot include a default. Add the column first, then set its default
  with a separate statement as shown below.
- Dynamic default expressions such as `current_timestamp()` and `current_date()` are not supported.
  Supply these values in the incoming query or DataFrame instead.

```sql
ALTER TABLE default_example ADD COLUMN d INT;
ALTER TABLE default_example ALTER COLUMN d SET DEFAULT 5;
```
