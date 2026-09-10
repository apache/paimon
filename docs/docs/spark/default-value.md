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
    tags ARRAY<STRING> DEFAULT ARRAY('tag1', 'tag2', 'tag3'),
    properties MAP<STRING, STRING> DEFAULT MAP('key1', 'value1', 'key2', 'value2'),
    nested STRUCT<x: INT, y: STRING> DEFAULT STRUCT(42, 'default_value')
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
SELECT tags, properties['key1'], nested.x FROM my_table WHERE a = 3;
-- [tag1, tag2, tag3]  value1  42
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
ALTER TABLE my_table ALTER COLUMN tags SET DEFAULT ARRAY('new_tag1', 'new_tag2');
ALTER TABLE my_table ALTER COLUMN properties SET DEFAULT MAP('new_key', 'new_value');
INSERT INTO my_table (a) VALUES (4);

SELECT a, tags, properties['new_key'] FROM my_table WHERE a = 4;
-- 4  [new_tag1, new_tag2]  new_value
```

## Limitation

- `ALTER TABLE ADD COLUMN` cannot include a default. Add the column first, then set its default
  with a separate statement as shown below.
- Dynamic default expressions such as `current_timestamp()` and `current_date()` are not supported.
  Supply these values in the incoming query or DataFrame instead.

```sql
ALTER TABLE default_example ADD COLUMN d INT;
ALTER TABLE default_example ALTER COLUMN d SET DEFAULT 5;
```
