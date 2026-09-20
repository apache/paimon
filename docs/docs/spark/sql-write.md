---
title: "SQL Writes"
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

# SQL Writes

Choose a write operation according to what should happen to existing rows:

| Operation | Effect |
| --- | --- |
| `INSERT INTO` | Append rows, or merge records by key in a primary key table. |
| `INSERT OVERWRITE` | Replace the table or selected partitions; scope depends on overwrite mode. |
| `UPDATE` / `DELETE` | Modify or remove rows matching a predicate. |
| `MERGE INTO` | Apply conditional updates, inserts, and deletes from a source. |
| [`COPY INTO`](./copy-into) | Import CSV, JSON, or Parquet files, or export query results. |

Configure the [catalog and SQL extensions](./quick-start#setup) before running these statements.
For new columns arriving with a write, see [Schema Evolution on Write](./schema-evolution).

## Insert Table

The `INSERT` statement inserts new rows into a table or overwrites the existing data in the table. The inserted rows can be specified by value expressions or result from a query.

**Syntax**

```sql
INSERT { INTO | OVERWRITE } table_identifier [ part_spec ] [ column_list ] { value_expr | query };
```
**Parameters**

- **table_identifier**: Specifies a table name, which may be optionally qualified with a database name.

- **part_spec**: An optional parameter that specifies a comma-separated list of key and value pairs for partitions.

- **column_list**: An optional parameter that specifies a comma-separated list of columns belonging to the table_identifier table. Spark will reorder the columns of the input query to match the table schema according to the specified column list.

  Note: Since Spark 3.4, INSERT INTO commands with explicit column lists comprising fewer columns than the target table will automatically add the corresponding default values for the remaining columns (or NULL for any column lacking an explicitly-assigned default value). In Spark 3.3 or earlier, column_list's size must be equal to the target table's column size, otherwise these commands would have failed.

- **value_expr** ( { value | NULL } [ , … ] ) [ , ( … ) ]: Specifies the values to be inserted. Either an explicitly specified value or a NULL can be inserted. A comma must be used to separate each value in the clause. More than one set of values can be specified to insert multiple rows.

For more information, please check the syntax document: [Spark INSERT Statement](https://spark.apache.org/docs/latest/sql-ref-syntax-dml-insert-table.html)

### Insert Into

Use `INSERT INTO` to apply records and changes to tables.

```sql
INSERT INTO my_table SELECT ...
```

### Insert Overwrite

In static mode, `INSERT OVERWRITE` without a partition filter replaces the whole table.
In dynamic mode, it replaces only partitions represented by the input rows.

```sql
INSERT OVERWRITE my_table SELECT ...
```

#### Insert Overwrite Partition

Use `INSERT OVERWRITE` to overwrite a partition.

```sql
INSERT OVERWRITE my_table PARTITION (key1 = value1, key2 = value2, ...) SELECT ...
```

#### Dynamic Overwrite Partition

Spark defaults to `static` partition overwrite. Set
`spark.sql.sources.partitionOverwriteMode=dynamic` to replace only the partitions written by the
input. Each case below resets the table to the same two rows before overwriting.

![Static overwrite replaces the whole table; an explicit partition or dynamic overwrite preserves untouched partitions.](/img/spark-partition-overwrite.svg)

For example:

```sql
CREATE TABLE my_table (id INT, pt STRING) PARTITIONED BY (pt);
INSERT INTO my_table VALUES (1, 'p1'), (2, 'p2');

-- Static overwrite (overwrite the whole table)
SET spark.sql.sources.partitionOverwriteMode=static;
INSERT OVERWRITE my_table VALUES (3, 'p1');
-- or
INSERT OVERWRITE my_table PARTITION (pt) VALUES (3, 'p1');

SELECT * FROM my_table;
/*
+---+---+
| id| pt|
+---+---+
|  3| p1|
+---+---+
*/

-- Restore the initial rows before the next case.
INSERT OVERWRITE my_table VALUES (1, 'p1'), (2, 'p2');

-- Static overwrite with specified partitions (only overwrite pt='p1')
INSERT OVERWRITE my_table PARTITION (pt='p1') VALUES (3);

SELECT * FROM my_table;
/*
+---+---+
| id| pt|
+---+---+
|  2| p2|
|  3| p1|
+---+---+
*/

-- Restore the initial rows while still in static mode.
INSERT OVERWRITE my_table VALUES (1, 'p1'), (2, 'p2');

-- Dynamic overwrite (only overwrite pt='p1')
SET spark.sql.sources.partitionOverwriteMode=dynamic;
INSERT OVERWRITE my_table VALUES (3, 'p1');

SELECT * FROM my_table;
/*
+---+---+
| id| pt|
+---+---+
|  2| p2|
|  3| p1|
+---+---+
*/
```

A Format Table read through Paimon (`format-table.implementation = paimon`, the default) follows
the same rule. An `INSERT OVERWRITE` that names no partition replaces the whole table, so a
partition the query does not write is replaced too, and a query that returns no rows leaves the
table empty; `dynamic` mode replaces only the partitions written, and writing nothing then replaces
nothing. With `metastore.partitioned-table = true` the catalog is the answer to which partitions
the table has, so overwriting the whole table empties those and leaves a directory still waiting
for `MSCK REPAIR TABLE` alone.

## Truncate Table

The `TRUNCATE TABLE` statement removes all the rows from a table or partition(s).

```sql
TRUNCATE TABLE my_table;
TRUNCATE TABLE my_table PARTITION (dt = '2025-01-01');
```

On a Format Table read through Paimon (`format-table.implementation = paimon`, the default),
`TRUNCATE TABLE` deletes the data files of the table or of the named partitions and keeps the
partitions: their directories remain, and with `metastore.partitioned-table = true` so do their
catalog registrations, so `SHOW PARTITIONS` returns what it returned before. That setting also
makes the catalog the answer to which partitions the table has, so truncating empties those, leaves
a directory still waiting for `MSCK REPAIR TABLE` alone, and replaces their statistics with zero. A
spec that names only some of the partition keys empties the partitions it covers; a complete spec
the table does not have is an error.

## Update Table

Updates the column values for the rows that match a predicate. When no predicate is provided, update the column values for all rows.

Note:

:::info

Update primary key columns is not supported when the target table is a primary key table.

:::

Spark supports update PrimitiveType and StructType, for example:

```sql
-- Syntax
UPDATE table_identifier SET column1 = value1, column2 = value2, ... WHERE condition;

CREATE TABLE t (
  id INT,
  s STRUCT<c1: INT, c2: STRING>,
  name STRING)
TBLPROPERTIES (
  'primary-key' = 'id',
  'merge-engine' = 'deduplicate'
);

-- you can use
UPDATE t SET name = 'a_new' WHERE id = 1;
UPDATE t SET s.c2 = 'a_new' WHERE s.c1 = 1;
```

## Delete From Table

Deletes the rows that match a predicate. When no predicate is provided, deletes all rows.

```sql
DELETE FROM my_table WHERE id = 1;
```

## Merge Into Table

Merges a set of updates, insertions and deletions based on a source table into a target table.

:::info

Updating primary key columns is not supported when the target table is a primary key table.

:::

### Syntax

```sql
MERGE INTO target
USING source
ON <merge condition>
WHEN MATCHED [AND <condition>] THEN { UPDATE SET ... | DELETE }
WHEN NOT MATCHED [AND <condition>] THEN INSERT ...
```

Each `WHEN` clause can be repeated; clauses are evaluated in order, and the first matching one wins for a given row.

### Examples

The examples below assume both source and target have schema `(a INT, b INT, c STRING)`, with `a` as the primary key.

Simple upsert — update existing rows, insert new ones:

```sql
MERGE INTO target
USING source
ON target.a = source.a
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

Multiple conditional clauses:

```sql
MERGE INTO target
USING source
ON target.a = source.a
WHEN MATCHED AND target.a = 5 THEN UPDATE SET b = source.b + target.b
WHEN MATCHED AND source.c > 'c2' THEN UPDATE SET *
WHEN MATCHED THEN DELETE
WHEN NOT MATCHED AND c > 'c9' THEN INSERT (a, b, c) VALUES (a, b * 1.1, c)
WHEN NOT MATCHED THEN INSERT *
```

### Column Alignment

Assignments are aligned to the target table by **column name**.

- **Explicit clauses** (`UPDATE SET col = expr` / `INSERT (col list) VALUES ...`) — only the mentioned columns are written. Unmentioned target columns preserve their current value for `UPDATE`, or get NULL / `CURRENT_DEFAULT` for `INSERT`.
- **Star clauses** (`UPDATE SET *` / `INSERT *`) — `*` expands against the **target** columns. When source and target columns don't match exactly, the behavior depends on `spark.paimon.write.merge-schema`; see [Column Alignment by Write Path](./schema-evolution#column-alignment-by-write-path) under Write Merge Schema for the full table covering both `MERGE INTO *` and byName `INSERT` paths.

## Write Merge Schema

<span id="how-it-evolves-the-schema"></span>
<span id="examples-1"></span>
<span id="column-alignment-by-write-path"></span>

See [Schema Evolution on Write](./schema-evolution) for options, examples, and the
[column alignment reference](./schema-evolution#column-alignment-by-write-path).

## COPY INTO

<span id="csv-import"></span>
<span id="json-import"></span>
<span id="parquet-import"></span>
<span id="write-csv-files"></span>
<span id="write-json-files"></span>
<span id="write-parquet-files"></span>
<span id="file_format-options"></span>
<span id="import-options"></span>
<span id="file-write-options"></span>
<span id="column-mapping"></span>
<span id="repeated-imports"></span>
<span id="result-output"></span>
<span id="limitations"></span>

See [COPY INTO](./copy-into) for CSV, JSON, and Parquet import/export syntax, options,
column mapping, load history, and limitations.
