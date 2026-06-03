---
title: "SQL Write"
sidebar_position: 2
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

# SQL Write

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

Use `INSERT OVERWRITE` to overwrite the whole table.

```sql
INSERT OVERWRITE my_table SELECT ...
```

#### Insert Overwrite Partition

Use `INSERT OVERWRITE` to overwrite a partition.

```sql
INSERT OVERWRITE my_table PARTITION (key1 = value1, key2 = value2, ...) SELECT ...
```

#### Dynamic Overwrite Partition

Spark's default overwrite mode is `static` partition overwrite. To enable dynamic overwritten you need to set the Spark session configuration `spark.sql.sources.partitionOverwriteMode` to `dynamic`

For example:

```sql
CREATE TABLE my_table (id INT, pt STRING) PARTITIONED BY (pt);
INSERT INTO my_table VALUES (1, 'p1'), (2, 'p2');

-- Static overwrite (Overwrite the whole table)
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

-- Static overwrite with specified partitions (Only overwrite pt='p1')
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
  
-- Dynamic overwrite (Only overwrite pt='p1')
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

## Truncate Table

The `TRUNCATE TABLE` statement removes all the rows from a table or partition(s).

```sql
TRUNCATE TABLE my_table;
```

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
- **Star clauses** (`UPDATE SET *` / `INSERT *`) — `*` expands against the **target** columns. When source and target columns don't match exactly, the behavior depends on `spark.paimon.write.merge-schema`; see [Column Alignment by Write Path](#column-alignment-by-write-path) under Write Merge Schema for the full table covering both `MERGE INTO *` and byName `INSERT` paths.

## Write Merge Schema

When `write.merge-schema` is enabled, Paimon automatically evolves the table schema during write to accommodate new columns in the incoming data, while preserving data integrity.

:::info

Since the table schema may be updated during writing, catalog caching needs to be disabled to use this feature. Configure `spark.sql.catalog.<catalogName>.cache-enabled` to `false`.

:::

### How It Evolves the Schema

Three options control how aggressively the schema evolves; each only takes effect when the previous one is enabled:

<table class="configuration table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 30%">Option</th>
            <th class="text-left" style="width: 70%">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>write.merge-schema</h5></td>
            <td>If true, evolve the table schema to accept new columns from the incoming data. Existing column types are preserved and incoming values are cast to them; to also widen existing types, enable <code>write.merge-schema.type-widening</code>.</td>
        </tr>
        <tr>
            <td><h5>write.merge-schema.type-widening</h5></td>
            <td>Only effective when <code>write.merge-schema</code> is true. If true, widen an existing column type when the incoming data has a wider compatible type (e.g. INT -> BIGINT, DECIMAL precision increase). Lossy changes are still rejected unless <code>write.merge-schema.explicit-cast</code> is also true.</td>
        </tr>
        <tr>
            <td><h5>write.merge-schema.explicit-cast</h5></td>
            <td>Only effective when <code>write.merge-schema.type-widening</code> is true. If true, also allow lossy type changes between compatible types (e.g. BIGINT -> INT, STRING -> DATE).</td>
        </tr>
    </tbody>
</table>

### Examples

DataFrame batch write:

```scala
data.write
  .format("paimon")
  .mode("append")
  .option("write.merge-schema", "true")
  .saveAsTable("t")
```

Spark SQL (requires Spark 3.5+ for `BY NAME`):

```sql
SET `spark.paimon.write.merge-schema` = true;

CREATE TABLE t (a INT, b STRING);
INSERT INTO t VALUES (1, '1'), (2, '2');

INSERT INTO t BY NAME SELECT 3 AS a, '3' AS b, 3 AS c;
```

Streaming write:

```scala
val inputData = MemoryStream[(Int, String)]
inputData
  .toDS()
  .toDF("col1", "col2")
  .writeStream
  .format("paimon")
  .option("checkpointLocation", "/path/to/checkpoint")
  .option("write.merge-schema", "true")
  .toTable("t")
```

### Column Alignment by Write Path

When the source schema doesn't match the target schema exactly, the behavior depends on both `write.merge-schema` and the write path. For nested struct fields, all byName paths behave the same; at the top level, `MERGE INTO *` differs from regular byName `INSERT` because `*` expansion only references target columns.

| Write path | Scenario | `merge-schema=false` (default) | `merge-schema=true` |
|------------|----------|-------------------------------|---------------------|
| **byName `INSERT`** (`INSERT INTO ... BY NAME` / `saveAsTable` / `writeTo`) | Top-level source-extra columns | Throws | Evolved into the target schema |
| | Top-level target columns missing from source | NULL-filled | NULL-filled |
| | Nested struct source-extra fields | Throws | Evolved into the target schema |
| | Nested struct target-missing fields | Throws | NULL-filled |
| **`MERGE INTO *`** (`UPDATE *` / `INSERT *`) | Top-level source-extra columns | Silently dropped (`*` only covers target columns) | Evolved into the target schema |
| | Top-level target columns missing from source | Throws | `UPDATE *` preserves current value; `INSERT *` fills `CURRENT_DEFAULT` (or NULL when no default) |
| | Nested struct source-extra fields | Throws | Evolved into the target schema |
| | Nested struct target-missing fields | Throws | `UPDATE *` preserves current value; `INSERT *` fills `CURRENT_DEFAULT` (or NULL when no default) |

Notes:
- Position-based writes (e.g. `INSERT INTO t VALUES (...)` without `BY NAME`) require an exact column count match and don't engage schema evolution; only byName writes are covered above.
- Top-level target-missing under `merge-schema=false` for byName `INSERT` mirrors Spark's `INSERT FILL` semantics — only nested missing fields throw.
- Under strict mode (`merge-schema=false`), nested source-extra fields throw to avoid silent data loss; for `MERGE INTO *` at the top level, source-extras are silently dropped because `*` never references them.

## COPY INTO

`COPY INTO` provides a SQL command for bulk loading data files into Paimon tables and exporting table data to files. Supported formats: **CSV**, **JSON**, and **Parquet**.

:::info
**SQL dialect:** Paimon's `COPY INTO` is a Snowflake-style extension (`FILE_FORMAT = (TYPE = ...)`, `PATTERN`, `FORCE`, `ON_ERROR`), not the Databricks `COPY INTO` form (`FILEFORMAT` + `FORMAT_OPTIONS (...)` / `COPY_OPTIONS (...)`). It implements only a subset of the Snowflake syntax. In particular, `ON_ERROR` supports `ABORT_STATEMENT` (default), `CONTINUE`, and `SKIP_FILE`; the Snowflake variants `SKIP_FILE_<num>` and `SKIP_FILE_<num>%` are **not** supported.
:::

#### CSV Import

```sql
COPY INTO table_name [(col1, col2, ...)]
FROM 'source_path'
FILE_FORMAT = (TYPE = CSV [, option = value, ...])
[PATTERN = 'regex']
[FORCE = TRUE|FALSE]
[ON_ERROR = { ABORT_STATEMENT | CONTINUE | SKIP_FILE }]
```

**Basic import:**

```sql
COPY INTO my_db.my_table
FROM '/data/csv_files/'
FILE_FORMAT = (TYPE = CSV);
```

**Import with explicit column mapping:**

```sql
-- Only load into specified columns; omitted columns use their DEFAULT value or NULL
COPY INTO my_db.users (id, name)
FROM '/data/new_users/'
FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1);
```

**Import with NULL_IF and PATTERN:**

```sql
COPY INTO my_db.events
FROM '/data/logs/'
FILE_FORMAT = (TYPE = CSV, FIELD_DELIMITER = '|', NULL_IF = ('NULL', '\\N', ''))
PATTERN = '.*\.csv'
FORCE = FALSE;
```

#### JSON Import

```sql
COPY INTO table_name [(col1, col2, ...)]
FROM 'source_path'
FILE_FORMAT = (TYPE = JSON [, option = value, ...])
[PATTERN = 'regex']
[FORCE = TRUE|FALSE]
[ON_ERROR = { ABORT_STATEMENT | CONTINUE | SKIP_FILE }]
```

**Basic import:**

```sql
COPY INTO my_db.my_table
FROM '/data/json_files/'
FILE_FORMAT = (TYPE = JSON);
```

**Import multi-line JSON array:**

```sql
COPY INTO my_db.events
FROM '/data/events/'
FILE_FORMAT = (TYPE = JSON, MULTI_LINE = TRUE);
```

JSON columns are matched **by column name** (not by position), so source field order does not matter.

#### Parquet Import

```sql
COPY INTO table_name [(col1, col2, ...)]
FROM 'source_path'
FILE_FORMAT = (TYPE = PARQUET [, option = value, ...])
[PATTERN = 'regex']
[FORCE = TRUE|FALSE]
[ON_ERROR = { ABORT_STATEMENT | CONTINUE | SKIP_FILE }]
```

**Basic import:**

```sql
COPY INTO my_db.my_table
FROM '/data/parquet_files/'
FILE_FORMAT = (TYPE = PARQUET);
```

**Import with PATTERN:**

```sql
COPY INTO my_db.events
FROM '/data/lake/'
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*\.parquet'
FORCE = FALSE;
```

Parquet columns are matched **by column name** (not by position). Extra columns in the source files are ignored; missing columns become NULL.

#### Write CSV Files

```sql
COPY INTO 'target_path'
FROM table_name
FILE_FORMAT = (TYPE = CSV [, option = value, ...])
[OVERWRITE = TRUE|FALSE]
```

**Write with header and overwrite:**

```sql
COPY INTO '/export/users_backup/'
FROM my_db.users
FILE_FORMAT = (TYPE = CSV, HEADER = TRUE, FIELD_DELIMITER = ',')
OVERWRITE = TRUE;
```

#### Write JSON Files

```sql
COPY INTO 'target_path'
FROM table_name
FILE_FORMAT = (TYPE = JSON [, option = value, ...])
[OVERWRITE = TRUE|FALSE]
```

**Basic JSON export:**

```sql
COPY INTO '/export/events_backup/'
FROM my_db.events
FILE_FORMAT = (TYPE = JSON)
OVERWRITE = TRUE;
```

#### Write Parquet Files

```sql
COPY INTO 'target_path'
FROM table_name
FILE_FORMAT = (TYPE = PARQUET [, option = value, ...])
[OVERWRITE = TRUE|FALSE]
```

**Basic Parquet export:**

```sql
COPY INTO '/export/data_backup/'
FROM my_db.events
FILE_FORMAT = (TYPE = PARQUET)
OVERWRITE = TRUE;
```

**Export with compression:**

```sql
COPY INTO '/export/data_compressed/'
FROM my_db.events
FILE_FORMAT = (TYPE = PARQUET, COMPRESSION = GZIP)
OVERWRITE = TRUE;
```

#### FILE_FORMAT Options

`FILE_FORMAT` is required and must include `TYPE = CSV`, `TYPE = JSON`, or `TYPE = PARQUET`.

**CSV import options:**

| Option | Description | Default |
|--------|-------------|---------|
| TYPE | File format type. `CSV`, `JSON`, or `PARQUET`. | (required) |
| FIELD_DELIMITER | Column delimiter character. | `,` |
| SKIP_HEADER | Skip the first line as header. Only `0` or `1`. | `0` |
| QUOTE | Quote character for enclosing fields. | `"` |
| ESCAPE | Escape character within quoted fields. | `\` |
| NULL_IF | List of string values to interpret as NULL, e.g. `('NULL', '\\N')`. | (none) |
| EMPTY_FIELD_AS_NULL | Treat empty fields as NULL. `TRUE` or `FALSE`. | `FALSE` |
| COMPRESSION | Compression codec (e.g. `GZIP`). | `NONE` |

**JSON import options:**

| Option | Description | Default |
|--------|-------------|---------|
| TYPE | File format type. `CSV`, `JSON`, or `PARQUET`. | (required) |
| MULTI_LINE | Parse multi-line JSON (e.g. JSON arrays or pretty-printed objects). | `FALSE` |
| NULL_IF | List of string values to interpret as NULL. | (none) |
| EMPTY_FIELD_AS_NULL | Treat empty string values as NULL. | `FALSE` |
| COMPRESSION | Compression codec (e.g. `GZIP`). | `NONE` |

**Parquet import options:**

| Option | Description | Default |
|--------|-------------|---------|
| TYPE | File format type. `CSV`, `JSON`, or `PARQUET`. | (required) |
| COMPRESSION | Compression codec. Usually auto-detected; rarely needed for import. | (auto) |

**CSV write options:**

| Option | Description | Default |
|--------|-------------|---------|
| TYPE | File format type. `CSV`, `JSON`, or `PARQUET`. | (required) |
| FIELD_DELIMITER | Column delimiter character. | `,` |
| HEADER | Write column names as the first line. `TRUE` or `FALSE`. | `FALSE` |
| QUOTE | Quote character for enclosing fields. | `"` |
| ESCAPE | Escape character within quoted fields. | `\` |
| COMPRESSION | Compression codec (e.g. `GZIP`). | `NONE` |

**JSON write options:**

| Option | Description | Default |
|--------|-------------|---------|
| TYPE | File format type. `CSV`, `JSON`, or `PARQUET`. | (required) |
| DATE_FORMAT | Custom date format pattern. | Spark default |
| TIMESTAMP_FORMAT | Custom timestamp format pattern. | Spark default |
| COMPRESSION | Compression codec (e.g. `GZIP`). | `NONE` |

**Parquet write options:**

| Option | Description | Default |
|--------|-------------|---------|
| TYPE | File format type. `CSV`, `JSON`, or `PARQUET`. | (required) |
| COMPRESSION | Compression codec (`SNAPPY`, `GZIP`, `NONE`, etc.). | `SNAPPY` |

#### Import Options

| Option | Description | Default |
|--------|-------------|---------|
| PATTERN | Regex to filter source files by base file name. Only matching files are loaded. | (all files) |
| FORCE | `FALSE`: skip files already loaded (idempotent). `TRUE`: reload all files. | `FALSE` |
| ON_ERROR | Error handling strategy. `ABORT_STATEMENT`: abort on any error. `CONTINUE`: skip bad rows and continue loading. `SKIP_FILE`: skip files that contain errors. | `ABORT_STATEMENT` |

#### File Write Options

| Option | Description | Default |
|--------|-------------|---------|
| OVERWRITE | `FALSE`: fail if target path exists. `TRUE`: overwrite existing files. | `FALSE` |

#### Column Mapping

When an explicit column list is provided (e.g., `COPY INTO t (col1, col2) FROM ...`):

- **CSV**: Columns are mapped **positionally** to the specified column list.
- **JSON**: Columns are matched **by name** to the specified column list.
- **Parquet**: Columns are matched **by name** to the specified column list.
- The number of source columns must match the column list length (CSV). For JSON and Parquet, missing fields in the source become NULL.
- Columns not in the list are filled with their **DEFAULT value** (if defined in the table schema) or **NULL**.
- Non-nullable columns without a default value that are not in the list will cause an error.

When no column list is provided:

- **CSV**: Columns are mapped positionally to all writable columns in the target table. The number of CSV columns must match the number of writable columns.
- **JSON**: Columns are matched by name to the writable columns. Missing fields in JSON become NULL.

#### Repeated Imports

By default (`FORCE = FALSE`), COPY INTO tracks which files have been successfully loaded. A file is identified by its path, size, and last-modified timestamp.

- Re-running the same COPY INTO command will **skip** already-loaded files and return status `SKIPPED`.
- If a source file is modified (size or timestamp changes), it becomes eligible for re-loading.
- `FORCE = TRUE` bypasses load history and always re-imports all matching files.

#### Result Output

**Import** returns one row per source file:

| Column | Type | Description |
|--------|------|-------------|
| file_name | STRING | Source file name |
| status | STRING | `LOADED`, `PARTIALLY_LOADED`, `LOAD_FAILED`, or `SKIPPED` |
| rows_loaded | BIGINT | Number of rows written |
| rows_parsed | BIGINT | Number of rows parsed from the file |
| errors_seen | BIGINT | Number of error rows (parse or cast failures) |
| first_error | STRING | First error message encountered (NULL if no errors) |

**File write** returns a single row:

| Column | Type | Description |
|--------|------|-------------|
| output_path | STRING | Target output path |
| file_count | INT | Number of files written |
| rows_written | BIGINT | Total rows written |

#### Limitations

- **CSV column-count mismatch**: Rows with fewer or more columns than the target schema are treated as malformed records. With `ON_ERROR = CONTINUE`, these rows are skipped and counted as errors.
- Only **CSV**, **JSON**, and **Parquet** formats are supported.
- Writing files only supports `FROM table_name`; `FROM (SELECT ...)` is not supported.
- `SINGLE = TRUE` (single-file output) is not supported.
- File format options must be specified inline in `FILE_FORMAT = (...)`.
- File listing is **non-recursive**: only direct files under the source path are processed. Subdirectories are ignored.
- `PATTERN` matches the **base file name** only (not the full path).
- Concurrent COPY INTO commands targeting the same table may produce duplicate data.
- `SKIP_HEADER` only supports values `0` or `1`.
