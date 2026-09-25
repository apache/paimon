---
title: "COPY INTO"
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

# COPY INTO

`COPY INTO` provides a SQL command for bulk loading data files into Paimon tables and exporting table data to files. Supported formats: **CSV**, **JSON**, and **Parquet**.

:::info
**SQL dialect:** Paimon's `COPY INTO` is a Snowflake-style extension (`FILE_FORMAT = (TYPE = ...)`, `PATTERN`, `FORCE`, `ON_ERROR`), not the Databricks `COPY INTO` form (`FILEFORMAT` + `FORMAT_OPTIONS (...)` / `COPY_OPTIONS (...)`). It implements only a subset of the Snowflake syntax. In particular, `ON_ERROR` supports `ABORT_STATEMENT` (default), `CONTINUE`, and `SKIP_FILE`; the Snowflake variants `SKIP_FILE_<num>` and `SKIP_FILE_<num>%` are **not** supported.
:::

## CSV Import

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

## JSON Import

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

## Parquet Import

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

## Write CSV Files

```sql
COPY INTO 'target_path'
FROM { table_name | (SELECT ...) }
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

**Write from query:**

```sql
COPY INTO '/export/active_users/'
FROM (SELECT id, name FROM my_db.users WHERE active = TRUE)
FILE_FORMAT = (TYPE = CSV, HEADER = TRUE);
```

## Write JSON Files

```sql
COPY INTO 'target_path'
FROM { table_name | (SELECT ...) }
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

**JSON export from query:**

```sql
COPY INTO '/export/recent_events/'
FROM (SELECT * FROM my_db.events WHERE event_date > '2024-01-01')
FILE_FORMAT = (TYPE = JSON);
```

## Write Parquet Files

```sql
COPY INTO 'target_path'
FROM { table_name | (SELECT ...) }
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

**Parquet export from aggregation query:**

```sql
COPY INTO '/export/summary/'
FROM (SELECT dept, COUNT(*) AS cnt FROM my_db.employees GROUP BY dept)
FILE_FORMAT = (TYPE = PARQUET);
```

## FILE_FORMAT Options

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

## Import Options

| Option | Description | Default |
|--------|-------------|---------|
| PATTERN | Regex to filter source files by base file name. Only matching files are loaded. | (all files) |
| FORCE | `FALSE`: skip files already loaded (idempotent). `TRUE`: reload all files. | `FALSE` |
| ON_ERROR | Error handling strategy. `ABORT_STATEMENT`: abort on any error. `CONTINUE`: skip bad rows and continue loading. `SKIP_FILE`: skip files that contain errors. | `ABORT_STATEMENT` |

## File Write Options

| Option | Description | Default |
|--------|-------------|---------|
| OVERWRITE | `FALSE`: fail if target path exists. `TRUE`: overwrite existing files. | `FALSE` |

## Column Mapping

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

## Repeated Imports

By default (`FORCE = FALSE`), COPY INTO tracks which files have been successfully loaded. A file is identified by its path, size, and last-modified timestamp.

- Re-running the same COPY INTO command will **skip** already-loaded files and return status `SKIPPED`.
- If a source file is modified (size or timestamp changes), it becomes eligible for re-loading.
- `FORCE = TRUE` bypasses load history and always re-imports all matching files.

## Result Output

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

## Limitations

- **CSV column-count mismatch**: Rows with fewer or more columns than the target schema are treated as malformed records. With `ON_ERROR = CONTINUE`, these rows are skipped and counted as errors.
- Only **CSV**, **JSON**, and **Parquet** formats are supported.
- `SINGLE = TRUE` (single-file output) is not supported.
- File format options must be specified inline in `FILE_FORMAT = (...)`.
- File listing is **non-recursive**: only direct files under the source path are processed. Subdirectories are ignored.
- `PATTERN` matches the **base file name** only (not the full path).
- Concurrent COPY INTO commands targeting the same table may produce duplicate data.
- `SKIP_HEADER` only supports values `0` or `1`.
- `FROM (...)` accepts any read-only query (e.g. `SELECT`, `WITH ... SELECT`, `VALUES`); statements with side effects (e.g. `INSERT`, `INSERT OVERWRITE DIRECTORY`, DDL) are rejected.
- For a `FROM (...)` export, `rows_written` is an execution-time statistic counted by a separate pass before the files are written. Because the DataFrame is lazy and not cached, writing re-executes the query a second time; if the query is non-deterministic (e.g. uses `rand()`, `current_timestamp()`, or reads a volatile source), the two runs can produce different rows, so `rows_written` may not match the actual file contents. The result is intentionally not staged, so the export does not consume extra executor disk.

For table-to-table row writes, see [SQL Writes](./sql-write). For copying existing Paimon
table files, see the [copy procedure](./procedures/migration#copy).
