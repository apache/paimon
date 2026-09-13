---
title: "CLI: Query and Inspect"
sidebar_label: "Query and Inspect"
description: "Inspect schemas, snapshots, partitions, scan plans, and table rows from the terminal."
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

# CLI: Query and Inspect

Inspect schemas, snapshots, partitions, scan plans, and table rows from the terminal. Configure a catalog with the [CLI quick start](./cli#basic-usage) first. The examples use existing tables and illustrative output; replace table and column names with your own.

## Table Read

Read data from a Paimon table and display it in a tabular format.

```shell
paimon table read mydb.users
```

**Options:**

- `--select, -s`: Select specific columns to read (comma-separated)
- `--where, -w`: Filter condition in SQL-like syntax
- `--limit, -l`: Maximum number of results to display (default: 100)
- `--format, -f`: Output format: `table` (default) or `json`

**Examples:**

```shell
# Read with limit
paimon table read mydb.users -l 50

# Read specific columns
paimon table read mydb.users -s id,name,age

# Filter with WHERE clause
paimon table read mydb.users --where "age > 18"

# Combine select, where, and limit
paimon table read mydb.users -s id,name -w "age >= 20 AND city = 'Beijing'" -l 50

# Output as JSON (for programmatic use)
paimon table read mydb.users --format json
```

**WHERE Operators**

The `--where` option supports SQL-like filter expressions:

| Operator | Example |
|---|---|
| `=`, `!=`, `<>` | `name = 'Alice'` |
| `<`, `<=`, `>`, `>=` | `age > 18` |
| `IS NULL`, `IS NOT NULL` | `deleted_at IS NULL` |
| `IN (...)`, `NOT IN (...)` | `status IN ('active', 'pending')` |
| `BETWEEN ... AND ...` | `age BETWEEN 20 AND 30` |
| `LIKE` | `name LIKE 'A%'` |

Multiple conditions can be combined with `AND` and `OR` (AND has higher precedence). Parentheses are supported for grouping:

```shell
# AND condition
paimon table read mydb.users -w "age >= 20 AND age <= 30"

# OR condition
paimon table read mydb.users -w "city = 'Beijing' OR city = 'Shanghai'"

# Parenthesized grouping
paimon table read mydb.users -w "(age > 18 OR name = 'Bob') AND city = 'Beijing'"

# IN list
paimon table read mydb.users -w "city IN ('Beijing', 'Shanghai', 'Hangzhou')"

# BETWEEN
paimon table read mydb.users -w "age BETWEEN 25 AND 35"

# LIKE pattern
paimon table read mydb.users -w "name LIKE 'A%'"

# IS NULL / IS NOT NULL
paimon table read mydb.users -w "email IS NOT NULL"
```

Literal values are automatically cast to the appropriate Python type based on the table schema (e.g., `INT` fields cast to `int`, `DOUBLE` to `float`).

Output:
```
 id    name  age      city
  1   Alice   25   Beijing
  2     Bob   30  Shanghai
  3 Charlie   35 Guangzhou
  4   David   28  Shenzhen
  5     Eve   32  Hangzhou
```

## Table Explain

Show the scan plan of a query without reading any data: the target snapshot, the pushed-down predicate / projection / limit, the partition / bucket / file-stats pruning funnel, and split-level signals (raw-convertible ratio, deletion-vector ratio, level histogram, files-per-split and split-size distribution). Useful for previewing the pruning effect of a predicate before actually running the read.

```shell
paimon table explain mydb.events
```

**Options:**

- `--select, -s`: Project specific columns (comma-separated)
- `--where, -w`: Filter condition in SQL-like syntax (same operators as `table read`)
- `--limit, -l`: Row limit to push down
- `--verbose, -v`: List every split with its files
- `--format, -f`: Output format: `table` (default) or `json`

**Examples:**

```shell
# Whole-table scan plan
paimon table explain mydb.events

# Push filter and projection through the planner
paimon table explain mydb.events --where "dt = '2026-05-16' AND id = 7" -s dt,id,val

# List every split (and its files) instead of just the aggregates
paimon table explain mydb.events -w "dt = '2026-05-16'" --verbose

# Machine-readable output for scripting (level_histogram keys are JSON strings)
paimon table explain mydb.events --format json
```

Output:
```
== PyPaimon Scan Plan ==
Table:              mydb.events (PK, HASH_FIXED)
Snapshot:           5  (schema 0)
Predicate:          (dt = '2026-05-16') AND (id = 7)
Projection:         [dt, id, val]
Limit:              <none>

Partition pruning:  20 -> 4  (pruned 16)
Bucket pruning:     4 -> 1  (pruned 3)
File skipping:      1 -> 1  (pruned 0)

Splits:             1
  raw-convertible:  1 / 1
  with DV:          0 / 1
  all-above-L0:     0 / 1
  files/split:      min=1  max=1  avg=1.00
  size/split:       min=2.6 KiB  p50=2.6 KiB  p95=2.6 KiB  max=2.6 KiB

Files:              1
Total size:         2.6 KiB
Estimated rows:     10   (merged: 10)
Level histogram:    L0=1
Deletion files:     0
```

`explain` reads the manifest list and manifest files but never opens any data files, so it is dramatically cheaper than a real read on large tables.

## Table Get

Get and display table schema information in JSON format. The output format is the same as the schema JSON format used
in table create, making it easy to export and reuse table schemas.

```shell
paimon table get mydb.users
```

Output:
```json
{
  "fields": [
    {"id": 0, "name": "user_id", "type": "BIGINT"},
    {"id": 1, "name": "username", "type": "STRING"},
    {"id": 2, "name": "email", "type": "STRING"},
    {"id": 3, "name": "age", "type": "INT"},
    {"id": 4, "name": "city", "type": "STRING"},
    {"id": 5, "name": "created_at", "type": "TIMESTAMP"},
    {"id": 6, "name": "is_active", "type": "BOOLEAN"}
  ],
  "partitionKeys": ["city"],
  "primaryKeys": ["user_id"],
  "options": {
    "bucket": "4",
    "changelog-producer": "input"
  },
  "comment": "User information table"
}
```

**Note:** The output JSON can be saved to a file and used directly with the `table create` command to recreate the table structure.

## Table Snapshot

Get and display the latest snapshot information of a Paimon table in JSON format. The snapshot contains metadata about the current state of the table.

```shell
paimon table snapshot mydb.users
```

Output:
```json
{
  "version": 3,
  "id": 5,
  "schemaId": 1,
  "baseManifestList": "manifest-list-5-base-...",
  "deltaManifestList": "manifest-list-5-delta-...",
  "changelogManifestList": null,
  "totalRecordCount": 1000,
  "deltaRecordCount": 100,
  "changelogRecordCount": null,
  "commitUser": "user-123",
  "commitIdentifier": 1709123456789,
  "commitKind": "APPEND",
  "timeMillis": 1709123456789,
  "watermark": null,
  "statistics": null,
  "nextRowId": null
}
```

## Table List Partitions

List partitions of a Paimon table. Supports optional pattern filtering to match specific partitions.

```shell
paimon table list-partitions mydb.orders
```

**Options:**

- `--pattern, -p`: Partition name pattern to filter partitions
- `--format, -f`: Output format: `table` (default) or `json`

**Examples:**

```shell
# List all partitions
paimon table list-partitions mydb.orders

# List partitions matching a pattern
paimon table list-partitions mydb.orders --pattern "dt=2024*"

# Output as JSON (for programmatic use)
paimon table list-partitions mydb.orders --format json
```

Output:
```
              Partition  RecordCount  FileSizeInBytes  FileCount  LastFileCreationTime       UpdatedAt  UpdatedBy
dt=2024-01-01,region=us          500          1048576         10         1704067200000  1704153600000      admin
dt=2024-01-02,region=eu          300           524288          5         1704153600000  1704240000000      user1
dt=2024-01-03,region=us          200           262144          3         1704240000000  1704326400000      admin
```

## Table Full-Text Search

Perform full-text search on a Paimon table with a native full-text index and display matching rows.

```shell
paimon table full-text-search mydb.articles --column content --query "paimon lake"
```

**Options:**

- `--column`: Text column to search on - **Required**
- `--query, -q`: Query text to search for - **Required**
- `--limit, -l`: Maximum number of results to return (default: 10)
- `--select, -s`: Select specific columns to display (comma-separated)
- `--format, -f`: Output format: `table` (default) or `json`

**Examples:**

```shell
# Basic full-text search
paimon table full-text-search mydb.articles --column content -q "paimon lake"

# Search with limit
paimon table full-text-search mydb.articles --column content -q "streaming data" -l 20

# Search with column projection
paimon table full-text-search mydb.articles --column content -q "paimon" -s "id,title,content"

# Output as JSON
paimon table full-text-search mydb.articles --column content -q "paimon" -f json
```

Output:
```
 id                                            content
  0  Apache Paimon is a streaming data lake platform
  2  Paimon supports real-time data ingestion and...
  4  Data lake platforms like Paimon handle large-...
```

**Note:** The table must have a full-text index built on the target column. PyPaimon uses
the tokenizer settings stored in the index metadata and requires the full-text extra for
full-text reads:

```shell
pip install 'pypaimon[full-text]'
```

See
[Global Index](../multimodal-table/global-index) for how to create full-text indexes.
