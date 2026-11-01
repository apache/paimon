---
title: "System Tables"
sidebar_position: 6
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

# System Tables

PyPaimon exposes `table$<name>` system tables through the existing
catalog and read-builder APIs. The supported short names are:
`snapshots`, `schemas`, `options`, `manifests`, `files`, `partitions`,
`tags`, and `branches`. Global tables under the `sys` database
(`sys.all_tables`, `sys.catalog_options`, ...) and the streaming
`audit_log` / `binlog` family are not exposed yet.

## Basic Usage

Reuse a single read builder for both the scan and the read so that any
filter, projection, or limit set on it is honoured by both sides:

```python
from pypaimon import CatalogFactory

catalog = CatalogFactory.create({'warehouse': '/path/to/warehouse'})
snapshots = catalog.get_table('default.my_table$snapshots')

read_builder = snapshots.new_read_builder()
splits = read_builder.new_scan().plan().splits()
print(read_builder.new_read().to_pandas(splits))
```

`with_projection`, `with_filter`, and `with_limit` chain on the same builder.
The filter runs on the in-memory system table before projection and limit:

```python
read_builder = snapshots.new_read_builder()
predicate = read_builder.new_predicate_builder().startswith('commit_user', 'job-')
read_builder = (
    read_builder
    .with_filter(predicate)
    .with_projection(['snapshot_id', 'commit_user', 'commit_time'])
    .with_limit(10)
)
splits = read_builder.new_scan().plan().splits()
arrow_table = read_builder.new_read().to_arrow(splits)
```

Supported filters are equality and comparison (`equal`, `not_equal`,
`less_than`, `less_or_equal`, `greater_than`, `greater_or_equal`,
`between`, `not_between`), `is_in` / `is_not_in`, `is_null` /
`is_not_null`, and `and_predicates` / `or_predicates`. On `STRING`
columns, `startswith`, `endswith`, `contains`, and SQL `like` (`%`,
`_`, and `\` escapes) are applied by an exact row match. `like` treats
`_` as one character, including a newline.

The returned object exposes the regular `Table` surface, so the same
read builder works with `to_pandas`, `to_arrow`, `to_iterator`,
`to_record_batch_iterator`, and `to_duckdb`. Writes raise
`NotImplementedError` — system tables are read-only.

## Available Tables

Each system table is listed below with its column layout (including
nullability) and primary-key choice. Tables are listed in the order
they appear in `SystemTableLoader`.

### `$snapshots`

One row per persisted snapshot.

| Column                    | Type            | Notes                          |
|---------------------------|-----------------|--------------------------------|
| `snapshot_id`             | BIGINT NOT NULL | Primary key                    |
| `schema_id`               | BIGINT NOT NULL |                                |
| `commit_user`             | STRING NOT NULL |                                |
| `commit_identifier`       | BIGINT NOT NULL |                                |
| `commit_kind`             | STRING NOT NULL | `APPEND`, `COMPACT`, ...       |
| `commit_time`             | TIMESTAMP(3) NOT NULL |                          |
| `base_manifest_list`      | STRING NOT NULL |                                |
| `delta_manifest_list`     | STRING NOT NULL |                                |
| `changelog_manifest_list` | STRING          |                                |
| `total_record_count`      | BIGINT          | Unmerged record count of all live data files; not a logical row count |
| `delta_record_count`      | BIGINT          | Net change of the unmerged record count from added and deleted data files |
| `changelog_record_count`  | BIGINT          |                                |
| `watermark`               | BIGINT          |                                |
| `next_row_id`             | BIGINT          |                                |

Dedicated Format columns are stored in separate data files, and each of those files contributes to
the snapshot record counts. For example, appending `N` logical rows to a table with one dedicated
BLOB column increases `delta_record_count` by `2 * N`. Use `COUNT(*)` when you need the logical row
count.

### `$schemas`

Every committed schema version, with `fields` / `partition_keys` /
`primary_keys` / `options` encoded as compact JSON strings.

| Column           | Type            | Notes        |
|------------------|-----------------|--------------|
| `schema_id`      | BIGINT NOT NULL | Primary key  |
| `fields`         | STRING NOT NULL | JSON         |
| `partition_keys` | STRING NOT NULL | JSON list    |
| `primary_keys`   | STRING NOT NULL | JSON list    |
| `options`        | STRING NOT NULL | JSON map     |
| `comment`        | STRING          |              |
| `update_time`    | TIMESTAMP(3) NOT NULL |        |

### `$options`

Two columns echoing the active table options.

| Column  | Type            | Notes        |
|---------|-----------------|--------------|
| `key`   | STRING NOT NULL | Primary key  |
| `value` | STRING NOT NULL |              |

### `$manifests`

Manifest list for the latest snapshot.

| Column                | Type            | Notes                         |
|-----------------------|-----------------|-------------------------------|
| `file_name`           | STRING NOT NULL | Primary key                   |
| `file_size`           | BIGINT NOT NULL |                               |
| `num_added_files`     | BIGINT NOT NULL |                               |
| `num_deleted_files`   | BIGINT NOT NULL |                               |
| `schema_id`           | BIGINT NOT NULL |                               |
| `min_partition_stats` | STRING          | `{v1, v2}`; `NULL` for some partition types (see Limitations) |
| `max_partition_stats` | STRING          | `{v1, v2}`; `NULL` for some partition types (see Limitations) |
| `min_row_id`          | BIGINT          |                               |
| `max_row_id`          | BIGINT          |                               |

### `$files`

One row per ADD entry surviving the latest snapshot. Stats columns are
compact JSON dictionaries keyed by column name. The wire name
`deleteRowCount` is intentionally camelCase.

| Column                  | Type                | Notes                       |
|-------------------------|---------------------|-----------------------------|
| `partition`             | STRING              | `pt=v/pt2=v2`               |
| `bucket`                | INT NOT NULL        |                             |
| `file_path`             | STRING NOT NULL     | Primary key                 |
| `file_format`           | STRING NOT NULL     |                             |
| `schema_id`             | BIGINT NOT NULL     |                             |
| `level`                 | INT NOT NULL        |                             |
| `record_count`          | BIGINT NOT NULL     |                             |
| `file_size_in_bytes`    | BIGINT NOT NULL     |                             |
| `min_key`               | STRING              | JSON list (PK tables only)  |
| `max_key`               | STRING              | JSON list (PK tables only)  |
| `null_value_counts`     | STRING NOT NULL     | JSON map                    |
| `min_value_stats`       | STRING NOT NULL     | JSON map                    |
| `max_value_stats`       | STRING NOT NULL     | JSON map                    |
| `min_sequence_number`   | BIGINT              |                             |
| `max_sequence_number`   | BIGINT              |                             |
| `creation_time`         | TIMESTAMP(3)        |                             |
| `deleteRowCount`        | BIGINT              | camelCase wire name         |
| `file_source`           | STRING              |                             |
| `first_row_id`          | BIGINT              |                             |
| `write_cols`            | ARRAY<STRING>       |                             |

### `$partitions`

Aggregated partition statistics for the latest snapshot.

| Column                | Type                  | Notes                          |
|-----------------------|-----------------------|--------------------------------|
| `partition`           | STRING                | `pt=v/pt2=v2`; primary key     |
| `record_count`        | BIGINT NOT NULL       |                                |
| `file_size_in_bytes`  | BIGINT NOT NULL       |                                |
| `file_count`          | BIGINT NOT NULL       |                                |
| `last_update_time`    | TIMESTAMP(3)          |                                |
| `created_at`          | TIMESTAMP(3)          | Filesystem path returns `NULL` |
| `created_by`          | STRING                | Filesystem path returns `NULL` |
| `updated_by`          | STRING                | Filesystem path returns `NULL` |
| `options`             | STRING                | Filesystem path returns `NULL` |
| `total_buckets`       | INT NOT NULL          |                                |
| `done`                | BOOLEAN NOT NULL      | Filesystem path returns `False`|
| `deleted_record_count` | BIGINT               | Records marked deleted by deletion vectors; `0` when none exist and `NULL` when legacy metadata lacks cardinality |

### `$tags`

Snapshot metadata for every tag.

| Column          | Type                  | Notes                          |
|-----------------|-----------------------|--------------------------------|
| `tag_name`      | STRING NOT NULL       | Primary key                    |
| `snapshot_id`   | BIGINT NOT NULL       |                                |
| `schema_id`     | BIGINT NOT NULL       |                                |
| `commit_time`   | TIMESTAMP(3) NOT NULL |                                |
| `record_count`  | BIGINT                |                                |
| `create_time`   | TIMESTAMP(3)          | `NULL` when the tag has no retention metadata |
| `time_retained` | STRING                | ISO-8601 duration, such as `PT24H`; `NULL` when unset |

### `$branches`

Every named branch with the branch directory's modification time.

| Column        | Type                  | Notes        |
|---------------|-----------------------|--------------|
| `branch_name` | STRING NOT NULL       | Primary key  |
| `create_time` | TIMESTAMP(3) NOT NULL |              |

## Limitations

* **Value comparisons on nested system-table columns are rejected.**
  Columns such as `$files.write_cols` (`ARRAY`) have no PyArrow
  comparison kernel. `with_filter` raises `NotImplementedError` for
  equality, comparison, `in`, and string match on those columns.
  `is_null` and `is_not_null` still apply. String match methods on a
  non-string column also raise `NotImplementedError`.
* **`min_partition_stats` / `max_partition_stats` in `$manifests`**
  use the same `{v1, v2}` text as Java. An unpartitioned table renders
  `{}`. The whole value stays `NULL` when any partition field is
  `FLOAT`, `DOUBLE`, `TIMESTAMP_LTZ`, `BINARY`, `VARBINARY`, or
  `BYTES`, because those casts are not stable across JVM versions and
  Python.
* **`$tags.create_time` and `$tags.time_retained` are `NULL` when the
  tag was created without retention.** A tag created with
  `time_retained` fills `create_time` and stores the duration as Java
  `Duration` text (`1d` becomes `PT24H`).
* **`branch.create_time` falls back to epoch 0** when the underlying
  store cannot provide an mtime (some remote object stores via
  `PyArrowFileIO`). Local filesystem catalogs always populate the
  real time.
* **`partitions.created_at / created_by / updated_by / options / done`**
  are filled with placeholders for the filesystem path. REST-managed
  catalogs that expose those fields will be wired in a follow-up.
* **`list_tables` does not enumerate system tables.** System tables
  remain accessible through `get_table('db.t$name')`.

## Supported via Catalogs

* `FilesystemCatalog` — fully supported.
* `RESTCatalog` — fully supported; columns that depend on catalog
  metadata (such as `$partitions.created_by`) are populated via the
  REST API where the server exposes them.
