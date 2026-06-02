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
projection or limit set on it is honoured by both sides:

```python
from pypaimon import CatalogFactory

catalog = CatalogFactory.create({'warehouse': '/path/to/warehouse'})
snapshots = catalog.get_table('default.my_table$snapshots')

read_builder = snapshots.new_read_builder()
splits = read_builder.new_scan().plan().splits()
print(read_builder.new_read().to_pandas(splits))
```

`with_projection` and `with_limit` chain on the same builder:

```python
read_builder = (
    snapshots.new_read_builder()
    .with_projection(['snapshot_id', 'commit_user', 'commit_time'])
    .with_limit(10)
)
splits = read_builder.new_scan().plan().splits()
arrow_table = read_builder.new_read().to_arrow(splits)
```

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
| `total_record_count`      | BIGINT          |                                |
| `delta_record_count`      | BIGINT          |                                |
| `changelog_record_count`  | BIGINT          |                                |
| `watermark`               | BIGINT          |                                |
| `next_row_id`             | BIGINT          |                                |

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
| `min_partition_stats` | STRING          | Placeholder (see Limitations) |
| `max_partition_stats` | STRING          | Placeholder (see Limitations) |
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

### `$tags`

Snapshot metadata for every tag.

| Column          | Type                  | Notes                          |
|-----------------|-----------------------|--------------------------------|
| `tag_name`      | STRING NOT NULL       | Primary key                    |
| `snapshot_id`   | BIGINT NOT NULL       |                                |
| `schema_id`     | BIGINT NOT NULL       |                                |
| `commit_time`   | TIMESTAMP(3) NOT NULL |                                |
| `record_count`  | BIGINT                |                                |
| `create_time`   | TIMESTAMP(3)          | Currently emitted as `NULL`    |
| `time_retained` | STRING                | Currently emitted as `NULL`    |

### `$branches`

Every named branch with the branch directory's modification time.

| Column        | Type                  | Notes        |
|---------------|-----------------------|--------------|
| `branch_name` | STRING NOT NULL       | Primary key  |
| `create_time` | TIMESTAMP(3) NOT NULL |              |

## Limitations

* **Predicate pushdown is not yet implemented.** Calling
  `with_filter(...)` is accepted, but invoking `new_read()` later will
  raise `NotImplementedError` rather than silently dropping the
  predicate. Filter the resulting Arrow table / DataFrame on the
  client side instead.
* **`min_partition_stats` / `max_partition_stats` in `$manifests`**
  are emitted as `NULL`. PyPaimon does not yet ship a helper that casts
  a partition row to its string form.
* **`tag.time_retained` and `tag.create_time` are `NULL`.** PyPaimon's
  `Tag` dataclass does not yet carry these fields — matching
  `FileSystemCatalog.get_tag`'s current behaviour.
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
