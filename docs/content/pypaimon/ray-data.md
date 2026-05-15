---
title: "Ray Data"
weight: 3
type: docs
aliases:
  - /pypaimon/ray-data.html
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

# Ray Data

This requires `ray` to be installed.

`pypaimon.ray` exposes a top-level `read_paimon` / `write_paimon` facade that
takes a table identifier and catalog options directly, mirroring the shape of
Ray's built-in Iceberg integration. The lower-level `TableRead.to_ray()` and
`TableWrite.write_ray()` entry points remain available for callers that have
already resolved a `(read_builder, splits)` pair or constructed a
`table_write` via the regular pypaimon API.

## Read

### `read_paimon` (recommended)

```python
from pypaimon.ray import read_paimon

ray_dataset = read_paimon(
    "database_name.table_name",
    catalog_options={"warehouse": "/path/to/warehouse"},
)

print(ray_dataset)
# MaterializedDataset(num_blocks=1, num_rows=9, schema={f0: int32, f1: string})

print(ray_dataset.take(3))
# [{'f0': 1, 'f1': 'a'}, {'f0': 2, 'f1': 'b'}, {'f0': 3, 'f1': 'c'}]

print(ray_dataset.to_pandas())
#    f0 f1
# 0   1  a
# 1   2  b
# 2   3  c
# 3   4  d
# ...
```

`read_paimon` opens its own catalog and resolves the table, so it is the
single-call equivalent of the four-step `CatalogFactory.create → get_table →
new_read_builder → to_ray` boilerplate.

**Projection and limit:**

```python
ray_dataset = read_paimon(
    "database_name.table_name",
    catalog_options={"warehouse": "/path/to/warehouse"},
    projection=["id", "score"],
    limit=1000,
)
```

**Distribution / scheduling:**

```python
ray_dataset = read_paimon(
    "database_name.table_name",
    catalog_options={"warehouse": "/path/to/warehouse"},
    override_num_blocks=4,
    ray_remote_args={"num_cpus": 2, "max_retries": 3},
    concurrency=8,
)
```

**Time travel:**

```python
# Read a specific snapshot.
ray_dataset = read_paimon(
    "database_name.table_name",
    catalog_options={"warehouse": "/path/to/warehouse"},
    snapshot_id=42,
)

# Read a tagged snapshot.
ray_dataset = read_paimon(
    "database_name.table_name",
    catalog_options={"warehouse": "/path/to/warehouse"},
    tag_name="release-2026-04",
)
```

`snapshot_id` and `tag_name` are mutually exclusive.

**Parameters:**
- `table_identifier`: full table name, e.g. `"db_name.table_name"`.
- `catalog_options`: kwargs forwarded to `CatalogFactory.create()`,
  e.g. `{"warehouse": "/path/to/warehouse"}`.
- `filter`: optional `Predicate` to push down into the scan.
- `projection`: optional list of column names to read.
- `limit`: optional row limit applied at scan planning time.
- `snapshot_id`: optional snapshot id to time-travel to. Mutually
  exclusive with `tag_name`.
- `tag_name`: optional tag name to time-travel to. Mutually
  exclusive with `snapshot_id`.
- `override_num_blocks`: optional override for the number of output blocks.
  Must be `>= 1`.
- `ray_remote_args`: optional kwargs passed to `ray.remote()` in read tasks
  (e.g. `{"num_cpus": 2, "max_retries": 3}`).
- `concurrency`: optional max number of Ray tasks to run concurrently.
- `**read_args`: additional kwargs forwarded to `ray.data.read_datasource`
  (e.g. `per_task_row_limit` in Ray 2.52.0+).

### `TableRead.to_ray()` (lower-level)

If you already have a `read_builder` and `splits`, you can convert them to a
Ray Dataset directly:

```python
table_read = read_builder.new_read()
splits = read_builder.new_scan().plan().splits()
ray_dataset = table_read.to_ray(
    splits,
    override_num_blocks=4,
    ray_remote_args={"num_cpus": 2, "max_retries": 3},
)
```

`to_ray()` accepts the same `override_num_blocks`, `ray_remote_args`,
`concurrency`, and `**read_args` parameters as `read_paimon`.

### Ray Block Size Configuration

If you need to configure Ray's block size (e.g., when Paimon splits exceed
Ray's default 128MB block size), set it on the `DataContext` before calling
either `read_paimon` or `to_ray`:

```python
from ray.data import DataContext

ctx = DataContext.get_current()
ctx.target_max_block_size = 256 * 1024 * 1024  # 256MB (default is 128MB)
```

See the [Ray Data API documentation](https://docs.ray.io/en/latest/data/api/doc/ray.data.read_datasource.html)
for more details.

## Write

### `write_paimon` (recommended)

```python
import ray
from pypaimon.ray import write_paimon

ray_dataset = ray.data.read_json("/path/to/data.jsonl")

write_paimon(
    ray_dataset,
    "database_name.table_name",
    catalog_options={"warehouse": "/path/to/warehouse"},
)
```

`write_paimon` opens its own catalog, resolves the table, and commits the
write through Ray's Datasink API — there is no separate `prepare_commit` or
`close` step to run.

**Overwrite mode:**

```python
write_paimon(
    ray_dataset,
    "database_name.table_name",
    catalog_options={"warehouse": "/path/to/warehouse"},
    overwrite=True,
)
```

**Distribution / scheduling:**

```python
write_paimon(
    ray_dataset,
    "database_name.table_name",
    catalog_options={"warehouse": "/path/to/warehouse"},
    concurrency=4,
    ray_remote_args={"num_cpus": 2},
)
```

**Automatic (partition, bucket) clustering for HASH_FIXED tables:**

For HASH_FIXED tables, `write_paimon` automatically clusters rows by
`(partition_keys..., bucket)` before writing so each (partition,
bucket) lands in a single Ray task — one writer, one file group. This
avoids the small-file storm that Ray's default round-robin
distribution would otherwise produce (`partitions × buckets ×
ray_tasks` files instead of `partitions × buckets`).

Bucket assignment uses the same hash routine the writer uses, so the
bucket seen by the groupby is byte-equivalent to the one the writer
would compute. No user configuration is required. For non-HASH_FIXED
tables the dataset is written as-is.

**Parameters:**
- `dataset`: the Ray Dataset to write.
- `table_identifier`: full table name, e.g. `"db_name.table_name"`.
- `catalog_options`: kwargs forwarded to `CatalogFactory.create()`.
- `overwrite`: if `True`, overwrite existing data in the table.
- `concurrency`: optional max number of Ray write tasks to run concurrently.
- `ray_remote_args`: optional kwargs passed to `ray.remote()` in write tasks
  (e.g. `{"num_cpus": 2}`).

### `TableWrite.write_ray()` (lower-level)

If you have already constructed a `table_write` from a write builder, you can
hand a Ray Dataset directly to it. `write_ray()` commits through the Ray
Datasink API, so there is no `prepare_commit` / `commit` step to run for the
Ray write itself — just close the writer when you are done with it:

```python
import ray

table = catalog.get_table('database_name.table_name')

# 1. Create table write and commit (commit is only needed for non-Ray writes
#    on the same table_write instance — see below).
write_builder = table.new_batch_write_builder()
table_write = write_builder.new_write()
table_commit = write_builder.new_commit()

# 2. Write Ray Dataset
ray_dataset = ray.data.read_json("/path/to/data.jsonl")
table_write.write_ray(ray_dataset, overwrite=False, concurrency=2)
# Parameters:
#   - dataset: Ray Dataset to write
#   - overwrite: Whether to overwrite existing data (default: False)
#   - concurrency: Optional max number of concurrent Ray tasks
#   - ray_remote_args: Optional kwargs passed to ray.remote() (e.g., {"num_cpus": 2})

# 3. Commit data (required for write_pandas/write_arrow/write_arrow_batch only)
commit_messages = table_write.prepare_commit()
table_commit.commit(commit_messages)

# 4. Close resources
table_write.close()
table_commit.close()
```

### Overwrite at builder level

The recommended way to overwrite via `write_paimon` is the `overwrite=True`
flag above. When using the lower-level builder API, you can also configure
overwrite mode on the write builder itself:

```python
# overwrite whole table
write_builder = table.new_batch_write_builder().overwrite()

# overwrite partition 'dt=2024-01-01'
write_builder = table.new_batch_write_builder().overwrite({'dt': '2024-01-01'})
```
