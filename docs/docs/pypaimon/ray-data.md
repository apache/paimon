---
title: "Ray Data"
sidebar_position: 3
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

Install the Ray integration in the driver and worker environments:

```shell
python -m pip install 'pypaimon[ray]'
```

`pypaimon.ray` exposes a top-level `read_paimon` / `write_paimon` facade that
takes a table identifier and catalog options directly, mirroring the shape of
Ray's built-in Iceberg integration. The lower-level `TableRead.to_ray()` and
`TableWrite.write_ray()` entry points remain available for callers that have
already resolved a `(read_builder, splits)` pair or constructed a
`table_write` via the regular pypaimon API.

If your application uses Daft DataFrames and only needs Ray as Daft's execution
backend, see [Running Daft on Ray](./daft#running-daft-on-ray).

| Task | Guide |
| --- | --- |
| Read a table into a Dataset | [Read](#read) |
| Write a Dataset and publish its results | [Write](#write) |
| Match rows across tables | [Joins and merge](./ray-joins) |
| Fetch selected payloads or backfill features | [Row IDs and backfills](./ray-row-ids) |

The basic examples use existing tables. Install the same dependencies in each
worker environment and give workers access to the catalog and warehouse. Local
filesystem paths are suitable for a local Ray cluster; a multi-node cluster needs
storage reachable from every worker.

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

**HASH_FIXED pre-clustering:**

HASH_FIXED rows are always assigned to the correct Paimon bucket by
the writer. For append-only tables, pre-clustering is only a file-count
optimization. Primary-key tables additionally require one writer per
`(partition_keys..., bucket)` group to generate ordered sequence numbers.

By default, `write_paimon` writes append-only HASH_FIXED tables
without pre-clustering. This avoids Ray `groupby().map_groups()`
materializing an entire `(partition_keys..., bucket)` group on one Ray
node.

HASH_FIXED primary-key tables reject the default/off mode. Direct Ray
writes can send the same bucket to multiple writer tasks, and those
writers can allocate overlapping sequence numbers. The explicit
`map_groups` mode avoids this by running one writer for each complete
`(partition_keys..., bucket)` group.

If every `(partition_keys..., bucket)` group fits in memory on a
single Ray node, you can opt in to the legacy small-file optimization:

```python
write_paimon(
    ray_dataset,
    "database_name.table_name",
    catalog_options={"warehouse": "/path/to/warehouse"},
    hash_fixed_precluster="map_groups",
)
```

`hash_fixed_precluster="map_groups"` groups rows by
`(partition_keys..., bucket)`. For primary-key tables, the Paimon writer
runs inside that `map_groups()` task and returns serialized commit
messages for the driver to commit. Ray output block splitting therefore
cannot create multiple writers for the same group. The mode inherits
Ray's `map_groups()` memory bound. Large append-only buckets or hot
append-only partitions should use the default mode or
`hash_fixed_precluster="off"`.

For non-HASH_FIXED append-only tables, the dataset is written as-is.
Postpone-bucket tables (`bucket = -2`) follow
`postpone.batch-write-fixed-bucket` (default: `true`). Existing partitions
reuse their bucket count; new partitions infer one from the configured target
row count or size. Ray materializes the input for this global plan, then sorts
by partition, bucket, and primary-key hash. One primary key stays with one
writer, while a large bucket can span multiple Ray blocks. Set
`hash_fixed_precluster="off"` to retain `bucket-postpone` writes. Fixed-bucket
postpone writes support `bucket-function.type=default` only. HASH_DYNAMIC and
CROSS_PARTITION primary-key Ray writes are not supported and fail fast,
including the default dynamic-bucket primary-key table (`bucket = -1`).
Ray write tasks create independent Paimon writers, which can assign
overlapping buckets or sequence numbers for those modes.

**Parameters:**
- `dataset`: the Ray Dataset to write.
- `table_identifier`: full table name, e.g. `"db_name.table_name"`.
- `catalog_options`: kwargs forwarded to `CatalogFactory.create()`.
- `overwrite`: if `True`, overwrite existing data in the table.
- `concurrency`: optional max number of Ray write tasks to run concurrently.
  For HASH_FIXED primary-key and postpone-bucket writes, this limits writer
  tasks.
- `ray_remote_args`: optional kwargs passed to `ray.remote()` in write tasks
  (e.g. `{"num_cpus": 2}`). These options also apply to HASH_FIXED
  primary-key and postpone-bucket writer tasks.
- `hash_fixed_precluster`: pre-clustering mode. `"auto"` follows table options,
  `"off"` disables pre-clustering, and `"map_groups"` explicitly enables
  HASH_FIXED grouping. This option does not enable HASH_DYNAMIC or
  CROSS_PARTITION primary-key writes.

### `TableWrite.write_ray()` (lower-level)

If you have already constructed a `table_write` from a write builder, you can
hand a Ray Dataset directly to it. `write_ray()` uses the same HASH_FIXED
pre-clustering modes and safety checks as the top-level `write_paimon()` API.
It commits through the Ray Datasink API, so there is no `prepare_commit` /
`commit` step to run for the Ray write itself — just close the writer when you
are done with it:

```python
import ray

table = catalog.get_table('database_name.table_name')

# 1. Create table write.
table_write = table.new_batch_write_builder().new_write()

# 2. Write Ray Dataset
ray_dataset = ray.data.read_json("/path/to/data.jsonl")
table_write.write_ray(
    ray_dataset,
    overwrite=False,
    concurrency=2,
    hash_fixed_precluster="auto",
    static_partition=None,
)
# Parameters:
#   - dataset: Ray Dataset to write
#   - overwrite: Whether to overwrite existing data (default: False)
#   - concurrency: Optional max number of concurrent Ray tasks
#   - ray_remote_args: Optional kwargs passed to ray.remote() (e.g., {"num_cpus": 2})
#   - hash_fixed_precluster: Same HASH_FIXED modes and primary-key safety
#     checks as write_paimon()
#   - static_partition: Optional partition spec to overwrite. When set,
#     write_ray() runs in overwrite mode for this partition.

# 3. Close resources
table_write.close()
```

An explicit `new_postpone_fixed_bucket_write_builder()` also enables the
real-bucket path without setting `hash_fixed_precluster`.

### Overwrite

The top-level `write_paimon()` API supports whole-table overwrite with the
`overwrite=True` flag above. With the lower-level `write_ray()` API, you can
use `overwrite=True` for whole-table overwrite and `static_partition={...}` for
partition overwrite:

```python
table_write.write_ray(ray_dataset, overwrite=True)
table_write.write_ray(ray_dataset, static_partition={'dt': '2024-01-01'})
```

When using the lower-level builder API, you can also configure overwrite mode
on the write builder itself. The resulting `table_write` carries the overwrite
partition into `write_ray()`. A `static_partition` argument passed directly to
`write_ray()` overrides the builder-level partition:

```python
# overwrite whole table
table_write = table.new_batch_write_builder().overwrite().new_write()
table_write.write_ray(ray_dataset)

# overwrite partition 'dt=2024-01-01'
table_write = (
    table.new_batch_write_builder()
    .overwrite({'dt': '2024-01-01'})
    .new_write()
)
table_write.write_ray(ray_dataset)
```

## Joins and merge

<span id="bucket-join"></span>
<span id="range-join"></span>
<span id="merge-into"></span>

| Data layout or operation | Guide |
| --- | --- |
| Two tables share a fixed bucket layout | [Bucket join](./ray-joins#bucket-join) |
| Tables are clustered by a join key | [Range join](./ray-joins#range-join) |
| Match source keys to update, delete, or insert rows | [Merge into](./ray-joins#merge-into) |

## Row IDs and backfills

<span id="update-by-row-id"></span>
<span id="read-by-row-id"></span>
<span id="process-row-id-ranges"></span>
<span id="resumable-embedding-backfill-from-a-blob-column"></span>

Use [row-ID reads and updates](./ray-row-ids) when you already have the target
row IDs. The [embedding backfill example](./ray-row-ids#resumable-embedding-backfill-from-a-blob-column)
shows bounded commits and application-managed resumption after failure.
