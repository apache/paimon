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

This requires `ray` to be installed.

`pypaimon.ray` exposes a top-level `read_paimon` / `write_paimon` facade that
takes a table identifier and catalog options directly, mirroring the shape of
Ray's built-in Iceberg integration. The lower-level `TableRead.to_ray()` and
`TableWrite.write_ray()` entry points remain available for callers that have
already resolved a `(read_builder, splits)` pair or constructed a
`table_write` via the regular pypaimon API.

If your application uses Daft DataFrames and only needs Ray as Daft's execution
backend, see [Running Daft on Ray](./daft#running-daft-on-ray).

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

## Bucket Join

`bucket_join` joins two **co-bucketed** tables (same bucket count and the same
bucket-key) on the bucket-key, with **no global shuffle**: the same key lands in
the same bucket on both sides, so each bucket is read and joined in its own Ray
task. It returns a `ray.data.Dataset` whose results stay distributed (never
pulled into the driver).

A common use is looking up a global `_ROW_ID` for a batch of keys without a
shuffle join against a large table: keep a small co-bucketed `(key, _ROW_ID)`
side table, `bucket_join` the incoming keys against it, then feed the resulting
row ids into a row-id update.

```python
from pypaimon.ray import bucket_join

ds = bucket_join(
    left="database_name.incoming_keys",   # co-bucketed table identifier
    right="database_name.key_rowid",       # co-bucketed table identifier
    catalog_options={"warehouse": "/path/to/warehouse"},
    on="url",                              # must equal the bucket-key
    left_projection=["url"],               # optional; must keep the join key
    right_projection=["url", "row_id"],    # optional; must keep the join key
)
# ds: ray.data.Dataset of the joined rows, e.g. {"url": ..., "row_id": ...}
```

**Parameters:**
- `left` / `right`: identifiers of the two co-bucketed tables to join.
- `on`: the join key(s). Must be exactly the bucket-key — equal keys only
  co-locate by bucket when joining on the bucket-key.
- `left_projection` / `right_projection`: optional column projections applied on
  read. If given, each must include the join key.
- `join_type`: only `"inner"` is supported (an outer join would need the union
  of buckets, which per-bucket intersection cannot produce).
- `ray_remote_args`: Ray remote options applied to each per-bucket join task.

**Returns:** a `ray.data.Dataset` of the joined rows.

**Notes:**
- Both tables must be fixed-bucket (`bucket > 0`) with the same bucket count and
  the same bucket-key (same column names, order, and types); otherwise
  `bucket_join` raises. For primary-key tables that do not set `bucket-key`
  explicitly, the bucket-key resolves to the (partition-trimmed) primary key.
- The two sides must not share columns other than the join key, or the
  underlying pyarrow join would collide; project them away with
  `left_projection` / `right_projection` first.
- Each side is planned at its own latest snapshot, and one bucket is joined by a
  single Ray task that reads the whole bucket into memory. Choose a bucket count
  that spreads keys evenly to avoid skewed, memory-heavy tasks.
- Partitioned tables are not supported yet (bucket ids are per-partition).

## Range Join

`range_join` joins tables clustered by the first join key without a global
shuffle. Each key range runs in one Ray task.

```python
from pypaimon.ray import range_join

ds = range_join(
    left="database_name.incoming_keys",
    right="database_name.key_rowid",
    catalog_options={"warehouse": "/path/to/warehouse"},
    left_on="url",
    right_on="lookup_url",
    left_projection=["url"],
    right_projection=["lookup_url", "row_id"],
    left_partitions={"dt": "2026-07-30"},  # optional
    num_ranges=64,                          # optional
)
```

Use `on="url"` when key names match. Multiple keys are supported; the first
defines ranges. Only inner join is supported.

Manifest/key stats are preferred; Parquet footers are the fallback. Missing
stats safely reduce parallelism, possibly to one task. Unclustered files may be
read repeatedly. Float/double and local-time-zone timestamp range keys are not
supported.

## Merge Into

`merge_into` updates or deletes matched rows and optionally inserts unmatched
rows of a **data-evolution** table from a source, like SQL `MERGE INTO`.
Matched rows are updated in place by `_ROW_ID`; only the touched columns are
rewritten. Matched delete clauses are written through deletion vectors.
Requires `ray >= 2.50` and a target table with `'data-evolution.enabled'` and
`'row-tracking.enabled'` set. If you use matched delete clauses, the target
must also enable `'deletion-vectors.enabled'`.

```python
from pypaimon.ray import merge_into, WhenMatched, WhenNotMatched

metrics = merge_into(
    target="database_name.table_name",
    source=ray_dataset,          # ray.data.Dataset / pa.Table / pandas / table-name str
    catalog_options={"warehouse": "/path/to/warehouse"},
    on=["id"],                   # or {"target_col": "source_col"} for renamed keys
    when_matched=[WhenMatched.update("*")],
    when_not_matched=[WhenNotMatched(insert="*")],             # optional
)
print(metrics)   # {"num_matched": 3, "num_inserted": 2, "num_unchanged": 0}
```

Conditional clauses filter which matched/unmatched rows are acted on:

```python
merge_into(
    target="db.table",
    source=source_ds,
    catalog_options=catalog_options,
    on=["id"],
    when_matched=[WhenMatched.update("*", condition="s.age > t.age")],
    when_not_matched=[WhenNotMatched(insert="*", condition="s.age > 18")],
)
```

Use `WhenMatched.delete()` to delete matched rows:

```python
merge_into(
    target="db.table",
    source=source_ds,
    catalog_options=catalog_options,
    on=["id"],
    when_matched=[
        WhenMatched.delete(condition="s.deleted = TRUE"),
        WhenMatched.update("*"),
    ],
)
```

Conditions use SQL-style expressions with `s.` (source) and `t.` (target)
column prefixes. `WhenNotMatched` conditions may only reference source
columns (`s.*`). Condition evaluation uses DataFusion through the PyPaimon SQL
extra. Install the extra before using conditions: `pip install pypaimon[sql]`.

- `update` / `delete` / `insert`: `WhenMatched.update(...)` updates matched
  rows, `WhenMatched.delete()` deletes matched rows, and
  `WhenNotMatched(insert=...)` inserts unmatched rows. `"*"` updates/inserts
  all columns from source, including blob columns.
  A mapping selects specific columns:
  ```python
  from pypaimon.ray import source_col, target_col, lit

  WhenMatched.update({"age": source_col("age"), "name": target_col("name")})
  WhenMatched.delete()
  WhenNotMatched(insert={"id": source_col("id"), "status": lit("new")})
  ```
  `"s.<col>"` / `"t.<col>"` shorthands also work (`t.*` only in update).
  Use `lit()` for literals starting with `s.` or `t.`.
- `condition`: an optional SQL-style boolean expression. Use `s.<col>` and
  `t.<col>` to reference source and target columns.
- Multiple clauses are evaluated in order; the first matching condition wins:
  ```python
  when_matched=[
      WhenMatched.update("*", condition="s.ts > t.ts"),
      WhenMatched.update("*"),  # fallback for unmatched rows
  ]
  ```

**Parameters:**
- `source`: a `ray.data.Dataset`, `pyarrow.Table`, `pandas.DataFrame`, or a
  Paimon table identifier string. When a string is passed, it reads the table
  from the same `catalog_options` at the latest snapshot.
- `on`: key columns, or `{target_col: source_col}` for renamed keys.
- `num_partitions`: shuffle parallelism for the join and the write; defaults to
  `max(1, cluster_cpus * 2)`. Raise it for large merges on big clusters.
- `ray_remote_args`: Ray remote options applied to the merge's map/group
  tasks (update/delete transform, group write, insert transform).
- `concurrency`: scheduling for the insert sink.

**Returns:** `{"num_matched", "num_inserted", "num_unchanged"}`. `num_matched`
counts the rows actually updated or deleted (after condition filtering).
`num_unchanged` is `0` in the current implementation.

For an end-to-end feature update workflow on Blob tables, see
[Distributed Feature Backfill with Ray](../learn-paimon/scenario-guide#distributed-feature-backfill-with-ray).

**Notes:**
- Partition key columns cannot be updated by matched update clauses, because
  cross-partition row movement is not implemented. Matched delete clauses and
  matched updates of non-partition columns work on partitioned tables.
  Not-matched inserts into partitioned tables work normally.
- Matched delete clauses require `deletion-vectors.enabled = true`.
- Blob columns can be updated and inserted by `merge_into`. With `update="*"`
  or `insert="*"`, the source must include the corresponding blob columns.
  If an insert mapping omits a blob column, that column is written as `NULL`.

## Update By Row Id

`update_by_row_id` updates columns of a **data-evolution** table straight from a
source that already carries `_ROW_ID` and the new values. Each row is routed to the
data file that owns its row id and only those files are rewritten — the target is
**never fully read** and there is **no join against it** (unlike
`merge_into(on=["_ROW_ID"])`, which reads and shuffle-joins the whole target). It
pairs with `bucket_join`, which produces the row ids without a shuffle. Requires
`ray >= 2.50` and a target with `data-evolution.enabled` and `row-tracking.enabled`.

```python
from pypaimon.ray import update_by_row_id

metrics = update_by_row_id(
    target="database_name.table_name",
    source=ray_dataset,          # ray.data.Dataset / pa.Table / pandas, carrying _ROW_ID
    catalog_options={"warehouse": "/path/to/warehouse"},
    update_cols=["feature"],     # non-blob columns to overwrite
)
print(metrics)   # {"num_updated": 50}
```

**Parameters:**
- `source`: a `ray.data.Dataset`, `pyarrow.Table`, or `pandas.DataFrame` carrying the
  target `_ROW_ID` and every column in `update_cols`; extra columns are ignored, and
  values are cast to the target column types. A table-name source is not accepted: a
  table's system `_ROW_ID` is its own and cannot address the target's rows.
- `update_cols`: the non-blob columns to overwrite. Must be non-empty.
- `num_partitions`: parallelism for grouping the update rows by target file;
  defaults to `max(1, cluster_cpus * 2)`.
- `ray_remote_args`: Ray remote options applied to the update tasks.

**Returns:** `{"num_updated": <rows>}`.

**Notes:**
- The row ids must exist in the target's current snapshot; a stale or foreign
  `_ROW_ID` raises rather than silently writing.
- Multiple source rows mapping to the same `_ROW_ID` is rejected — deduplicate first.
- Blob columns cannot be updated through this path.
- Partition columns cannot be updated (in-place rewrite can't move a row across partitions).
- Deletion-vectors-enabled tables are not supported yet: a DV-deleted row still lives
  in its data file, so it can't be told apart from a live row without reading the target.

## Read By Row Id

`read_by_row_id` is the read-side mirror of `update_by_row_id`: it reads columns
(including blob) of a **data-evolution** table for a set of `_ROW_ID`s, without
scanning or joining the whole target. Each row id is routed to the data file that
owns it and only those files — and only the matched rows — are read. It pairs with
`bucket_join` (which produces the row ids) and feeds `update_by_row_id`: match by
key → read the matched rows → transform → write back by row id. Requires
`ray >= 2.50` and a target with `data-evolution.enabled` and `row-tracking.enabled`.

```python
from pypaimon.ray import read_by_row_id

ds = read_by_row_id(
    target="database_name.table_name",
    row_ids=locator_ds,          # ray.data.Dataset / pa.Table / pandas, carrying the row ids
    catalog_options={"warehouse": "/path/to/warehouse"},
    projection=["image", "feature"],   # columns to read; may include blob columns
    row_id_col="row_id",         # source column holding the row ids (default "_ROW_ID")
)
# ds: ray.data.Dataset of (image, feature, _ROW_ID) for the matched rows
```

**Parameters:**
- `row_ids`: a `ray.data.Dataset`, `pyarrow.Table`, or `pandas.DataFrame` carrying the
  target row ids in column `row_id_col`; other columns are ignored. A table-name source
  is not accepted (a table's system `_ROW_ID` is its own and cannot address the target).
- `projection`: top-level columns to read (nested paths are not supported). Blob columns
  are resolved to their payloads, unless overridden via `dynamic_options`. Must be non-empty.
- `row_id_col`: the source column holding the row ids (default `_ROW_ID`); set e.g.
  `row_id_col="row_id"` to consume a `bucket_join` locator directly.
- `dynamic_options`: read options applied via `table.copy`, e.g.
  `{"blob-as-descriptor": "true"}` to read blob columns as small `BlobDescriptor` bytes
  (resolved later with `map_with_blobs`), or `scan.snapshot-id` / `scan.tag-name` to read a
  specific snapshot. Options that flip table invariants (`data-evolution.enabled`,
  `row-tracking.enabled`, `deletion-vectors.enabled`) are rejected.
- `num_partitions`: parallelism for grouping the row ids by target file; defaults to
  `max(1, cluster_cpus * 2)`.
- `ray_remote_args`: Ray remote options applied to the read tasks.

**Returns:** a `ray.data.Dataset` of `(*projection, _ROW_ID)`.

**Notes:**
- Lookup/set semantics, like SQL `... WHERE _ROW_ID IN (...)`: one row per **distinct**
  matched row id (duplicates deduplicated), input order not preserved (rows come out
  grouped by owning file). An empty source yields an empty but correctly-typed Dataset.
- The row ids must exist in the resolved target snapshot (latest, or the one selected via
  `dynamic_options`); a foreign `_ROW_ID` raises.
- Deletion-vectors-enabled tables are not supported yet, for the same reason as
  `update_by_row_id`.
- For a non-empty target, the `row_ids` source is consumed lazily by the downstream
  action, not read here. A lazy source missing `row_id_col` raises when the read runs
  (a materialized source raises up front).

## Update By Predicate

`update_by_predicate` applies a vectorized Arrow transform to matching rows and
commits file-group-aligned batches. The application supplies the predicate,
columns, transform, and target rows per commit; it does not handle row ids or
physical files.

```python
import pyarrow as pa
import pyarrow.compute as pc

from pypaimon import CatalogFactory
from pypaimon.data import variant_get, variant_replace
from pypaimon.ray import update_by_predicate

target = "database_name.topics"
options = {"warehouse": "/path/to/warehouse"}
table = CatalogFactory.create(options).get_table(target)
pb = table.new_read_builder().new_predicate_builder()
predicate = pb.or_predicates([
    pb.is_null("topic_schema"),
    pb.not_equal("topic_schema", "imu-v2"),
])


def fix_payload(batch):
    payload = batch["payload"]
    value = variant_get(payload, "$.angular_velocity.y", pa.float64())
    return pa.table({
        "payload": variant_replace(payload, {
            "$.angular_velocity.y": pc.negate(value),
        }),
        "topic_schema": ["imu-v2"] * len(batch),
    })


metrics = update_by_predicate(
    target,
    predicate,
    fix_payload,
    options,
    read_columns=["payload"],
    update_cols=["payload", "topic_schema"],
    rows_per_commit=10_000_000,
    batch_size=4096,
)
```

`read_columns` must also be present in `update_cols`, so concurrent changes to
transform inputs use the existing row-id update conflict detection. Completed
batches remain visible after a later failure. Use an idempotent transform or a
predicate such as the version check above so retry skips completed rows.

## Process Row Id Ranges

`process_row_id_ranges` plans the latest snapshot into logical file groups and
calls a user-supplied processor synchronously for each target-sized batch. The
processor receives a `List[Range]` and owns the read, distributed computation,
commit, and retry policy. Base files and overlapping data-evolution, BLOB, or
VECTOR files remain in one indivisible group.

`rows_per_commit` is therefore a target rather than a hard limit: the function
never splits a file group, so a batch can contain more rows. The range plan is
captured once at the start of a run, callbacks execute in row-id order, and an
exception stops later callbacks.

### Resumable embedding backfill from a BLOB column

The following pattern reads an `image` BLOB, computes a nullable `embedding`
VECTOR with Ray, and commits about one million row ids at a time. It pushes
`embedding IS NULL` into each range scan, so a completed row is filtered before
its image payload is materialized. Rerun the whole function after a failure;
already committed ranges are skipped automatically.

```python
import pyarrow as pa

from my_embedding_model import load_model
from pypaimon import CatalogFactory
from pypaimon.ray import process_row_id_ranges, update_by_row_id

TARGET = "database_name.images"
CATALOG_OPTIONS = {"warehouse": "/path/to/warehouse"}
EMBEDDING_DIM = 768


class EmbedImages:
    def __init__(self):
        # Constructed once in every Ray actor, not once per Arrow batch.
        self.model = load_model()

    def __call__(self, batch: pa.Table) -> pa.Table:
        vectors = self.model.encode(batch["image"].to_pylist())
        return pa.table({
            "_ROW_ID": batch["_ROW_ID"],
            "embedding": pa.array(
                vectors.tolist(),
                type=pa.list_(pa.float32(), EMBEDDING_DIM),
            ),
        })


def process_ranges(ranges):
    # Resolve a fresh table for every batch so this scan sees embeddings
    # committed by earlier callbacks. Force BLOB payloads rather than descriptors.
    table = (
        CatalogFactory.create(CATALOG_OPTIONS)
        .get_table(TARGET)
        .copy({"blob-as-descriptor": "false"})
    )
    read_builder = table.new_read_builder().with_projection(
        ["image", "embedding", "_ROW_ID"]
    )
    read_builder.with_filter(
        read_builder.new_predicate_builder().is_null("embedding")
    )
    splits = (
        read_builder.new_scan()
        .with_row_ranges(ranges)
        .plan()
        .splits()
    )
    pending = read_builder.new_read().to_ray(
        splits,
        concurrency=64,
        ray_remote_args={"num_cpus": 1},
    )
    if pending.limit(1).count() == 0:
        return

    updates = pending.map_batches(
        EmbedImages,
        batch_format="pyarrow",
        batch_size=128,
        concurrency=8,       # required for a callable-class Ray actor pool
        num_gpus=1,
    )

    # update_by_row_id executes the Ray pipeline and makes one Paimon commit.
    # It is valid for VECTOR/ARRAY embedding columns; BLOB columns themselves
    # cannot be updated through update_by_row_id.
    update_by_row_id(
        target=TARGET,
        source=updates,
        catalog_options=CATALOG_OPTIONS,
        update_cols=["embedding"],
        num_partitions=128,
    )


process_row_id_ranges(
    TARGET,
    CATALOG_OPTIONS,
    rows_per_commit=1_000_000,
    processor=process_ranges,
)
```

The target must enable `row-tracking.enabled` and
`data-evolution.enabled`; `embedding` must be nullable and the table must not
enable deletion vectors. If the source BLOB or embedding model can change,
use an additional source/model-version column instead of treating every
non-null embedding as permanently complete. `process_row_id_ranges` does not
retry a failed processor itself—the resumability in this example comes from
rerunning it and selecting only rows whose embedding is still null.
