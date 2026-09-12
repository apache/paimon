---
title: "Ray Joins and Merge"
sidebar_label: "Joins and Merge"
description: "Join Paimon tables or merge a distributed source into a data-evolution table."
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

# Ray Joins and Merge

Join Paimon tables or merge a distributed source into a data-evolution table. Install Ray as described in [Ray Data](./ray-data). The examples use existing tables with the stated bucket, key, and schema requirements.

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
columns (`s.*`). Condition evaluation uses the PyPaimon DataFusion extra.
Python 3.10 or newer is required. Install it with
`pip install 'pypaimon[datafusion]'`.

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

For self-merge (`source == target` and `on=["_ROW_ID"]`), update values may
also be callables. A callable receives the matched `read_columns` plus
`_ROW_ID` as a `pyarrow.Table` and must return one `pyarrow.Array` or
`pyarrow.ChunkedArray` value per input row:

```python
import pyarrow.compute as pc

merge_into(
    target="db.table",
    source="db.table",
    catalog_options=catalog_options,
    on=["_ROW_ID"],
    read_columns=["age"],
    when_matched=[WhenMatched.update({
        "age": lambda rows: pc.add(rows["age"], 1),
    }, condition="t.id IN (1, 3)")],
)
```

Callables may run zero, one, or multiple times and must be deterministic,
side-effect-free, and row-local. They are not supported for general
source-target merges.

**Parameters:**
- `source`: a `ray.data.Dataset`, `pyarrow.Table`, `pandas.DataFrame`, or a
  Paimon table identifier string. When a string is passed, it reads the table
  from the same `catalog_options` at the latest snapshot.
- `on`: key columns, or `{target_col: source_col}` for renamed keys.
- `read_columns`: columns passed to callable self-merge assignments. Required
  when an update mapping contains a callable; otherwise it must be omitted.
- `num_partitions`: shuffle parallelism for the join and the write. When input
  in-memory byte-size metadata is reliable, the default targets Ray's maximum
  block size. Otherwise it uses Ray's hash-shuffle default. A nonempty target
  keeps that default as a lower bound, and cluster CPUs cap the result. Self-merge
  keeps its CPU-based default. Set it explicitly to override the default.
- `ray_remote_args`: Ray remote options applied to the merge's map/group
  tasks (update/delete transform, group write, insert transform).
- `concurrency`: scheduling for the insert sink.

**Returns:** `{"num_matched", "num_inserted", "num_unchanged"}`. `num_matched`
counts the rows actually updated or deleted (after condition filtering).
`num_unchanged` is `0` in the current implementation.

For an end-to-end feature update workflow on Blob tables, see
[Distributed Feature Backfill with Ray](../learn-paimon/ai-pipelines#distributed-feature-backfill-with-ray).

**Notes:**
- Partition key columns cannot be updated by matched update clauses, because
  cross-partition row movement is not implemented. Matched delete clauses and
  matched updates of non-partition columns work on partitioned tables.
  Not-matched inserts into partitioned tables work normally.
- Matched delete clauses require `deletion-vectors.enabled = true`.
- Blob columns can be updated and inserted by `merge_into`. With `update="*"`
  or `insert="*"`, the source must include the corresponding blob columns.
  If an insert mapping omits a blob column, that column is written as `NULL`.
