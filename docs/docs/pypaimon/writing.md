---
title: "Batch Writes"
description: "Write Arrow or pandas data with an explicit write-and-commit lifecycle."
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

# Batch Writes

Write Arrow or pandas data with an explicit write-and-commit lifecycle. These examples assume you have a `table` from [Catalogs and Tables](./catalogs).

## Batch Write

A batch write has two steps: write data files, then publish them in a snapshot.
You can write multiple Arrow or pandas batches with one writer. Call
`prepare_commit()` once, pass its messages to the commit object, and close both
objects. Create a new writer for another commit.

The following example shows alternative input formats. Replace the placeholder
values with data matching your table schema; the [quick start](./quick-start)
contains a complete executable example.

```python
table = catalog.get_table('database_name.table_name')

# 1. Create table write and commit
write_builder = table.new_batch_write_builder()
table_write = write_builder.new_write()
table_commit = write_builder.new_commit()

# 2. Choose the method for your input format:
# 2.1 Write pandas.DataFrame
dataframe = ...
table_write.write_pandas(dataframe)

# 2.2 Write pyarrow.Table
pa_table = ...
table_write.write_arrow(pa_table)

# 2.3 Write pyarrow.RecordBatch
record_batch = ...
table_write.write_arrow_batch(record_batch)

# 3. Publish the written batches in one snapshot
commit_messages = table_write.prepare_commit()
table_commit.commit(commit_messages)

# 4. Close resources
table_write.close()
table_commit.close()
```

`new_batch_write_builder()` keeps postpone-bucket writes in `bucket-postpone`.
Use `new_postpone_fixed_bucket_write_builder()` to write real buckets. New
partitions are buffered until `prepare_commit()`; existing ones are incremental.
This builder currently supports `bucket-function.type=default` only.

### Native Write and Commit

With the `pypaimon-rust` extension installed, set `write.native.enabled=true`
to use Rust for Parquet/BLOB writes and `commit.native.enabled=true` to use
Rust for REST snapshot publication. Batch and stream writers retain the same
prepare-and-commit lifecycle. Native commits also support overwrite,
`truncate_table()`, `truncate_partitions()` and commit callbacks.
Overwrite and truncate record an OVERWRITE snapshot even when no files match,
and remove index metadata for the replaced partitions. Physical files remain
available to historical snapshots. Changelog-only commits also publish a snapshot.
For Parquet tables, both paths support `manifest.compression=null`, `deflate`,
`snappy`, `zstd`, `bzip2` and `xz`, including manifests retained from earlier
snapshots. Overwrite and truncate partition specs convert textual values to the
declared partition type. `None` and the configured default partition name select
NULL partitions; malformed values are rejected before publication.

Standard REST tables with `query-auth.enabled=true` can write and commit
without requesting SELECT permission. The REST server still authorizes snapshot
publication. Row filters and column masks continue to apply to explicit reads;
internal sequence-number and HASH-index restoration reads manifest metadata.
For query-authorized targets, Native row-ID updates, predicate updates/deletes,
upserts, shard updates and MERGE require an unrestricted whole-table SELECT grant
before reading target rows or evaluating assignments. A row filter, column mask
or denied grant rejects the operation; ordinary writes still require no SELECT
grant. MERGE table sources obtain their own grants independently from the target,
including historical snapshots and branches.
Query-authorized cross-partition index bootstrap remains unsupported.

Whole-partition predicate deletes can use Native on primary-key tables configured
with input changelogs. Native upserts also accept fully NULL source key columns
with a different physical type, including selected NULL dictionary values.

`write_ray()` uses the shared Python coordinator and lets workers select Native
writers. For postpone fixed-bucket writes, overwrite plans use the new input
unless an explicit bucket plan was supplied. A complete primary-key
`with_write_type()` leaves the writer unchanged; primary-key column subsets
remain unsupported.

Append `with_write_type()` preserves the selected column order. Both the full
table schema and the selected schema are accepted as Arrow input; only selected
columns are written. Full Arrow inputs and named Rows can carry partition and
hash columns used only for routing, independently of the physical write type.
Reads restore omitted partition columns from split metadata. Selected-schema
inputs must include the routing columns they need; `write_arrow(data, bucket=...)`
also accepts a bucket assigned upstream.

Native commit selection requires a standard REST catalog and a reconstructible
built-in FileIO. Other catalogs and custom Python FileIO implementations retain
the Python path. Binary partition keys require `partition.legacy-name=false` for
portable names across implementations.

To replace data, configure overwrite on the write builder before creating the
writer and commit object:

```python
# overwrite whole table
write_builder = table.new_batch_write_builder().overwrite()

# overwrite partition 'dt=2024-01-01'
write_builder = table.new_batch_write_builder().overwrite({'dt': '2024-01-01'})
```

### Parquet Page Indexes

Python Parquet writers write page indexes by default on PyArrow >= 13; older
versions omit them. Set `parquet.write-page-index.enabled=false` to disable them,
or `true` to require them. Explicit `true` on PyArrow < 13 fails before creating files.

This affects new files only, independently of read-side filtering. When enabled,
page-level statistics move from page headers into the index. Check reader
compatibility when upgrading; PyArrow reads do not currently use these indexes.

### Manifest Merging

Python commits retain existing manifest files without merging or rewriting them,
including during commit retries. Run manifest compaction through a centralized
maintenance service, for example using the Java engines' `compact_manifest` procedure.

The table options `manifest.merge-min-count`, `manifest.merge.skip-on-write-only`,
and `write-only` do not enable manifest merging in Python.
`manifest.target-file-size` still controls the size of newly written manifest files.

### Commit Callback

You can register `CommitCallback` instances on a `TableCommit` to be notified after each successful
snapshot commit. This is useful for post-commit actions such as syncing metadata to external systems.

Implementations must be **idempotent** — a callback may be invoked more than once for the same commit
if a failure occurs right after the commit succeeds.

```python
from pypaimon.write.commit_callback import CommitCallback, CommitCallbackContext

class MyCallback(CommitCallback):
    def call(self, context: CommitCallbackContext) -> None:
        print(f"Committed snapshot {context.snapshot.id}, "
              f"{len(context.commit_entries)} entries, "
              f"identifier {context.identifier}")

    def close(self) -> None:
        pass  # release resources if needed

write_builder = table.new_batch_write_builder()
table_write = write_builder.new_write()
table_commit = write_builder.new_commit()
table_commit.add_commit_callback(MyCallback())

table_write.write_arrow(data)
table_commit.commit(table_write.prepare_commit())
table_write.close()
table_commit.close()
```

`CommitCallbackContext` provides:

| Field            | Type                | Description                              |
|------------------|---------------------|------------------------------------------|
| `snapshot`       | `Snapshot`          | The committed snapshot (id, commit_kind, time_millis, next_row_id, …) |
| `commit_entries` | `List[ManifestEntry]` | Delta manifest entries in this commit (each carries `file.first_row_id` when row-tracking is enabled) |
| `identifier`     | `int`               | Commit identifier                        |
