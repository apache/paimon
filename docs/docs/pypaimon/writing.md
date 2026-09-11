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

To replace data, configure overwrite on the write builder before creating the
writer and commit object:

```python
# overwrite whole table
write_builder = table.new_batch_write_builder().overwrite()

# overwrite partition 'dt=2024-01-01'
write_builder = table.new_batch_write_builder().overwrite({'dt': '2024-01-01'})
```

### Manifest Merging

`manifest.merge.skip-on-write-only` defaults to `false` in both Python and Java,
so commits keep their automatic manifest merging behavior. Set both this option
and `write-only` to `true` to retain existing manifest files during commit and
avoid the cost of reading and rewriting them. This option has no effect when
`write-only=false`, which is also the default.

Python supports minor manifest compaction, using `manifest.merge-min-count` and
`manifest.target-file-size`. Python does not support manifest sort rewrite.
In Java, skipping automatic manifest merging also skips automatic manifest sort
rewrite; explicit manifest compaction remains available.

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
