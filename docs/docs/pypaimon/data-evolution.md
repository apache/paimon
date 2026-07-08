---
title: "Data Evolution"
sidebar_position: 5
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

# Data Evolution

PyPaimon for Data Evolution mode. See [Data Evolution](../multimodal-table/data-evolution).

## Prerequisites

To use partial updates / data evolution, enable both options when creating the table:

- **`row-tracking.enabled`**: `true`
- **`data-evolution.enabled`**: `true`

## Update Columns By Row ID

You can use `update_by_arrow_with_row_id` to update columns in data evolution tables.

The input data should include the `_ROW_ID` column. The update operation will automatically sort and match each `_ROW_ID`
to its corresponding `first_row_id`, then group rows with the same `first_row_id` and write them to a separate file.

**Requirements for `_ROW_ID` updates**

- **Update columns only**: include `_ROW_ID` plus the columns you want to update (partial schema is OK).

```python
import pyarrow as pa
from pypaimon import CatalogFactory, Schema

catalog = CatalogFactory.create({'warehouse': '/tmp/warehouse'})
catalog.create_database('default', False)

simple_pa_schema = pa.schema([
  ('f0', pa.int8()),
  ('f1', pa.int16()),
])
schema = Schema.from_pyarrow_schema(simple_pa_schema,
                                    options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'})
catalog.create_table('default.test_row_tracking', schema, False)
table = catalog.get_table('default.test_row_tracking')

# write all columns
write_builder = table.new_batch_write_builder()
table_write = write_builder.new_write()
table_commit = write_builder.new_commit()
expect_data = pa.Table.from_pydict({
  'f0': [-1, 2],
  'f1': [-1001, 1002]
}, schema=simple_pa_schema)
table_write.write_arrow(expect_data)
table_commit.commit(table_write.prepare_commit())
table_write.close()
table_commit.close()

# update partial columns
write_builder = table.new_batch_write_builder()
table_update = write_builder.new_update().with_update_type(['f0'])
table_commit = write_builder.new_commit()
data2 = pa.Table.from_pydict({
  '_ROW_ID': [0, 1],
  'f0': [5, 6],
}, schema=pa.schema([
  ('_ROW_ID', pa.int64()),
  ('f0', pa.int8()),
]))
cmts = table_update.update_by_arrow_with_row_id(data2)
table_commit.commit(cmts)
table_commit.close()

# content should be:
#   'f0': [5, 6],
#   'f1': [-1001, 1002]
```

## Update Columns By Predicate

You can use `update_by_predicate` for SQL-like `UPDATE ... SET ... WHERE ...`
operations. The `Predicate` identifies rows to update, and the assignment map
contains literal values for updated columns.
When global indexes are available, `update_by_predicate` discovers matching
`_ROW_ID` values with `global-index.search-mode=full` on the configured
point-in-time scan snapshot or, if none is configured, the latest snapshot.

```python
import pyarrow as pa
from pypaimon import CatalogFactory, Schema

catalog = CatalogFactory.create({'warehouse': '/tmp/warehouse'})
catalog.create_database('default', False)

pa_schema = pa.schema([
    ('id', pa.int32()),
    ('name', pa.string()),
    ('age', pa.int32()),
])
schema = Schema.from_pyarrow_schema(
    pa_schema,
    options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'},
)
catalog.create_table('default.users_update', schema, False)
table = catalog.get_table('default.users_update')

# write initial data
write_builder = table.new_batch_write_builder()
write = write_builder.new_write()
commit = write_builder.new_commit()
write.write_arrow(pa.Table.from_pydict(
    {'id': [1, 2, 3], 'name': ['Alice', 'Bob', 'Charlie'], 'age': [30, 25, 28]},
    schema=pa_schema,
))
commit.commit(write.prepare_commit())
write.close()
commit.close()

# UPDATE users_update SET age = 99 WHERE id IN (1, 3)
write_builder = table.new_batch_write_builder()
table_update = write_builder.new_update()
predicate = table_update.new_predicate_builder().is_in('id', [1, 3])
messages = table_update.update_by_predicate(predicate, {'age': 99})

commit = write_builder.new_commit()
commit.commit(messages)
commit.close()
```

## Delete Rows

Use `delete_by_predicate` for SQL-like `DELETE ... WHERE ...` operations.
For row-level deletes, the target table must enable deletion vectors in
addition to the [Prerequisites](#prerequisites):

- **`deletion-vectors.enabled`**: `true`

Deletes are written as deletion-vector index updates. If the predicate only
references partition columns, PyPaimon uses a partition overwrite/drop path
instead of scanning `_ROW_ID` values; that partition-only fast path does not
require deletion vectors.

```python
schema = Schema.from_pyarrow_schema(
    pa_schema,
    options={
        'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true',
        'deletion-vectors.enabled': 'true',
    },
)
catalog.create_table('default.users_delete', schema, False)
table = catalog.get_table('default.users_delete')

# ... write initial data ...

write_builder = table.new_batch_write_builder()
table_update = write_builder.new_update()
table_commit = write_builder.new_commit()

# DELETE FROM users_delete WHERE age >= 35
predicate = table_update.new_predicate_builder().greater_or_equal('age', 35)
messages = table_update.delete_by_predicate(predicate)
table_commit.commit(messages)
table_commit.close()
```

If you already have `_ROW_ID` values, use `delete_by_row_id` to write deletion
vectors directly:

```python
messages = table_update.delete_by_row_id([0, 2, 4])
table_commit.commit(messages)
```

## Filter by _ROW_ID

Requires the same [Prerequisites](#prerequisites) (row-tracking and data-evolution enabled). On such tables you can filter by `_ROW_ID` to prune files at scan time. Supported: `equal('_ROW_ID', id)`, `is_in('_ROW_ID', [id1, ...])`, `between('_ROW_ID', low, high)`.

```python
pb = table.new_read_builder().new_predicate_builder()
rb = table.new_read_builder().with_filter(pb.equal('_ROW_ID', 0))
result = rb.new_read().to_arrow(rb.new_scan().plan().splits())
```

## Upsert By Key

If you want to **upsert** (update-or-insert) rows by one or more business key columns — without manually providing
`_ROW_ID` — use `upsert_by_arrow_with_key`. For each input row:

- **Key matches** an existing row → update that row in place.
- **No match** → append as a new row.

**Requirements**

- The table must have `data-evolution.enabled = true` and `row-tracking.enabled = true`.
- All `upsert_keys` must exist in both the table schema and the input data.
- For **partitioned tables**, the input data must contain all partition key columns. Partition keys are
  **automatically stripped** from `upsert_keys` during matching (since each partition is processed independently),
  so you do **not** need to include them in `upsert_keys`.

**Example: basic upsert**

```python
import pyarrow as pa
from pypaimon import CatalogFactory, Schema

catalog = CatalogFactory.create({'warehouse': '/tmp/warehouse'})
catalog.create_database('default', False)

pa_schema = pa.schema([
    ('id', pa.int32()),
    ('name', pa.string()),
    ('age', pa.int32()),
])
schema = Schema.from_pyarrow_schema(
    pa_schema,
    options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'},
)
catalog.create_table('default.users', schema, False)
table = catalog.get_table('default.users')

# write initial data
write_builder = table.new_batch_write_builder()
write = write_builder.new_write()
commit = write_builder.new_commit()
write.write_arrow(pa.Table.from_pydict(
    {'id': [1, 2], 'name': ['Alice', 'Bob'], 'age': [30, 25]},
    schema=pa_schema,
))
commit.commit(write.prepare_commit())
write.close()
commit.close()

# upsert: update id=1, insert id=3
write_builder = table.new_batch_write_builder()
table_update = write_builder.new_update()
table_commit = write_builder.new_commit()

upsert_data = pa.Table.from_pydict(
    {'id': [1, 3], 'name': ['Alice_v2', 'Charlie'], 'age': [31, 28]},
    schema=pa_schema,
)
cmts = table_update.upsert_by_arrow_with_key(upsert_data, upsert_keys=['id'])
table_commit.commit(cmts)
table_commit.close()

# content should be:
#   id=1: name='Alice_v2', age=31   (updated)
#   id=2: name='Bob',      age=25   (unchanged)
#   id=3: name='Charlie',  age=28   (new)
```

**Example: partial-column upsert with `update_cols`**

Combine `with_update_type` with `upsert_by_arrow_with_key` to update only specific columns for
matched rows while still appending full rows for new keys:

```python
write_builder = table.new_batch_write_builder()
table_update = write_builder.new_update().with_update_type(['age'])
table_commit = write_builder.new_commit()

upsert_data = pa.Table.from_pydict(
    {'id': [1, 4], 'name': ['ignored', 'David'], 'age': [99, 22]},
    schema=pa_schema,
)
cmts = table_update.upsert_by_arrow_with_key(upsert_data, upsert_keys=['id'])
table_commit.commit(cmts)
table_commit.close()

# id=1: only 'age' is updated to 99; 'name' remains 'Alice_v2'
# id=4: appended as a full new row
```

**Example: partitioned table with composite key**

```python
partitioned_schema = pa.schema([
    ('id', pa.int32()),
    ('name', pa.string()),
    ('region', pa.string()),
])
schema = Schema.from_pyarrow_schema(
    partitioned_schema,
    partition_keys=['region'],
    options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'},
)
catalog.create_table('default.users_partitioned', schema, False)
table = catalog.get_table('default.users_partitioned')

# ... write initial data ...

write_builder = table.new_batch_write_builder()
table_update = write_builder.new_update()
table_commit = write_builder.new_commit()

upsert_data = pa.Table.from_pydict(
    {'id': [1, 3], 'name': ['Alice_v2', 'Charlie'], 'region': ['US', 'EU']},
    schema=partitioned_schema,
)
# upsert_keys=['id'] only; partition key 'region' is auto-stripped
cmts = table_update.upsert_by_arrow_with_key(upsert_data, upsert_keys=['id'])
table_commit.commit(cmts)
table_commit.close()
```

**Notes**

- Execution is driven **partition-by-partition**: only one partition's key set is loaded into memory at a time.
- Duplicate keys in the input data are automatically deduplicated — the **last occurrence** is kept.
- The upsert is atomic per commit — all matched updates and new appends are included in the same commit.

## Merge Into

Use `merge_into` when your source data should update or delete matched target
rows and optionally insert rows that do not match, similar to SQL `MERGE INTO`.
`merge_into` is exposed from `TableUpdate`, so it follows the same
commit-message lifecycle as other PyPaimon update APIs. The PyPaimon
implementation runs in a single process and materializes the rows it needs
locally.

Matched rows are updated by `_ROW_ID` internally, or deleted through deletion
vectors for delete clauses. Only the columns touched by update clauses are
rewritten. `merge_into` derives the update columns from the `WhenMatched`
clauses; `with_update_type` is not needed.

**Requirements**

- The target table must have `data-evolution.enabled = true` and
  `row-tracking.enabled = true`.
- Matched delete clauses require `deletion-vectors.enabled = true`.
- `source` must be a `pyarrow.Table`, `pandas.DataFrame`, or another PyPaimon
  table object.
- `on` can be a list of same-named key columns, or `{target_col: source_col}`
  for renamed source keys.
- If multiple source rows match the same target `_ROW_ID`, `merge_into` raises
  an error. Deduplicate the source before merging.

```python
import pyarrow as pa
from pypaimon import CatalogFactory, Schema
from pypaimon.table.data_evolution_merge_into import (
    WhenMatched,
    WhenNotMatched,
)

catalog = CatalogFactory.create({'warehouse': '/tmp/warehouse'})
catalog.create_database('default', False)

pa_schema = pa.schema([
    ('id', pa.int32()),
    ('name', pa.string()),
    ('age', pa.int32()),
])
schema = Schema.from_pyarrow_schema(
    pa_schema,
    options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'},
)
catalog.create_table('default.users_merge', schema, False)
table = catalog.get_table('default.users_merge')

# write initial data
write_builder = table.new_batch_write_builder()
write = write_builder.new_write()
commit = write_builder.new_commit()
write.write_arrow(pa.Table.from_pydict(
    {'id': [1, 2], 'name': ['Alice', 'Bob'], 'age': [30, 25]},
    schema=pa_schema,
))
commit.commit(write.prepare_commit())
write.close()
commit.close()

# merge: update id=2, insert id=3
source = pa.Table.from_pydict(
    {'id': [2, 3], 'name': ['Bob_v2', 'Charlie'], 'age': [26, 28]},
    schema=pa_schema,
)

write_builder = table.new_batch_write_builder()
table_update = write_builder.new_update()
table_commit = write_builder.new_commit()

messages = table_update.merge_into(
    source,
    on=['id'],
    when_matched=[WhenMatched.update('*')],
    when_not_matched=[WhenNotMatched(insert='*')],
)
table_commit.commit(messages)
table_commit.close()
```

`WhenMatched` and `WhenNotMatched` clauses can use `'*'` to copy same-named
columns from source, or a mapping for explicit assignments:

```python
from pypaimon.table.data_evolution_merge_into import (
    WhenMatched,
    WhenNotMatched,
    lit,
    source_col,
    target_col,
)

messages = table_update.merge_into(
    source,
    on={'id': 'source_id'},
    when_matched=[
        WhenMatched.update({
            'age': source_col('new_age'),
            'name': target_col('name'),
        }),
    ],
    when_not_matched=[
        WhenNotMatched(insert={
            'id': source_col('source_id'),
            'name': source_col('name'),
            'age': lit(0),
        }),
    ],
)
```

Conditions use SQL-style expressions with `s.` (source) and `t.` (target)
column prefixes. `WhenNotMatched` conditions may only reference source columns
(`s.*`). Condition evaluation uses DataFusion through the PyPaimon SQL extra.
Install the extra before using conditions: `pip install pypaimon[sql]`.

```python
messages = table_update.merge_into(
    source,
    on=['id'],
    when_matched=[WhenMatched.update('*', condition='s.age > t.age')],
    when_not_matched=[WhenNotMatched(insert='*', condition='s.age > 18')],
)
```

Use `WhenMatched.delete()` to delete matched rows:

```python
messages = table_update.merge_into(
    source,
    on=['id'],
    when_matched=[
        WhenMatched.delete(condition='s.deleted = TRUE'),
        WhenMatched.update('*'),
    ],
)
```

**Notes**

- Multiple clauses are evaluated in order; the first matching condition wins.
- Matched clauses cannot update partition key columns, because cross-partition
  row movement is not implemented.
- Matched delete clauses use deletion vectors, so the target table must enable
  `deletion-vectors.enabled`.
- Blob columns can be updated and inserted by `merge_into`. With `update="*"`
  or `insert="*"`, the source must include the corresponding blob columns.
  If an insert mapping omits a blob column, that column is written as `NULL`.

## Update Columns By Shards

If you want to **compute a derived column** (or **update an existing column based on other columns**) without providing
`_ROW_ID`, you can use the shard scan + rewrite workflow:

- Read only the columns you need (projection)
- Compute the new values in the same row order
- Write only the updated columns back
- Commit per shard

This is useful for backfilling a newly added column, or recomputing a column from other columns.

**Example: compute `d = c + b - a`**

```python
import pyarrow as pa
from pypaimon import CatalogFactory, Schema

catalog = CatalogFactory.create({'warehouse': '/tmp/warehouse'})
catalog.create_database('default', False)

table_schema = pa.schema([
    ('a', pa.int32()),
    ('b', pa.int32()),
    ('c', pa.int32()),
    ('d', pa.int32()),
])

schema = Schema.from_pyarrow_schema(
    table_schema,
    options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'},
)
catalog.create_table('default.t', schema, False)
table = catalog.get_table('default.t')

# write initial data (a, b, c only)
write_builder = table.new_batch_write_builder()
write = write_builder.new_write().with_write_type(['a', 'b', 'c'])
commit = write_builder.new_commit()
write.write_arrow(pa.Table.from_pydict({'a': [1, 2], 'b': [10, 20], 'c': [100, 200]}))
commit.commit(write.prepare_commit())
write.close()
commit.close()

# shard update: read (a, b, c), write only (d)
update = write_builder.new_update()
update.with_read_projection(['a', 'b', 'c'])
update.with_update_type(['d'])

shard_idx = 0
num_shards = 1
upd = update.new_shard_updator(shard_idx, num_shards)
reader = upd.arrow_reader()

for batch in iter(reader.read_next_batch, None):
    a = batch.column('a').to_pylist()
    b = batch.column('b').to_pylist()
    c = batch.column('c').to_pylist()
    d = [ci + bi - ai for ai, bi, ci in zip(a, b, c)]

    upd.update_by_arrow_batch(
        pa.RecordBatch.from_pydict({'d': d}, schema=pa.schema([('d', pa.int32())]))
    )

commit_messages = upd.prepare_commit()
commit = write_builder.new_commit()
commit.commit(commit_messages)
commit.close()
```

**Example: update an existing column `c = b - a`**

```python
update = write_builder.new_update()
update.with_read_projection(['a', 'b'])
update.with_update_type(['c'])

upd = update.new_shard_updator(0, 1)
reader = upd.arrow_reader()
for batch in iter(reader.read_next_batch, None):
    a = batch.column('a').to_pylist()
    b = batch.column('b').to_pylist()
    c = [bi - ai for ai, bi in zip(a, b)]
    upd.update_by_arrow_batch(
        pa.RecordBatch.from_pydict({'c': c}, schema=pa.schema([('c', pa.int32())]))
    )

commit_messages = upd.prepare_commit()
commit = write_builder.new_commit()
commit.commit(commit_messages)
commit.close()
```

**Notes**

- **Row order matters**: the batches you write must have the **same number of rows** as the batches you read, in the
  same order for that shard.
- **Parallelism**: run multiple shards by calling `new_shard_updator(shard_idx, num_shards)` for each shard.

## Stream Mode

Data evolution also supports stream mode. The operation semantics are the same
as the batch APIs above; the main differences are the builder lifecycle and the
required `commit_identifier`.

- Use `table.new_stream_write_builder()` instead of
  `table.new_batch_write_builder()`.
- `StreamTableWrite`, `StreamTableUpdate`, and `StreamTableCommit` are reusable
  across multiple rounds.
- Each round must use a monotonically increasing `commit_identifier`.
- Pass the same `commit_identifier` to the write prepare step or update method,
  and to the corresponding commit call for that round.

The API mapping is:

| Batch API | Stream API |
| --- | --- |
| `write.prepare_commit()` | `write.prepare_commit(commit_identifier)` |
| `update.update_by_arrow_with_row_id(table)` | `update.update_by_arrow_with_row_id(table, commit_identifier)` |
| `update.update_by_predicate(predicate, assignments)` | `update.update_by_predicate(predicate, assignments, commit_identifier)` |
| `update.delete_by_predicate(predicate)` | `update.delete_by_predicate(predicate, commit_identifier)` |
| `update.delete_by_row_id(row_ids)` | `update.delete_by_row_id(row_ids, commit_identifier)` |
| `update.upsert_by_arrow_with_key(table, keys)` | `update.upsert_by_arrow_with_key(table, keys, commit_identifier)` |
| `update.merge_into(source, on=..., when_matched=..., when_not_matched=...)` | `update.merge_into(source, on=..., when_matched=..., when_not_matched=..., commit_identifier=...)` |
| `commit.commit(messages)` | `commit.commit(messages, commit_identifier)` |

For shard updates, create the updater from `StreamTableUpdate` in the same way
as batch mode. `new_shard_updator(...)`, `arrow_reader()`,
`update_by_arrow_batch(...)`, and `prepare_commit()` stay the same; pass
`commit_identifier` when committing the returned messages.
