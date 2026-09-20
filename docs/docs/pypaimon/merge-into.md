---
title: "Upsert and Merge by Key"
description: "Match input rows to existing data-evolution rows using business keys."
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

# Upsert and Merge by Key

Match input rows to existing data-evolution rows using business keys. Choose upsert for update-or-insert behavior, or merge when you need ordered conditions and matched deletes. Both APIs return commit messages for an explicit commit.

| Operation | Matching behavior | Duplicate source keys |
| --- | --- | --- |
| `upsert_by_arrow_with_key` | Update matches; append new keys | Last input occurrence wins |
| `merge_into` | Evaluate matched and unmatched clauses | Multiple source rows matching one target row raise an error |

The complete examples below use separate tables. Run each example with a fresh
warehouse, or reuse the database with `ignore_if_exists=True` and choose a new
table name. See [Data Evolution](./data-evolution#prerequisites) for table options
and [Ray joins and merge](./ray-joins#merge-into) for distributed execution.

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
(`s.*`). Condition evaluation uses the PyPaimon DataFusion extra.
Python 3.10 or newer is required. Install it with
`pip install 'pypaimon[datafusion]'`.

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
