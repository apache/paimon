---
title: "Data Evolution"
weight: 5
type: docs
aliases:
  - /pypaimon/data-evolution.html
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

PyPaimon for Data Evolution mode. See [Data Evolution]({{< ref "append-table/data-evolution" >}}).

## Prerequisites

To use partial updates / data evolution, enable both options when creating the table:

- **`row-tracking.enabled`**: `true`
- **`data-evolution.enabled`**: `true`

## Update Columns By Row ID

You can create `TableUpdate.update_by_arrow_with_row_id` to update columns to data evolution tables.

The input data should include the `_ROW_ID` column, update operation will automatically sort and match each `_ROW_ID` to
its corresponding `first_row_id`, then groups rows with the same `first_row_id` and writes them to a separate file.

**Requirements for `_ROW_ID` updates**

- **All rows required**: the input table must contain **exactly the full table row count** (one row per existing row).
- **Row id coverage**: after sorting by `_ROW_ID`, it must be **0..N-1** (no duplicates, no gaps).
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

## Filter by _ROW_ID

On data evolution tables you can filter by `_ROW_ID` to prune files at scan time. Supported: `equal('_ROW_ID', id)`, `is_in('_ROW_ID', [id1, ...])`, `between('_ROW_ID', low, high)`.

```python
pb = table.new_read_builder().new_predicate_builder()
rb = table.new_read_builder().with_filter(pb.equal('_ROW_ID', 0))
result = rb.new_read().to_arrow(rb.new_scan().plan().splits())
```

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
