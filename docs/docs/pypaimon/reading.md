---
title: "Batch Reads"
description: "Build a scan, select the rows and columns you need, and consume its splits."
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

# Batch Reads

Build a scan, select the rows and columns you need, and consume its splits. These examples assume you have a `table` from [Catalogs and Tables](./catalogs). Use [Streaming Reads](./streaming) to keep polling for new snapshots.

## Batch Read

### Predicate pushdown

A `ReadBuilder` is used to build reading utils and perform filter and projection pushdown.

```python
table = catalog.get_table('database_name.table_name')
read_builder = table.new_read_builder()
```

You can use `PredicateBuilder` to build filters and pushdown them by `ReadBuilder`:

```python
# Example filter: ('f0' < 3 OR 'f1' > 6) AND 'f3' = 'A'

predicate_builder = read_builder.new_predicate_builder()

predicate1 = predicate_builder.less_than('f0', 3)
predicate2 = predicate_builder.greater_than('f1', 6)
predicate3 = predicate_builder.or_predicates([predicate1, predicate2])

predicate4 = predicate_builder.equal('f3', 'A')
predicate_5 = predicate_builder.and_predicates([predicate3, predicate4])

read_builder = read_builder.with_filter(predicate_5)
```

See [Predicate](./reading#predicate) for all supported filters and building methods. Filter by `_ROW_ID`: see [Data Evolution](./data-evolution#filter-by-_row_id).

You can also pushdown projection by `ReadBuilder`:

```python
# select f3 and f2 columns
read_builder = read_builder.with_projection(['f3', 'f2'])
```

For tables with nested struct columns, you can project individual sub-fields using dotted names:

```python
# Given a table with schema: id BIGINT, info ROW<name STRING, age INT>, val STRING

# Select a nested sub-field and a top-level field
read_builder = read_builder.with_projection(['info.name', 'val'])

# The result columns are flattened with underscore-joined names:
# info_name, val
```

Nested `ROW` projections are supported for ordinary append tables and primary-key
merge reads. The reader may read the containing `ROW` and extract the requested
leaf, so selecting a leaf does not guarantee physical leaf-only I/O.

Limitations:

- Data-evolution tables do not support nested projection.
- `ARRAY<ROW>` and `MAP` nested paths are not supported.

### Generate Splits

Then you can step into Scan Plan stage to get `splits`:

```python
table_scan = read_builder.new_scan()
splits = table_scan.plan().splits()
```

Finally, you can read data from the `splits` to various data format.

### Read Apache Arrow

This requires `pyarrow` to be installed.

You can read all the data into a `pyarrow.Table`:

```python
table_read = read_builder.new_read()
pa_table = table_read.to_arrow(splits)
print(pa_table)

# pyarrow.Table
# f0: int32
# f1: string
# ----
# f0: [[1,2,3],[4,5,6],...]
# f1: [["a","b","c"],["d","e","f"],...]
```

You can also read data into a `pyarrow.RecordBatchReader` and iterate record batches:

```python
table_read = read_builder.new_read()
for batch in table_read.to_arrow_batch_reader(splits):
    print(batch)

# pyarrow.RecordBatch
# f0: int32
# f1: string
# ----
# f0: [1,2,3]
# f1: ["a","b","c"]
```

### Read Python Iterator

You can read the data row by row into a native Python iterator.
This is convenient for custom row-based processing logic.

```python
table_read = read_builder.new_read()
for row in table_read.to_iterator(splits):
    print(row)

# [1,2,3]
# ["a","b","c"]
```

### Read Pandas

This requires `pandas` to be installed.

You can read all the data into a `pandas.DataFrame`:

```python
table_read = read_builder.new_read()
df = table_read.to_pandas(splits)
print(df)

#    f0 f1
# 0   1  a
# 1   2  b
# 2   3  c
# 3   4  d
# ...
```

### Read DuckDB

This requires `duckdb` to be installed.

You can convert the splits into an in-memory DuckDB table and query it:

```python
table_read = read_builder.new_read()
duckdb_con = table_read.to_duckdb(splits, 'duckdb_table')

print(duckdb_con.query("SELECT * FROM duckdb_table").fetchdf())
#    f0 f1
# 0   1  a
# 1   2  b
# 2   3  c
# 3   4  d
# ...

print(duckdb_con.query("SELECT * FROM duckdb_table WHERE f0 = 1").fetchdf())
#    f0 f1
# 0   1  a
```

### Incremental Read

This API allows reading data committed between two snapshot timestamps. The steps are as follows.

- Set the option `incremental-between-timestamp` on a copied table via `table.copy({...})`. The value must
  be a string: `"startMillis,endMillis"`, where `startMillis` is exclusive and `endMillis` is inclusive.
- Use `SnapshotManager` to obtain snapshot timestamps or you can determine them by yourself.
- Read the data as above.

Example:

```python
from pypaimon import CatalogFactory

# Prepare catalog and obtain a table
catalog = CatalogFactory.create({'warehouse': '/path/to/warehouse'})
table = catalog.get_table('default.your_table_name')

# Assume the table has at least two snapshots (1 and 2)
snapshot_manager = table.snapshot_manager()
t1 = snapshot_manager.get_snapshot_by_id(1).time_millis
t2 = snapshot_manager.get_snapshot_by_id(2).time_millis

# Read records committed in (t1, t2]
table_inc = table.copy({"incremental-between-timestamp": f"{t1},{t2}"})

read_builder = table_inc.new_read_builder()
table_scan = read_builder.new_scan()
table_read = read_builder.new_read()
splits = table_scan.plan().splits()

# To Arrow
arrow_table = table_read.to_arrow(splits)

# Or to pandas
pandas_df = table_read.to_pandas(splits)
```

### Shard Read

Shard Read allows you to read data in parallel by dividing the table into multiple shards. This is useful for
distributed processing and parallel computation.

You can specify the shard index and total number of shards to read a specific portion of the data:

```python
# Prepare read builder
table = catalog.get_table('database_name.table_name')
read_builder = table.new_read_builder()
table_read = read_builder.new_read()

# Read the second shard (index 1) out of 3 total shards
splits = read_builder.new_scan().with_shard(1, 3).plan().splits()

# Read all shards and concatenate results
splits1 = read_builder.new_scan().with_shard(0, 3).plan().splits()
splits2 = read_builder.new_scan().with_shard(1, 3).plan().splits()
splits3 = read_builder.new_scan().with_shard(2, 3).plan().splits()

# Combine results from all shards

all_splits = splits1 + splits2 + splits3
pa_table = table_read.to_arrow(all_splits)
```

Example with shard read:

```python
import pyarrow as pa
from pypaimon import CatalogFactory, Schema

# Create catalog
catalog_options = {'warehouse': 'file:///path/to/warehouse'}
catalog = CatalogFactory.create(catalog_options)
catalog.create_database("default", False)
# Define schema
pa_schema = pa.schema([
    ('user_id', pa.int64()),
    ('item_id', pa.int64()),
    ('behavior', pa.string()),
    ('dt', pa.string()),
])

# Create table and write data
schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=['dt'])
catalog.create_table('default.test_table', schema, False)
table = catalog.get_table('default.test_table')

# Write data in two batches
write_builder = table.new_batch_write_builder()

# First write
table_write = write_builder.new_write()
table_commit = write_builder.new_commit()
data1 = {
    'user_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
    'item_id': [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014],
    'behavior': ['a', 'b', 'c', None, 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm'],
    'dt': ['p1', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p1'],
}
pa_table = pa.Table.from_pydict(data1, schema=pa_schema)
table_write.write_arrow(pa_table)
table_commit.commit(table_write.prepare_commit())
table_write.close()
table_commit.close()

# Second write
table_write = write_builder.new_write()
table_commit = write_builder.new_commit()
data2 = {
    'user_id': [5, 6, 7, 8, 18],
    'item_id': [1005, 1006, 1007, 1008, 1018],
    'behavior': ['e', 'f', 'g', 'h', 'z'],
    'dt': ['p2', 'p1', 'p2', 'p2', 'p1'],
}
pa_table = pa.Table.from_pydict(data2, schema=pa_schema)
table_write.write_arrow(pa_table)
table_commit.commit(table_write.prepare_commit())
table_write.close()
table_commit.close()

# Read specific shard
read_builder = table.new_read_builder()
table_read = read_builder.new_read()

# Read shard 2 out of 3 total shards
splits = read_builder.new_scan().with_shard(2, 3).plan().splits()
shard_data = table_read.to_arrow(splits)

# Verify shard distribution by reading all shards
splits1 = read_builder.new_scan().with_shard(0, 3).plan().splits()
splits2 = read_builder.new_scan().with_shard(1, 3).plan().splits()
splits3 = read_builder.new_scan().with_shard(2, 3).plan().splits()

# Combine all shards should equal full table read
all_shards_data = pa.concat_tables([
    table_read.to_arrow(splits1),
    table_read.to_arrow(splits2),
    table_read.to_arrow(splits3),
])
full_table_data = table_read.to_arrow(read_builder.new_scan().plan().splits())
```

Key points about shard read:

- **Shard Index**: Zero-based index of the shard to read (0 to total_shards-1)
- **Total Shards**: Total number of shards to divide the data into
- **Data Distribution**: Data is distributed evenly across shards, with remainder rows going to the last shard
- **Parallel Processing**: Each shard can be processed independently for better performance
- **Consistency**: Combining all shards should produce the complete table data

### Explain Scan Plan

`ReadBuilder.explain()` returns a structured view of the scan plan without reading any data files. It is useful for understanding which splits a query will produce, how aggressively the pushdown pruned the input, and whether the resulting splits can be read on the zero-copy fast path.

```python
table = catalog.get_table('default.events')
read_builder = table.new_read_builder()
predicate_builder = read_builder.new_predicate_builder()
read_builder = read_builder.with_filter(predicate_builder.equal('dt', '2026-05-16'))
print(read_builder.explain())

# == PyPaimon Scan Plan ==
# Table:              default.events (PK, HASH_FIXED)
# Snapshot:           1  (schema 0)
# Predicate:          dt = '2026-05-16'
# Projection:         <all columns>
# Limit:              <none>
#
# Partition pruning:  12 -> 4  (pruned 8)
# Bucket pruning:     4 -> 4  (pruned 0)
# File skipping:      4 -> 4  (pruned 0)
#
# Splits:             4
#   raw-convertible:  4 / 4
#   with DV:          0 / 4
#   all-above-L0:     0 / 4
#   files/split:      min=1  max=1  avg=1.00
#   size/split:       min=2.8 KiB  p50=2.9 KiB  p95=3.0 KiB  max=3.0 KiB
#
# Files:              4
# Total size:         11.6 KiB
# Estimated rows:     20   (merged: 20)
# Level histogram:    L0=4
# Deletion files:     0
```

Pass `verbose=True` to also list every split with its partition, bucket, file count, size, level histogram, and file paths:

```python
print(read_builder.explain(verbose=True))

# ...
#
# Splits[]
#   [0] partition={'dt': '2026-05-16'} bucket=3 files=1 size=2.9 KiB rows=4 raw=True dv=False
#       levels: L0=1
#       file: /warehouse/default.db/events/dt=2026-05-16/bucket-3/data-...parquet
#   [1] partition={'dt': '2026-05-16'} bucket=2 files=1 size=2.8 KiB rows=2 raw=True dv=False
#       levels: L0=1
#       file: /warehouse/default.db/events/dt=2026-05-16/bucket-2/data-...parquet
#   ...
```

What the fields tell you:

- **Pushdown** (`Predicate` / `Projection` / `Limit`): exactly what the reader sees after `with_filter` / `with_projection` / `with_limit`.
- **Pruning funnel** (`Partition pruning` / `Bucket pruning` / `File skipping`): three `before -> after` counts that show at which stage the predicate paid off. `n/a` means the stage did not apply — for example, bucket pruning is reported for HASH_FIXED tables where every bucket key is pinned by the predicate, and for POSTPONE_BUCKET tables that skip their synthetic-bucket entries.
- **Split shape**: `raw-convertible` counts splits that can be read zero-copy (no merge, no deletion-vector apply); `with DV` counts splits whose files need a deletion vector applied; `all-above-L0` counts splits whose data lives entirely on L1+, i.e. the merge pipeline can skip the L0 buffer.
- **File aggregates**: total file size + estimated rows (with the post-merge row estimate for primary-key tables in parentheses), plus a level histogram of where the data sits.

:::info

**Cost**: `explain()` reads the manifest list and manifest files but does not open any data files. It suppresses the manifest-reader's early bucket filter and forces single-threaded manifest decoding so the before/after counters are accurate. On tables where the early filter usually prunes aggressively (e.g. very wide HASH_FIXED tables with a tight predicate), this can make `explain()` measurably slower than a regular `new_scan().plan()`.

:::

`ExplainResult` is a plain dataclass — alongside the human-readable `__str__` shown above, every field (`partition_pruning`, `bucket_pruning`, `file_skipping`, `split_count`, `splits_raw_convertible`, `level_histogram`, `splits`, ...) is addressable in Python for programmatic use.

#### CLI

The same scan plan is available from the `paimon` command line — useful for previewing pruning effects of a predicate without writing any Python:

```bash
# Whole-table scan
paimon -c paimon.yaml table explain default.events

# Push down filter / projection / limit and list every split
paimon -c paimon.yaml table explain default.events \
    --where "dt = '2026-05-16' AND id = 7" \
    --select dt,id,val \
    --limit 100 \
    --verbose

# Machine-readable output (level_histogram keys are JSON strings)
paimon -c paimon.yaml table explain default.events --format json
```

`--where` accepts the same SQL-like syntax as `paimon table read`. With `--format json`, the result is a structured dump of `ExplainResult` suitable for piping into `jq` or further processing.

## Predicate

| Predicate kind        | Predicate method                              |
|:----------------------|:----------------------------------------------|
| p1 and p2             | PredicateBuilder.and_predicates([p1, p2])     |
| p1 or p2              | PredicateBuilder.or_predicates([p1, p2])      |
| f = literal           | PredicateBuilder.equal(f, literal)            |
| f != literal          | PredicateBuilder.not_equal(f, literal)        |
| f \< literal           | PredicateBuilder.less_than(f, literal)        |
| f \<= literal          | PredicateBuilder.less_or_equal(f, literal)    |
| f > literal           | PredicateBuilder.greater_than(f, literal)     |
| f >= literal          | PredicateBuilder.greater_or_equal(f, literal) |
| f is null             | PredicateBuilder.is_null(f)                   |
| f is not null         | PredicateBuilder.is_not_null(f)               |
| f.startswith(literal) | PredicateBuilder.startswith(f, literal)       |
| f.endswith(literal)   | PredicateBuilder.endswith(f, literal)         |
| f.contains(literal)   | PredicateBuilder.contains(f, literal)         |
| f is in [l1, l2]      | PredicateBuilder.is_in(f, [l1, l2])           |
| f is not in [l1, l2]  | PredicateBuilder.is_not_in(f, [l1, l2])       |
| lower \<= f \<= upper   | PredicateBuilder.between(f, lower, upper)     |
