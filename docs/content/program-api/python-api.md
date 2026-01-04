---
title: "Python API"
weight: 5
type: docs
aliases:
  - /api/python-api.html
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

# Python API

PyPaimon is a Python implementation for connecting Paimon catalog, reading & writing tables. The complete Python
implementation of the brand new PyPaimon does not require JDK installation.

## Environment Settings

SDK is published at [pypaimon](https://pypi.org/project/pypaimon/). You can install by

```shell
pip install pypaimon
```

## Create Catalog

Before coming into contact with the Table, you need to create a Catalog.

{{< tabs "create-catalog" >}}
{{< tab "filesystem" >}}
```python
from pypaimon import CatalogFactory

# Note that keys and values are all string
catalog_options = {
    'warehouse': 'file:///path/to/warehouse'
}
catalog = CatalogFactory.create(catalog_options)
```
{{< /tab >}}
{{< tab "rest catalog" >}}
The sample code is as follows. The detailed meaning of option can be found in [DLF Token](../concepts/rest/dlf.md).

```python
from pypaimon import CatalogFactory

# Note that keys and values are all string
catalog_options = {
  'metastore': 'rest',
  'warehouse': 'xxx',
  'uri': 'xxx',
  'dlf.region': 'xxx',
  'token.provider': 'xxx',
  'dlf.access-key-id': 'xxx',
  'dlf.access-key-secret': 'xxx'
}
catalog = CatalogFactory.create(catalog_options)
```
{{< /tab >}}
{{< /tabs >}}

Currently, PyPaimon only support filesystem catalog and rest catalog. See [Catalog]({{< ref "concepts/catalog" >}}).

You can use the catalog to create table for writing data.

## Create Database

Table is located in a database. If you want to create table in a new database, you should create it.

```python
catalog.create_database(
    name='database_name',
    ignore_if_exists=True,  # To raise error if the database exists, set False
    properties={'key': 'value'}  # optional database properties
)
```

## Create Table

Table schema contains fields definition, partition keys, primary keys, table options and comment.
The field definition is described by `pyarrow.Schema`. All arguments except fields definition are optional.

Generally, there are two ways to build `pyarrow.Schema`.

First, you can use `pyarrow.schema` method directly, for example:

```python
import pyarrow as pa

from pypaimon import Schema

pa_schema = pa.schema([
    ('dt', pa.string()),
    ('hh', pa.string()),
    ('pk', pa.int64()),
    ('value', pa.string())
])

schema = Schema.from_pyarrow_schema(
    pa_schema=pa_schema,
    partition_keys=['dt', 'hh'],
    primary_keys=['dt', 'hh', 'pk'],
    options={'bucket': '2'},
    comment='my test table')
```

See [Data Types]({{< ref "python-api#data-types" >}}) for all supported `pyarrow-to-paimon` data types mapping.

Second, if you have some Pandas data, the `pa_schema` can be extracted from `DataFrame`:

```python
import pandas as pd
import pyarrow as pa

from pypaimon import Schema

# Example DataFrame data
data = {
    'dt': ['2024-01-01', '2024-01-01', '2024-01-02'],
    'hh': ['12', '15', '20'],
    'pk': [1, 2, 3],
    'value': ['a', 'b', 'c'],
}
dataframe = pd.DataFrame(data)

# Get Paimon Schema
record_batch = pa.RecordBatch.from_pandas(dataframe)
schema = Schema.from_pyarrow_schema(
    pa_schema=record_batch.schema,
    partition_keys=['dt', 'hh'],
    primary_keys=['dt', 'hh', 'pk'],
    options={'bucket': '2'},
    comment='my test table'
)
```

After building table schema, you can create corresponding table:

```python
schema = ...
catalog.create_table(
    identifier='database_name.table_name',
    schema=schema,
    ignore_if_exists=True  # To raise error if the table exists, set False
)

# Get Table
table = catalog.get_table('database_name.table_name')
```

## Batch Write

Paimon table write is Two-Phase Commit, you can write many times, but once committed, no more data can be written.

{{< hint warning >}}
Currently, the feature of writing multiple times and committing once only supports append only table.
{{< /hint >}}

```python
table = catalog.get_table('database_name.table_name')

# 1. Create table write and commit
write_builder = table.new_batch_write_builder()
table_write = write_builder.new_write()
table_commit = write_builder.new_commit()

# 2. Write data. Support 3 methods:
# 2.1 Write pandas.DataFrame
dataframe = ...
table_write.write_pandas(dataframe)

# 2.2 Write pyarrow.Table
pa_table = ...
table_write.write_arrow(pa_table)

# 2.3 Write pyarrow.RecordBatch
record_batch = ...
table_write.write_arrow_batch(record_batch)

# 3. Commit data
commit_messages = table_write.prepare_commit()
table_commit.commit(commit_messages)

# 4. Close resources
table_write.close()
table_commit.close()
```

By default, the data will be appended to table. If you want to overwrite table, you should use `TableWrite#overwrite`
API:

```python
# overwrite whole table
write_builder = table.new_batch_write_builder().overwrite()

# overwrite partition 'dt=2024-01-01'
write_builder = table.new_batch_write_builder().overwrite({'dt': '2024-01-01'})
```

### Update columns

You can create `TableUpdate.update_by_arrow_with_row_id` to update columns to data evolution tables.

The input data should include the `_ROW_ID` column, update operation will automatically sort and match each `_ROW_ID` to
its corresponding `first_row_id`, then groups rows with the same `first_row_id` and writes them to a separate file.

```python
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

See [Predicate]({{< ref "python-api#predicate" >}}) for all supported filters and building methods.

You can also pushdown projection by `ReadBuilder`:

```python
# select f3 and f2 columns
read_builder = read_builder.with_projection(['f3', 'f2'])
```

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

### Read Ray

This requires `ray` to be installed.

You can convert the splits into a Ray Dataset and handle it by Ray Data API for distributed processing:

```python
table_read = read_builder.new_read()
ray_dataset = table_read.to_ray(splits)

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

The `to_ray()` method supports a `parallelism` parameter to control distributed reading. Use `parallelism=1` for single-task read (default) or `parallelism > 1` for distributed read with multiple Ray workers:

```python
# Simple mode (single task)
ray_dataset = table_read.to_ray(splits, parallelism=1)

# Distributed mode with 4 parallel tasks
ray_dataset = table_read.to_ray(splits, parallelism=4)

# Use Ray Data operations
mapped_dataset = ray_dataset.map(lambda row: {'value': row['value'] * 2})
filtered_dataset = ray_dataset.filter(lambda row: row['score'] > 80)
df = ray_dataset.to_pandas()
```

### Incremental Read

This API allows reading data committed between two snapshot timestamps. The steps are as follows.

- Set the option `CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP` on a copied table via `table.copy({...})`. The value must
  be a string: `"startMillis,endMillis"`, where `startMillis` is exclusive and `endMillis` is inclusive.
- Use `SnapshotManager` to obtain snapshot timestamps or you can determine them by yourself.
- Read the data as above.

Example:

```python
from pypaimon import CatalogFactory
from pypaimon.common.core_options import CoreOptions
from pypaimon.snapshot.snapshot_manager import SnapshotManager

# Prepare catalog and obtain a table
catalog = CatalogFactory.create({'warehouse': '/path/to/warehouse'})
table = catalog.get_table('default.your_table_name')

# Assume the table has at least two snapshots (1 and 2)
snapshot_manager = SnapshotManager(table)
t1 = snapshot_manager.get_snapshot_by_id(1).time_millis
t2 = snapshot_manager.get_snapshot_by_id(2).time_millis

# Read records committed between [t1, t2]
table_inc = table.copy({CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP: f"{t1},{t2}"})

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

## Data Types

| Python Native Type  | PyArrow Type                                     | Paimon Type                       |
|:--------------------|:-------------------------------------------------|:----------------------------------|
| `int`               | `pyarrow.int8()`                                 | `TINYINT`                         |
| `int`               | `pyarrow.int16()`                                | `SMALLINT`                        |
| `int`               | `pyarrow.int32()`                                | `INT`                             |
| `int`               | `pyarrow.int64()`                                | `BIGINT`                          |
| `float`             | `pyarrow.float32()`                              | `FLOAT`                           |
| `float`             | `pyarrow.float64()`                              | `DOUBLE`                          |
| `bool`              | `pyarrow.bool_()`                                | `BOOLEAN`                         |
| `str`               | `pyarrow.string()`                               | `STRING`, `CHAR(n)`, `VARCHAR(n)` |
| `bytes`             | `pyarrow.binary()`                               | `BYTES`, `VARBINARY(n)`           |
| `bytes`             | `pyarrow.binary(length)`                         | `BINARY(length)`                  |
| `decimal.Decimal`   | `pyarrow.decimal128(precision, scale)`           | `DECIMAL(precision, scale)`       |
| `datetime.datetime` | `pyarrow.timestamp(unit, tz=None)`               | `TIMESTAMP(p)`                    |
| `datetime.date`     | `pyarrow.date32()`                               | `DATE`                            |
| `datetime.time`     | `pyarrow.time32(unit)` or `pyarrow.time64(unit)` | `TIME(p)`                         |

## Predicate

| Predicate kind        | Predicate method                              | 
|:----------------------|:----------------------------------------------|
| p1 and p2             | PredicateBuilder.and_predicates([p1, p2])     |
| p1 or p2              | PredicateBuilder.or_predicates([p1, p2])      |
| f = literal           | PredicateBuilder.equal(f, literal)            |
| f != literal          | PredicateBuilder.not_equal(f, literal)        |
| f < literal           | PredicateBuilder.less_than(f, literal)        |
| f <= literal          | PredicateBuilder.less_or_equal(f, literal)    |
| f > literal           | PredicateBuilder.greater_than(f, literal)     |
| f >= literal          | PredicateBuilder.greater_or_equal(f, literal) |
| f is null             | PredicateBuilder.is_null(f)                   |
| f is not null         | PredicateBuilder.is_not_null(f)               |
| f.startswith(literal) | PredicateBuilder.startswith(f, literal)       |
| f.endswith(literal)   | PredicateBuilder.endswith(f, literal)         |
| f.contains(literal)   | PredicateBuilder.contains(f, literal)         |
| f is in [l1, l2]      | PredicateBuilder.is_in(f, [l1, l2])           |
| f is not in [l1, l2]  | PredicateBuilder.is_not_in(f, [l1, l2])       |
| lower <= f <= upper   | PredicateBuilder.between(f, lower, upper)     |

## Supported Features

The following shows the supported features of Python Paimon compared to Java Paimon:

**Catalog Level**
   - FileSystemCatalog
   - RestCatalog

**Table Level**
   - Append Tables
     - `bucket = -1` (unaware)
     - `bucket > 0` (fixed)
   - Primary Key Tables
     - only support deduplicate
     - `bucket = -2` (postpone)
     - `bucket > 0` (fixed)
     - read with deletion vectors enabled
   - Read/Write Operations
     - Batch read and write for append tables and primary key tables
     - Predicate filtering
     - Overwrite semantics
     - Incremental reading of Delta data
     - Reading and writing blob data
     - `with_shard` feature
