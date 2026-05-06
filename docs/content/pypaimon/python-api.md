---
title: "Python API"
weight: 2
type: docs
aliases:
  - /pypaimon/python-api.html
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

For an S3 warehouse, pass S3 authentication options with the filesystem catalog options:

```python
from pypaimon import CatalogFactory

catalog_options = {
    'warehouse': 's3://bucket/path/to/warehouse',
    's3.endpoint': 'https://s3.amazonaws.com',
    's3.access-key': 'xxx',
    's3.secret-key': 'yyy',
    # Optional. Required for temporary credentials.
    's3.session-token': 'zzz',
    # Optional. Useful for S3 compatible stores such as MinIO.
    's3.path-style-access': 'true',
}
catalog = CatalogFactory.create(catalog_options)
```

For an HDFS warehouse with Kerberos authentication:

```python
from pypaimon import CatalogFactory

catalog_options = {
    'warehouse': 'hdfs://namenode:8020/path/to/warehouse',
    # Keytab mode: automatic kinit
    'security.kerberos.login.principal': 'user@YOUR.REALM',
    'security.kerberos.login.keytab': '/path/to/user.keytab',
}
catalog = CatalogFactory.create(catalog_options)
```

If you have already run `kinit` externally, you can omit `principal` and `keytab`.
PyPaimon will automatically pick up the ticket from `KRB5CCNAME` environment variable
or the default `/tmp/krb5cc_<uid>`:

```python
from pypaimon import CatalogFactory

catalog_options = {
    'warehouse': 'hdfs://namenode:8020/path/to/warehouse',
    # Ticket cache mode: uses existing Kerberos ticket
}
catalog = CatalogFactory.create(catalog_options)
```

To disable ticket cache auto-detection and force SIMPLE authentication, set:

```python
catalog_options = {
    'warehouse': 'hdfs://namenode:8020/path/to/warehouse',
    'security.kerberos.login.use-ticket-cache': 'false',
}
```

{{< /tab >}}
{{< tab "rest catalog" >}}
The sample code is as follows. The detailed meaning of option can be found in [REST]({{< ref "concepts/rest/overview" >}}).

```python
from pypaimon import CatalogFactory

# Note that keys and values are all string
catalog_options = {
  'metastore': 'rest',
  'warehouse': 'xxx',
  'uri': 'xxx',
  'token.provider': 'xxx'
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

## Alter Table

Alter a table with a list of schema changes. Use `SchemaChange` from `pypaimon.schema.schema_change` and types from `pypaimon.schema.data_types` (e.g. `AtomicType`).

```python
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.schema.data_types import AtomicType

# Add column(s)
catalog.alter_table(
    'database_name.table_name',
    [
        SchemaChange.add_column('new_col', AtomicType('STRING')),
        SchemaChange.add_column('score', AtomicType('DOUBLE'), comment='optional'),
    ],
    ignore_if_not_exists=False
)

# Drop column
catalog.alter_table(
    'database_name.table_name',
    [SchemaChange.drop_column('col_name')],
    ignore_if_not_exists=False
)
```

Other supported changes: `SchemaChange.rename_column`, `update_column_type`, `update_column_comment`, `set_option`, `remove_option`, `update_comment`.

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

# 2. Write data. Support 4 methods:
# 2.1 Write pandas.DataFrame
dataframe = ...
table_write.write_pandas(dataframe)

# 2.2 Write pyarrow.Table
pa_table = ...
table_write.write_arrow(pa_table)

# 2.3 Write pyarrow.RecordBatch
record_batch = ...
table_write.write_arrow_batch(record_batch)

# 3. Commit data (required for write_pandas/write_arrow/write_arrow_batch only)
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

See [Predicate]({{< ref "python-api#predicate" >}}) for all supported filters and building methods. Filter by `_ROW_ID`: see [Data Evolution]({{< ref "pypaimon/data-evolution#filter-by-_row_id" >}}).

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

### Snapshot Query

You can inspect table snapshots through `SnapshotManager`:

```python
snapshot_manager = table.snapshot_manager()

latest = snapshot_manager.get_latest_snapshot()
earliest = snapshot_manager.try_get_earliest_snapshot()
snapshot_10 = snapshot_manager.get_snapshot_by_id(10)
snapshots = snapshot_manager.list_snapshots()
snapshot_at_time = snapshot_manager.earlier_or_equal_time_mills(1700000000000)
```

The CLI also supports querying snapshots:

```shell
paimon -c catalog.yaml table snapshot default.my_table
paimon -c catalog.yaml table snapshot default.my_table --id 10
paimon -c catalog.yaml table snapshot default.my_table --earliest
paimon -c catalog.yaml table snapshot default.my_table --time-millis 1700000000000
paimon -c catalog.yaml table snapshot default.my_table --all
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

## Rollback

Paimon supports rolling back a table to a previous snapshot or tag. This is useful for undoing unwanted changes or
restoring the table to a known good state.

### Rollback to Snapshot

You can rollback a table to a specific snapshot by its ID:

```python
table = catalog.get_table('database_name.table_name')

# Rollback to snapshot 3
table.rollback_to(3)  # snapshot id
```

### Rollback to Tag

You can also rollback a table to a previously created tag:

```python
table = catalog.get_table('database_name.table_name')

# Rollback to tag 'v3'
table.rollback_to('v3')  # tag name
```

The `rollback_to` method accepts either an `int` (snapshot ID) or a `str` (tag name) and automatically dispatches
to the appropriate rollback logic.

## Maintenance

PyPaimon supports native maintenance helpers for rewriting current table data.

### Compact

Use `compact` to rewrite the current visible records and commit a `COMPACT` snapshot. You can compact the whole table
or a specific partition:

```python
table = catalog.get_table('database_name.table_name')
result = table.new_maintenance().compact({'dt': '2024-01-01'})

print(result.snapshot_id)
print(result.rewritten_record_count)
print(result.rewritten_file_count)
```

You can also trigger compaction from the CLI:

```shell
paimon -c catalog.yaml table compact default.my_table --partition dt=2024-01-01
```

### Rescale Bucket

Use `rescale_bucket` to rewrite data with a new bucket number. For partitioned tables, specify the target partition:

```python
table = catalog.get_table('database_name.table_name')
result = table.new_maintenance().rescale_bucket(
    bucket_num=4,
    partition={'dt': '2024-01-01'}
)
```

The same operation is available from the CLI:

```shell
paimon -c catalog.yaml table rescale default.my_table --bucket-num 4 --partition dt=2024-01-01
```

Native Python maintenance does not yet support row tracking, data evolution, or deletion vectors.

## Streaming Read

Streaming reads allow you to continuously read new data as it arrives in a Paimon table. This is useful for building
real-time data pipelines and ETL jobs.

### Basic Streaming Read

Use `StreamReadBuilder` to create a streaming scan that continuously polls for new snapshots:

```python
table = catalog.get_table('database_name.table_name')

# Create streaming read builder
stream_builder = table.new_stream_read_builder()
stream_builder.with_poll_interval_ms(1000)  # Poll every 1 second

# Create streaming scan and table read
scan = stream_builder.new_streaming_scan()
table_read = stream_builder.new_read()

# Async streaming (recommended for ETL pipelines)
import asyncio

async def process_stream():
    async for plan in scan.stream():
        for split in plan.splits():
            arrow_batch = table_read.to_arrow([split])
            # Process the data
            print(f"Received {arrow_batch.num_rows} rows")

asyncio.run(process_stream())
```

### Synchronous Streaming

For simpler use cases, you can use the synchronous wrapper:

```python
# Synchronous streaming
for plan in scan.stream_sync():
    arrow_table = table_read.to_arrow(plan.splits())
    process(arrow_table)
```

### Manual Position Control

You can directly read and set the scan position via `next_snapshot_id`:

```python
# Save current position
saved_position = scan.next_snapshot_id

# Later, restore position
scan.next_snapshot_id = saved_position

# Or start from a specific snapshot
scan.next_snapshot_id = 42
```

### Filtering Streaming Data

You can apply predicates and projections to streaming reads:

```python
stream_builder = table.new_stream_read_builder()

# Build predicate
predicate_builder = stream_builder.new_predicate_builder()
predicate = predicate_builder.greater_than('timestamp', 1704067200000)

# Apply filter and projection
stream_builder.with_filter(predicate)
stream_builder.with_projection(['id', 'name', 'timestamp'])

scan = stream_builder.new_streaming_scan()
```

Key points about streaming reads:

- **Poll Interval**: Controls how often to check for new snapshots (default: 1000ms)
- **Initial Scan**: First iteration returns all existing data, subsequent iterations return only new data
- **Commit Types**: By default, only APPEND commits are processed; COMPACT and OVERWRITE are skipped

### Parallel Consumption

For high-throughput streaming, you can run multiple consumers in parallel, each reading a disjoint subset of buckets.
This is similar to Kafka consumer groups.

**Using `with_buckets()` for explicit bucket assignment**:

```python
# Consumer 0 reads buckets 0, 1, 2
stream_builder.with_buckets([0, 1, 2])

# Consumer 1 reads buckets 3, 4, 5
stream_builder.with_buckets([3, 4, 5])
```

**Using `with_bucket_filter()` for custom filtering**:

```python
# Read only even buckets
stream_builder.with_bucket_filter(lambda b: b % 2 == 0)
```

### Row Kind Support

For changelog streams, you can include the row kind to distinguish between inserts, updates, and deletes:

```python
stream_builder = table.new_stream_read_builder()
stream_builder.with_include_row_kind(True)

scan = stream_builder.new_streaming_scan()
table_read = stream_builder.new_read()

async for plan in scan.stream():
    arrow_table = table_read.to_arrow(plan.splits())
    for row in arrow_table.to_pylist():
        row_kind = row['_row_kind']  # +I, -U, +U, or -D
        if row_kind == '+I':
            handle_insert(row)
        elif row_kind == '-D':
            handle_delete(row)
        elif row_kind in ('-U', '+U'):
            handle_update(row)
```

Row kind values:
- `+I`: Insert
- `-U`: Update before (old value)
- `+U`: Update after (new value)
- `-D`: Delete

## Data Types

### Scalar Types

| Python Native Type  | PyArrow Type                           | Paimon Type                       |
|:--------------------|:---------------------------------------|:----------------------------------|
| `int`               | `pyarrow.int8()`                       | `TINYINT`                         |
| `int`               | `pyarrow.int16()`                      | `SMALLINT`                        |
| `int`               | `pyarrow.int32()`                      | `INT`                             |
| `int`               | `pyarrow.int64()`                      | `BIGINT`                          |
| `float`             | `pyarrow.float32()`                    | `FLOAT`                           |
| `float`             | `pyarrow.float64()`                    | `DOUBLE`                          |
| `bool`              | `pyarrow.bool_()`                      | `BOOLEAN`                         |
| `str`               | `pyarrow.string()`                     | `STRING`, `CHAR(n)`, `VARCHAR(n)` |
| `bytes`             | `pyarrow.binary()`                     | `BYTES`, `VARBINARY(n)`           |
| `bytes`             | `pyarrow.binary(length)`               | `BINARY(length)`                  |
| `bytes`             | `pyarrow.large_binary()`               | `BLOB`                            |
| `decimal.Decimal`   | `pyarrow.decimal128(precision, scale)` | `DECIMAL(precision, scale)`       |
| `datetime.datetime` | `pyarrow.timestamp(unit, tz=None)`     | `TIMESTAMP(p)` — unit: `'s'` p=0, `'ms'` p=1–3, `'us'` p=4–6, `'ns'` p=7–9 |
| `datetime.datetime` | `pyarrow.timestamp(unit, tz='UTC')`    | `TIMESTAMP_LTZ(p)` — same unit/p mapping as above                             |
| `datetime.date`     | `pyarrow.date32()`                     | `DATE`                            |
| `datetime.time`     | `pyarrow.time32('ms')`                 | `TIME(p)`                         |

### Complex Types

| Python Native Type | PyArrow Type                          | Paimon Type            |
|:-------------------|:--------------------------------------|:-----------------------|
| `list`             | `pyarrow.list_(element_type)`         | `ARRAY<element_type>`  |
| `dict`             | `pyarrow.map_(key_type, value_type)`  | `MAP<key, value>`      |
| `dict`             | `pyarrow.struct([field, ...])`        | `ROW<field ...>`       |

### VARIANT Type

`VARIANT` stores semi-structured, schema-flexible data (JSON objects, arrays, and primitives)
in the [Parquet Variant binary encoding](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md).

pypaimon exposes VARIANT columns as Arrow `struct<value: binary NOT NULL, metadata: binary NOT NULL>` and
provides `GenericVariant` for encoding and decoding.

Paimon supports two Parquet storage layouts for VARIANT:

- **Plain VARIANT** — the standard two-field struct (`value` + `metadata`). Default for all writes.
- **Shredded VARIANT** — typed sub-columns are stored alongside overflow bytes, enabling column-skipping
  inside the Parquet file. Controlled by the `variant.shreddingSchema` table option.

{{< tabs "variant-read-write" >}}
{{< tab "Plain VARIANT" >}}

**Read**

A VARIANT column arrives as `struct<value: binary, metadata: binary>` in every Arrow batch.
Use `GenericVariant.from_arrow_struct` to decode each row:

```python
from pypaimon.data.generic_variant import GenericVariant

read_builder = table.new_read_builder()
result = read_builder.new_read().to_arrow(read_builder.new_scan().plan().splits())

for record in result.to_pylist():
    if (payload := record["payload"]) is not None:
        gv = GenericVariant.from_arrow_struct(payload)
        print(gv.to_python())   # decode to Python dict / list / scalar
```

`from_arrow_struct` is a lightweight operation — it only wraps the two raw byte arrays without
parsing them. Actual variant binary decoding is deferred to `to_python()`.

**Write**

Build `GenericVariant` values and convert them to an Arrow column with `to_arrow_array`:

```python
import pyarrow as pa
from pypaimon.data.generic_variant import GenericVariant

gv1 = GenericVariant.from_python({'city': 'Beijing', 'age': 30})
gv2 = GenericVariant.from_python({'tags': [1, 2, 3], 'active': True})
# None represents SQL NULL

data = pa.table({
    'id':      pa.array([1, 2, 3], type=pa.int32()),
    'payload': GenericVariant.to_arrow_array([gv1, gv2, None]),
})

write_builder = table.new_batch_write_builder()
table_write = write_builder.new_write()
table_commit = write_builder.new_commit()
table_write.write_arrow(data)
table_commit.commit(table_write.prepare_commit())
table_write.close()
table_commit.close()
```

{{< /tab >}}
{{< tab "Shredded VARIANT" >}}

In shredded mode the VARIANT column is physically split inside Parquet into a three-field group:

```
payload (GROUP)
├── metadata   BYTE_ARRAY          -- key dictionary (always present)
├── value      BYTE_ARRAY OPTIONAL -- overflow bytes for un-shredded fields
└── typed_value (GROUP) OPTIONAL
    ├── age    (GROUP)
    │   ├── value        BYTE_ARRAY OPTIONAL
    │   └── typed_value  INT64 OPTIONAL
    └── city   (GROUP)
        ├── value        BYTE_ARRAY OPTIONAL
        └── typed_value  BYTE_ARRAY OPTIONAL
```

**Read — automatic reassembly**

When pypaimon reads a Parquet file that contains shredded VARIANT columns (whether written by Paimon Java
or by pypaimon with shredding enabled), it **automatically detects and reassembles** them back to the
standard `struct<value, metadata>` form before returning any batch. No code changes are needed on the
read side:

```python
from pypaimon.data.generic_variant import GenericVariant

# Works identically for both shredded and plain Parquet files
read_builder = table.new_read_builder()
result = read_builder.new_read().to_arrow(read_builder.new_scan().plan().splits())

for record in result.to_pylist():
    if (payload := record["payload"]) is not None:
        gv = GenericVariant.from_arrow_struct(payload)   # same API as plain VARIANT
        print(gv.to_python())
```

Reassembly (reconstructing the variant binary from `typed_value` sub-columns and overflow bytes)
happens inside `FormatPyArrowReader.read_arrow_batch()` — that is, **at batch read time**, before
the Arrow data is returned to the caller. Note: When sub-field projection is active
(`with_variant_sub_fields`), reassembly is skipped entirely and only the requested typed
sub-columns are decoded.

**Write — shredding mode**

Set the `variant.shreddingSchema` table option to a JSON-encoded `ROW` type that describes which
sub-fields of which VARIANT columns to shred. The top-level fields map VARIANT column names to their
sub-field schemas:

```python
import json

shredding_schema = json.dumps({
    "type": "ROW",
    "fields": [
        {
            "id": 0,
            "name": "payload",          # VARIANT column name in the table
            "type": {
                "type": "ROW",
                "fields": [             # sub-fields to extract as typed columns
                    {"id": 0, "name": "age",  "type": "BIGINT"},
                    {"id": 1, "name": "city", "type": "VARCHAR"},
                ]
            }
        }
    ]
})

# Pass the option when creating the table
schema = Schema.from_pyarrow_schema(
    pa_schema,
    options={'variant.shreddingSchema': shredding_schema}
)
catalog.create_table('db.events', schema, ignore_if_exists=True)
```

Once the option is set, each `write_arrow` call transparently converts VARIANT columns to the shredded
Parquet layout. The read path — including Java Paimon and other engines — can then exploit the typed
sub-columns for column-skipping via sub-field projection.

Fields not listed in `variant.shreddingSchema` are stored in the overflow `value` bytes and remain
fully accessible on the read path.

Supported Paimon type strings for shredded sub-fields: `BOOLEAN`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE`,
`VARCHAR`, `DECIMAL(p,s)`, and nested `ROW` types for recursive object shredding.

{{< /tab >}}
{{< /tabs >}}


**`GenericVariant` API:**

| Method | Description |
|:-------|:------------|
| `GenericVariant.from_python(obj)` | Build from a Python object (`dict`, `list`, `int`, `str`, …) |
| `GenericVariant.from_arrow_struct({"value": b"...", "metadata": b"..."})` | Wrap raw bytes from an Arrow VARIANT struct row (read path) |
| `GenericVariant.to_arrow_array([gv1, gv2, None, ...])` | Convert a list of `GenericVariant` (or `None`) to a `pa.StructArray` for writing |
| `gv.to_python()` | Decode to native Python (`dict`, `list`, `int`, `str`, `None`, …) |
| `gv.value()` | Return raw value bytes |
| `gv.metadata()` | Return raw metadata bytes |

**Limitations:**

- `VARIANT` is only supported with Parquet file format. ORC and Avro are not supported.
- `VARIANT` cannot be used as a primary key or partition key.

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

## Consumer Management

Consumer management allows you to track consumption progress, prevent snapshot expiration, and resume from breakpoints.

### Create ConsumerManager

```python
from pypaimon import CatalogFactory

# Get table and file_io
catalog = CatalogFactory.create({'warehouse': 'file:///path/to/warehouse'})
table = catalog.get_table('database_name.table_name')
file_io = table.file_io()

# Create consumer manager
manager = table.consumer_manager()
```

### Get Consumer

Retrieve a consumer by its ID:

```python
from pypaimon.consumer.consumer import Consumer

consumer = manager.consumer('consumer_id')
if consumer:
    print(f"Next snapshot: {consumer.next_snapshot}")
else:
    print("Consumer not found")
```

### Reset Consumer

Create or reset a consumer with a new snapshot ID:

```python
# Reset consumer to snapshot 10
manager.reset_consumer('consumer_id', Consumer(next_snapshot=10))
```

### Delete Consumer

Delete a consumer by its ID:

```python
manager.delete_consumer('consumer_id')
```

### List Consumers

Get all consumers with their next snapshot IDs:

```python
consumers = manager.consumers()
for consumer_id, next_snapshot in consumers.items():
    print(f"Consumer {consumer_id}: next snapshot {next_snapshot}")
```

### List All Consumer IDs

List all consumer IDs:

```python
consumer_ids = manager.list_all_ids()
for consumer_id in consumer_ids:
    print(consumer_id)
```

### Get Minimum Next Snapshot

Get the minimum next snapshot across all consumers:

```python
min_snapshot = manager.min_next_snapshot()
if min_snapshot:
    print(f"Minimum next snapshot: {min_snapshot}")
```

### Expire Consumers

Expire consumers modified before a given datetime:

```python
from datetime import datetime, timedelta

# Expire consumers older than 1 day
expire_time = datetime.now() - timedelta(days=1)
manager.expire(expire_time)
```

### Clear Consumers

Clear consumers matching regular expression patterns:

```python
# Clear all consumers starting with "test_"
manager.clear_consumers('test_.*')

# Clear all consumers except those starting with "prod_"
manager.clear_consumers(
    '.*',
    'prod_.*'
)
```

### Branch Support

ConsumerManager supports multiple branches:

```python
# Custom branch
branch_manager = manager.with_branch('feature_branch')

# Each branch maintains its own consumers
print(branch_manager.consumers())  # Consumers on feature branch
```

## Branch Management

Branch management allows you to create multiple versions of a table, enabling parallel development and experimentation. Paimon supports creating branches from the current state or from specific tags.

{{< hint info >}}
PyPaimon provides two implementations of `BranchManager`:
- **FileSystemBranchManager**: For tables accessed directly via filesystem (default for filesystem catalog)
- **CatalogBranchManager**: For tables accessed via catalog (e.g., REST catalog)

The `table.branch_manager()` method automatically returns the appropriate implementation based on the table's catalog environment.
{{< /hint >}}

### Create Branch

Create a new branch from the current table state:

```python
from pypaimon import CatalogFactory

catalog = CatalogFactory.create({'warehouse': 'file:///path/to/warehouse'})
table = catalog.get_table('database_name.table_name')

# Create a branch from current state
table.branch_manager().create_branch('feature_branch')
```

Create a branch from a specific tag:

```python
# Create a branch from tag 'v1.0'
table.branch_manager().create_branch('feature_branch', tag_name='v1.0')
```

Create a branch and ignore if it already exists:

```python
# No error if branch already exists
table.branch_manager().create_branch('feature_branch', ignore_if_exists=True)
```

### List Branches

List all branches for a table:

```python
# Get all branch names
branches = table.branch_manager().branches()

for branch in branches:
    print(f"Branch: {branch}")
```

### Check Branch Exists

Check if a specific branch exists:

```python
if table.branch_manager().branch_exists('feature_branch'):
    print("Branch exists")
else:
    print("Branch does not exist")
```

### Drop Branch

Delete an existing branch:

```python
# Drop a branch
table.branch_manager().drop_branch('feature_branch')
```

### Rename Branch

Rename an existing branch to a new name:

```python
# Rename a branch
table.branch_manager().rename_branch('old_branch_name', 'new_branch_name')
```

{{< hint warning >}}
The source branch must exist and cannot be the main branch. The target branch name must be valid and not already exist.
{{< /hint >}}

### Fast Forward

Fast forward the main branch to a specific branch:

```python
# Fast forward main to feature branch
# This is useful when you want to merge changes from a feature branch back to main
table.branch_manager().fast_forward('feature_branch')
```

{{< hint warning >}}
Fast forward operation is irreversible and will replace the current state of the main branch with the target branch's state.
{{< /hint >}}

### Branch Path Structure

Paimon organizes branches in the file system as follows:

- **Main branch**: Stored directly in the table directory (e.g., `/path/to/table/`)
- **Feature branches**: Stored in a `branch` subdirectory (e.g., `/path/to/table/branch/branch-feature_branch/`)

### Branch Name Validation

Branch names have the following constraints:

- Cannot be "main" (the default branch)
- Cannot be blank or whitespace only
- Cannot be a pure numeric string
- Valid examples: `feature`, `develop`, `feature-123`, `my-branch`

## Supported Features

The following shows the supported features of Python Paimon compared to Java Paimon:

**Catalog Level**

- FileSystemCatalog
- RestCatalog

**Filesystem & Security**

- Local filesystem (`file://`)
- HDFS (`hdfs://`) with SIMPLE authentication
- HDFS (`hdfs://`) with Kerberos authentication (keytab or ticket cache)
- Aliyun OSS (`oss://`)
- S3 (`s3://`)

**Table Level**

- Append Tables
    - `bucket = -1` (unaware)
    - `bucket > 0` (fixed)
- Primary Key Tables
    - only support deduplicate
    - `bucket = -2` (postpone)
    - `bucket > 0` (fixed)
    - read with deletion vectors enabled
- Format Tables
    - PARQUET
    - CSV
    - JSON
    - ORC
    - TEXT
- Read/Write Operations
    - Batch read and write for append tables and primary key tables
    - Predicate filtering
    - Overwrite semantics
    - Incremental reading of Delta data
    - Reading and writing blob data
    - `with_shard` feature
    - Rollback feature
    - Streaming reads
    - Parallel consumption with bucket filtering
    - Row kind support for changelog streams
