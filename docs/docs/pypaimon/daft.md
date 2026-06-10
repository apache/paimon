---
title: "Daft"
sidebar_position: 4
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

# Daft

[Daft](https://www.daft.io/) is a distributed DataFrame engine for Python.
See also the [Daft Paimon connector documentation](https://docs.daft.ai/en/stable/connectors/paimon/).

This requires `daft` to be installed:

```bash
pip install 'pypaimon[daft]'
```

To execute Daft plans on Ray, install both extras:

```bash
pip install 'pypaimon[daft,ray]'
```

`pypaimon.daft` exposes a top-level `read_paimon` / `write_paimon` API that
takes a table identifier and catalog options directly.

## Read

### `read_paimon` (recommended)

```python
from pypaimon.daft import read_paimon

df = read_paimon(
    "database_name.table_name",
    catalog_options={"warehouse": "/path/to/warehouse"},
)

df.show()
```

`read_paimon` opens its own catalog and resolves the table in a single call.

The returned DataFrame is lazy. Use standard Daft operations for filtering,
projection, and limit — they are automatically pushed down into the Paimon scan
via Daft's DataSource protocol:

```python
import daft

df = read_paimon(
    "database_name.table_name",
    catalog_options={"warehouse": "/path/to/warehouse"},
)

# Filter pushdown (partition pruning + file-level skipping)
df = df.where(daft.col("dt") == "2024-01-01")

# Projection pushdown (only requested columns are read from disk)
df = df.select("id", "name")

# Limit pushdown
df = df.limit(100)

df.show()
```

**Time travel:**

```python
# Read a specific snapshot.
df = read_paimon(
    "database_name.table_name",
    catalog_options={"warehouse": "/path/to/warehouse"},
    snapshot_id=42,
)

# Read a tagged snapshot.
df = read_paimon(
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
- `snapshot_id`: optional snapshot id to time-travel to. Mutually
  exclusive with `tag_name`.
- `tag_name`: optional tag name to time-travel to. Mutually
  exclusive with `snapshot_id`.
- `io_config`: optional Daft `IOConfig` for accessing object storage.
  If `None`, will be inferred from the catalog options.

For tables on object stores, credentials are inferred from the catalog options
automatically, or you can pass an explicit `IOConfig`:

```python
from daft.io import IOConfig, S3Config

df = read_paimon(
    "my_db.my_table",
    catalog_options={
        "warehouse": "s3://my-bucket/warehouse",
        "fs.s3.accessKeyId": "...",
        "fs.s3.accessKeySecret": "...",
    },
)
df.show()
```

**Features:**
- Append-only tables with Parquet format use Daft's native high-performance Parquet reader.
- Primary-key tables that require LSM merge fall back to pypaimon's built-in reader.
- Partition pruning, predicate pushdown, projection pushdown, and limit pushdown are all supported.

## Write

### `write_paimon` (recommended)

```python
import daft
from pypaimon.daft import write_paimon

df = daft.from_pydict({
    "id": [1, 2, 3],
    "name": ["alice", "bob", "charlie"],
    "dt": ["2024-01-01", "2024-01-01", "2024-01-01"],
})

write_paimon(
    df,
    "database_name.table_name",
    catalog_options={"warehouse": "/path/to/warehouse"},
)
```

`write_paimon` opens its own catalog, resolves the table, and commits the
write through Daft's DataSink API.

**Overwrite mode:**

```python
write_paimon(
    df,
    "database_name.table_name",
    catalog_options={"warehouse": "/path/to/warehouse"},
    mode="overwrite",
)
```

For unpartitioned tables, overwrite replaces the table contents. For
partitioned tables, overwrite follows Paimon's dynamic partition overwrite
semantics by default: only partitions present in the input DataFrame are
replaced, and existing partitions not present in the input are kept.

**Parameters:**
- `df`: the Daft DataFrame to write.
- `table_identifier`: full table name, e.g. `"db_name.table_name"`.
- `catalog_options`: kwargs forwarded to `CatalogFactory.create()`.
- `mode`: write mode — `"append"` (default) or `"overwrite"`.

## Running Daft on Ray

`pypaimon.daft` works with Daft's Ray runner. Configure the runner before the
first Daft execution in the process:

```python
import daft
import ray
from daft import runners
from pypaimon.daft import read_paimon, write_paimon

ray.init()  # use address="auto" to connect to an existing Ray cluster
runners.set_runner_ray()

df = daft.from_pydict({
    "id": [1, 2, 3],
    "name": ["alice", "bob", "charlie"],
    "dt": ["2024-01-01", "2024-01-01", "2024-01-02"],
})

write_paimon(
    df,
    "database_name.table_name",
    catalog_options={"warehouse": "/path/to/warehouse"},
)

result = (
    read_paimon(
        "database_name.table_name",
        catalog_options={"warehouse": "/path/to/warehouse"},
    )
    .where(daft.col("dt") == "2024-01-01")
    .select("id", "name")
)

result.show()
```

Use `pypaimon.daft` when your application is written with Daft DataFrames and
you want Daft to schedule the execution on Ray. Use `pypaimon.ray` instead when
your application directly reads or writes Ray Datasets.

## Reading Blob Columns

Tables with BLOB columns (see [Blob Storage](./blob)) can be read with Daft.
Blob columns are returned as `daft.File` references — the actual bytes are
**not** loaded until you explicitly read them.

:::caution Daft's built-in connector does not support blob lazy loading yet

`daft.read_paimon()` currently reads blob data eagerly during the scan phase,
ignoring column pruning and filter pushdown for blob columns.
Use `from pypaimon.daft import read_paimon` instead for blob lazy loading.
:::

### Lazy loading

When you read a blob table through `pypaimon.daft`, the connector
automatically enables descriptor mode internally. Blob columns appear as
`daft.File` references in the DataFrame — no bytes are fetched from storage
at this point. The actual I/O only happens when you call `file.open()` inside
a UDF, and only for the rows that survive earlier filters:

```python
import daft
from pypaimon.daft import read_paimon

df = read_paimon(
    "my_db.image_table",
    catalog_options={"warehouse": "/path/to/warehouse"},
)

# Filter runs BEFORE any blob bytes are read.
df = df.where(daft.col("obs_index") < 2)

# Only filtered rows trigger blob I/O inside the UDF.
@daft.func
def image_size(file: daft.File) -> int:
    with file.open() as f:
        return len(f.read())

result = df.with_column("real_size", image_size(df["image_jpeg"]))
result.show()
```

### Parallel blob reading

By default, `@daft.func` processes rows sequentially. To read blobs
concurrently, use an async UDF with `max_concurrency`:

```python
import asyncio
import daft
from pypaimon.daft import read_paimon

df = read_paimon(
    "my_db.image_table",
    catalog_options={"warehouse": "/path/to/warehouse"},
)

@daft.func(max_concurrency=8)
async def image_size(file: daft.File) -> int:
    await asyncio.sleep(0)
    with file.open() as f:
        return len(f.read())

result = df.with_column("real_size", image_size(df["image_jpeg"]))
result.show()
```

When running on Ray, the UDF calls are distributed across Ray workers
automatically — each worker processes its partition in parallel, giving you
batch-level concurrency without any extra code.

## Catalog Abstraction

Paimon catalogs can integrate with Daft's unified `Catalog` / `Table` interfaces:

```python
import pypaimon
from pypaimon.daft import PaimonCatalog

inner = pypaimon.CatalogFactory.create({"warehouse": "/path/to/warehouse"})
catalog = PaimonCatalog(inner, name="my_paimon")

# Browse
catalog.list_namespaces()
catalog.list_tables()

# Read / write through catalog
table = catalog.get_table("my_db.my_table")
df = table.read()
table.append(df)
table.overwrite(df)
```

You can also wrap a single table directly:

```python
from pypaimon.daft import PaimonTable

inner_table = inner.get_table("my_db.my_table")
table = PaimonTable(inner_table)
df = table.read()
```

### Creating Tables

```python
import daft
from daft.io.partitioning import PartitionField

schema = daft.from_pydict({"id": [1], "name": ["a"], "dt": ["2024-01-01"]}).schema()
dt_field = schema["dt"]
partition_fields = [PartitionField.create(dt_field)]

table = catalog.create_table(
    "my_db.new_table",
    schema,
    partition_fields=partition_fields,
)

# Primary-key table
table = catalog.create_table(
    "my_db.pk_table",
    schema,
    properties={"primary_keys": ["id", "dt"]},
    partition_fields=partition_fields,
)
```
