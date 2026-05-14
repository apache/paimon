---
title: "Daft"
weight: 4
type: docs
aliases:
  - /pypaimon/daft.html
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

This requires `daft` to be installed:

```bash
pip install pypaimon[daft]
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

**Parameters:**
- `df`: the Daft DataFrame to write.
- `table_identifier`: full table name, e.g. `"db_name.table_name"`.
- `catalog_options`: kwargs forwarded to `CatalogFactory.create()`.
- `mode`: write mode — `"append"` (default) or `"overwrite"`.

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
