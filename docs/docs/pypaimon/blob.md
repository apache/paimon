---
title: "Blob Storage"
sidebar_position: 7
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

# Blob Storage in pypaimon

For Paimon's Blob storage concepts (storage modes, table options, SQL usage,
Java API), see [Blob Storage](../multimodal-table/blob).

This page covers the Python API for reading and writing BLOB columns.

## Creating a Table

A BLOB column maps to PyArrow `large_binary()`. The table must enable
`row-tracking.enabled` and `data-evolution.enabled`.

```python
from pypaimon import CatalogFactory, Schema
import pyarrow as pa

catalog = CatalogFactory.create({'warehouse': '/tmp/paimon-warehouse'})
catalog.create_database('my_db', True)

pa_schema = pa.schema([
    ('id', pa.int32()),
    ('name', pa.string()),
    ('image', pa.large_binary()),
])
schema = Schema.from_pyarrow_schema(
    pa_schema,
    options={
        'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true',
    },
)
catalog.create_table('my_db.image_table', schema, True)
```

## Writing Blob Data

Pass raw bytes for the blob column in a PyArrow Table; pypaimon writes them
to dedicated `.blob` files automatically.

```python
table = catalog.get_table('my_db.image_table')
write_builder = table.new_batch_write_builder()
writer = write_builder.new_write()

with open('cat.jpg', 'rb') as f1, open('dog.jpg', 'rb') as f2:
    writer.write_arrow(pa.Table.from_pydict({
        'id': [1, 2],
        'name': ['cat', 'dog'],
        'image': [f1.read(), f2.read()],
    }, schema=pa_schema))

write_builder.new_commit().commit(writer.prepare_commit())
writer.close()
```

## Reading Blob Data

### Batch reading (recommended)

Use `to_arrow_batch_reader` to read blob data in batches. Set
`blob_parallelism` to enable concurrent blob reads within each batch:

```python
read_builder = table.new_read_builder()
splits = read_builder.new_scan().plan().splits()
read = read_builder.new_read()

for batch in read.to_arrow_batch_reader(splits, blob_parallelism=16):
    for i in range(len(batch)):
        image_bytes = batch['image'][i].as_py()
```

Or read all data into a single Arrow Table:

```python
arrow_table = read.to_arrow(splits, blob_parallelism=16)
```

### Row-by-row reading

Use `row.get_blob(pos)` to access blob columns one row at a time:

```python
for row in read.to_iterator(splits):
    blob = row.get_blob(2)
    if blob is None:
        continue
    data = blob.to_data()
```

### Streaming / partial reads

For true on-demand streaming (large blobs like videos or model weights),
set `blob-as-descriptor=true` so blob values are kept as lightweight
references instead of being materialized into memory:

```python
table = table.copy({'blob-as-descriptor': 'true'})

read_builder = table.new_read_builder()
splits = read_builder.new_scan().plan().splits()
read = read_builder.new_read()

for row in read.to_iterator(splits):
    blob = row.get_blob(2)
    if blob is None:
        continue
    with blob.new_input_stream() as stream:
        chunk = stream.read(4096)
```

Without `blob-as-descriptor=true`, blob values are materialized before
`row.get_blob(...)` returns; `new_input_stream()` then reads from
in-memory bytes, not from storage.

## Lower-level: `Blob.from_bytes`

When you already have raw or descriptor bytes (for example from a custom
source) and want to wrap them as a `Blob`, use the factory:

```python
from pypaimon.table.row.blob import Blob

# Inline bytes → BlobData (no file_io required)
blob = Blob.from_bytes(b'hello')

# Descriptor bytes → BlobRef (lazy; requires file_io to resolve the URI)
file_io = table.file_io
blob = Blob.from_bytes(descriptor_bytes, file_io)

data = blob.to_data()
```

The factory auto-dispatches based on the bytes content (BLOBDESC magic
header). This mirrors Java's `Blob.fromBytes(...)`.

## See Also

- [Blob Storage](../multimodal-table/blob) — concept, storage modes,
  SQL/Java API
- [Data Evolution](./data-evolution) — required for
  blob tables
