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

Use `row.get_blob(pos)` to access blob columns. It returns a `Blob` object
regardless of how the blob is stored.

```python
read_builder = table.new_read_builder()
splits = read_builder.new_scan().plan().splits()
read = read_builder.new_read()

for row in read.to_iterator(splits):
    blob = row.get_blob(2)
    if blob is None:
        continue
    data = blob.to_data()
```

## Streaming for Large Blobs

`blob.new_input_stream()` returns a file-like object. Whether it is
genuinely lazy depends on how the table is configured:

- Default mode (`blob-as-descriptor=false`): the read path materialises
  the payload before it reaches `row.get_blob(pos)`. `Blob` is a
  `BlobData` and `new_input_stream()` wraps the in-memory bytes — not
  true streaming. For large blobs this can still OOM.
- Descriptor mode (`blob-as-descriptor=true`): the read path preserves
  the descriptor. `Blob` is a `BlobRef` and `new_input_stream()` opens
  the underlying file on demand.

This mirrors Java's `BlobFormatReader` semantics.

For genuine on-demand streaming of large blobs (videos, model weights),
use `table.copy` to set `blob-as-descriptor=true` before reading:

```python
table = catalog.get_table('my_db.image_table')
table = table.copy({'blob-as-descriptor': 'true'})

read_builder = table.new_read_builder()
splits = read_builder.new_scan().plan().splits()
read = read_builder.new_read()

# Reads now return BlobRef whose new_input_stream() is lazy.
for row in read.to_iterator(splits):
    with row.get_blob(2).new_input_stream() as stream:
        chunk = stream.read(1024)
```

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
