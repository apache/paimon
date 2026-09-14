---
title: "BLOB Object API"
description: "Use object keys to put, fetch, and delete values in a BLOB column."
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

# BLOB Object API

Use object keys to put, fetch, and delete values in a BLOB column. This interface stores objects in a Paimon table; for Arrow and row reads, see [BLOB Storage](./blob).

## Create an object table

Create a table with an explicit object key and a BLOB column. This gives the
`docs` object used below; examples that add more metadata columns require those
columns in the schema as well.

```python
import pyarrow as pa
import pypaimon.multimodal as pm

conn = pm.connect(options={"warehouse": "file:///tmp/warehouse"})
docs = conn.create_table("objects", schema=pa.schema([
    ("key", pa.string()),
    ("image", pa.large_binary()),
    ("content_type", pa.string()),
    ("owner", pa.string()),
]))
```

## Blobs

Use `blobs()` to work with a BLOB column through an S3-like object API. A blob
store maps object keys to a table column such as `key`, `object_key`, or an
explicit `key_column`.

### Put Objects

`put_object` writes one object. `put_objects` writes a batch of objects in one
Paimon commit. Both methods upsert by key: existing matching rows are updated,
and missing keys append new rows. If the same key appears more than once in a
single `put_objects` batch, the last object wins. BlobStore does not enforce
object-key uniqueness; that should be modeled with a table-level unique key when
available. The object body can be raw bytes, a binary file-like object, or a
PyPaimon `Blob`. Use `columns` to set the non-key, non-BLOB table columns for
the object row. For managed BLOB storage, use `head_object` or `get_object` to
inspect the stored descriptor after the write.

```python
store = docs.blobs(column="image", key_column="key")

store.put_object(
    "images/cat.jpg",
    body=open("cat.jpg", "rb"),
    columns={"content_type": "image/jpeg"},
)

store.put_objects([
    {
        "key": "images/dog.jpg",
        "body": open("dog.jpg", "rb"),
        "columns": {"content_type": "image/jpeg"},
    },
    {
        "key": "images/logo.png",
        "body": b"...",
        "columns": {"content_type": "image/png"},
    },
])
```

For Paimon-native reference storage, configure the BLOB column as a
`blob-descriptor-field` and pass `uri`/`offset`/`length` or `descriptor` to
`put_object` and `put_objects`. The table stores the external `BlobDescriptor`
instead of copying the object into Paimon `.blob` files. Without
`blob-descriptor-field`, descriptor-backed inputs are streamed into managed
Paimon `.blob` files. External S3 URIs are read with the table's FileIO, so S3
credentials such as `fs.s3.accessKeyId`, `fs.s3.accessKeySecret`,
`fs.s3.securityToken`, `fs.s3.endpoint`, and `fs.s3.region` can be supplied in
the options passed to `connect`. The same options are used for every external
URI; there is no per-object credential override.

```python
store.put_objects([
    {
        "key": "videos/intro.mp4",
        "uri": "s3://bucket/videos/intro.mp4",
        "offset": 0,
        "length": intro_content_length,
        "columns": {"content_type": "video/mp4"},
    },
    {
        "key": "videos/demo.mp4",
        "uri": "s3://bucket/videos/demo.mp4",
        "offset": 0,
        "length": demo_content_length,
        "columns": {"content_type": "video/mp4"},
    },
])

store.put_object(
    "videos/trailer.mp4",
    uri="s3://bucket/videos/trailer.mp4",
    offset=0,
    length=trailer_content_length,
    columns={"content_type": "video/mp4"},
)
```

### Read Objects

`get_object` reads an object by key. Its `range` parameter accepts a single
S3-style byte range such as `bytes=0-1023`, `bytes=1024-`, or `bytes=-512`.
`head_object` and `list_objects` return object information without opening the
object stream. `get_object`, `head_object`, and `list_objects` expose non-key,
non-BLOB table columns through `columns`. These columns are all returned by
default. Pass `columns` to return only selected columns, or `[]` to skip them.
`list_objects` requires a non-negative `limit`; `limit=0` returns an empty
list.

```python
obj = store.get_object("images/cat.jpg", range="bytes=0-1023")
with obj.open() as stream:
    chunk = stream.read()

info = store.head_object(
    "images/cat.jpg",
    columns=["content_type"],
)
content_type = info.columns["content_type"]

objects = store.list_objects(prefix="images/", columns=[])
for obj_info in objects:
    print(obj_info.key, obj_info.size, obj_info.columns)
```

### Delete Objects

`delete_object` deletes one object key. `delete_objects` deletes a batch of keys
in one Paimon commit. Missing keys are ignored, and repeated keys in the same
request are folded before deletion. BlobStore does not enforce object-key
uniqueness; if multiple table rows already match a key, deleting that key removes
all matching rows.

```python
store.delete_object("images/dog.jpg")

store.delete_objects([
    "images/old-1.jpg",
    "images/old-2.jpg",
])
```

### Update Columns

Amazon S3 updates object metadata by copying the object to the same key with
replacement metadata. BlobStore exposes the corresponding Paimon-native
operation directly: `update_object_columns` and `update_objects_columns` update
non-key, non-BLOB table columns without rewriting the blob data. Missing keys
raise `NoSuchKey`.

```python
store.update_object_columns(
    "images/cat.jpg",
    {"content_type": "image/webp", "owner": "alice"},
)

store.update_objects_columns([
    {
        "key": "images/dog.jpg",
        "columns": {"content_type": "image/webp"},
    },
    {
        "key": "images/logo.png",
        "columns": {"content_type": "image/svg+xml"},
    },
])
```
