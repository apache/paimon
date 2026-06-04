---
title: "Blob Storage"
sidebar_position: 7
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

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

# Blob Storage

## Overview

The `BLOB` (Binary Large Object) type is a data type designed for storing multimodal data such as images, videos, audio files, and other large binary objects in Paimon tables. Unlike traditional `BYTES` type which stores binary data inline with other columns, `BLOB` type stores large binary data in separate files and maintains references to them, providing better performance for large objects.

The Blob Storage is based on Data Evolution mode.

The Blob type is ideal for:

- **Image Storage**: Store product images, user avatars, medical imaging data
- **Video Content**: Store video clips, surveillance footage, multimedia content
- **Audio Files**: Store voice recordings, music files, podcast episodes
- **Document Storage**: Store PDF documents, office files, large text files
- **Machine Learning**: Store embeddings, model weights, feature vectors
- **Any Large Binary Data**: Any data that is too large to store efficiently inline

## Storage Layout

When you define a table with a Blob column, Paimon automatically separates the storage:

1. **Normal Data Files** (e.g., `.parquet`, `.orc`): Store regular columns (INT, STRING, etc.)
2. **Blob Data Files** (`.blob`): Store the actual blob data

For example, given a table with schema `(id INT, name STRING, picture BLOB)`:

```
table/
├── bucket-0/
│   ├── data-uuid-0.parquet      # Contains id, name columns
│   ├── data-uuid-1.blob         # Contains picture blob data
│   ├── data-uuid-2.blob         # Contains more picture blob data
│   └── ...
├── manifest/
├── schema/
└── snapshot/
```

This separation provides several benefits:
- Efficient column projection (reading non-blob columns doesn't load blob data)
- Optimized file rolling based on blob size
- Better compression for regular columnar data

For details about the blob file format structure, see [File Format - BLOB](../concepts/spec/fileformat#blob).

## Storage Modes

Paimon supports four storage modes for BLOB fields, selected via **comment directives** on the column:

1. **Default blob storage** (`__BLOB_FIELD`)
   Blob bytes are written to Paimon-managed `.blob` files under the table path.

2. **Descriptor-only storage** (`__BLOB_DESCRIPTOR_FIELD`)
   Only serialized `BlobDescriptor` bytes are stored inline in data files. Paimon does not write `.blob` files for these fields, and writes must provide descriptor-based input.

3. **External-storage descriptor mode** (`__BLOB_EXTERNAL_STORAGE_FIELD`)
   At write time, Paimon writes the raw blob data to the configured `blob-external-storage-path` and stores only serialized `BlobDescriptor` bytes inline in data files.

4. **Blob view storage** (`__BLOB_VIEW_FIELD`)
   Serialized `BlobViewStruct` bytes are stored inline. The struct points to a BLOB value in an upstream table by table identifier, BLOB field, and row id. The actual blob bytes are resolved from the upstream table at read time.

This allows one table to mix different storage modes for different BLOB columns.

## Table Options

<table className="table table-bordered">
    <thead>
      <tr>
        <th className="text-left" style={{width: "25%"}}>Option</th>
        <th className="text-center" style={{width: "8%"}}>Required</th>
        <th className="text-center" style={{width: "7%"}}>Default</th>
        <th className="text-center" style={{width: "10%"}}>Type</th>
        <th className="text-center" style={{width: "50%"}}>Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>blob-as-descriptor</h5></td>
      <td>No</td>
      <td style={{wordWrap: "break-word"}}>false</td>
      <td>Boolean</td>
      <td>Controls read output format for blob fields. When set to true, queries return serialized BlobDescriptor bytes; when false, queries return actual blob bytes. This option is dynamic and can be changed with <code>ALTER TABLE ... SET</code>.</td>
    </tr>
    <tr>
      <td><h5>blob-write-null-on-missing-file</h5></td>
      <td>No</td>
      <td style={{wordWrap: "break-word"}}>false</td>
      <td>Boolean</td>
      <td>
        When enabled for Flink writes, if a descriptor BLOB value references a file that does not exist, Paimon writes <code>NULL</code> for that value and logs a warning instead of failing when reading the descriptor.
      </td>
    </tr>
    <tr>
      <td><h5>blob-view.resolve.enabled</h5></td>
      <td>No</td>
      <td style={{wordWrap: "break-word"}}>true</td>
      <td>Boolean</td>
      <td>
        Controls whether blob view fields are resolved to the upstream BLOB
        content at read time. Set to <code>false</code> when forwarding blob view
        references from one view table to another.
      </td>
    </tr>
    <tr>
      <td><h5>blob-external-storage-path</h5></td>
      <td>No</td>
      <td style={{wordWrap: "break-word"}}>(none)</td>
      <td>String</td>
      <td>
        External storage path for fields declared with <code>__BLOB_EXTERNAL_STORAGE_FIELD</code>.
        Orphan file cleanup is not applied to this path.
      </td>
    </tr>
    <tr>
      <td><h5>blob.target-file-size</h5></td>
      <td>No</td>
      <td style={{wordWrap: "break-word"}}>(same as target-file-size)</td>
      <td>MemorySize</td>
      <td>Target size for blob files. When a blob file reaches this size, a new file is created. If not specified, uses the same value as <code>target-file-size</code>.</td>
    </tr>
    <tr>
      <td><h5>row-tracking.enabled</h5></td>
      <td>Yes*</td>
      <td style={{wordWrap: "break-word"}}>false</td>
      <td>Boolean</td>
      <td>Must be enabled for blob tables to support row-level operations.</td>
    </tr>
    <tr>
      <td><h5>data-evolution.enabled</h5></td>
      <td>Yes*</td>
      <td style={{wordWrap: "break-word"}}>false</td>
      <td>Boolean</td>
      <td>Must be enabled for blob tables to support schema evolution.</td>
    </tr>
    </tbody>
</table>

*Required for blob functionality to work correctly.

Specifically, if the storage system of the input BlobDescriptor differs from that used by Paimon, 
you can specify the storage configuration for the input blob descriptor using the prefix 
`blob-descriptor.`. For example, if the source data is stored in a different OSS endpoint, 
you can configure it as below (using flink sql as an example):
```sql
CREATE TABLE image_table (
    id INT,
    name STRING,
    image BYTES COMMENT '__BLOB_FIELD'
) WITH (
    'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true',
    'fs.oss.endpoint' = 'aaa',                   -- This is for Paimon's own config
    'blob-descriptor.fs.oss.endpoint' = 'bbb'    -- This is for input blob descriptors' config
);
```

## Creating a Table

The recommended way to create a blob table in SQL is to use the **comment directive** `__BLOB_FIELD`, `__BLOB_DESCRIPTOR_FIELD`, or `__BLOB_VIEW_FIELD` on the column. Paimon automatically converts the column type to `BLOB` and registers it in the corresponding option.

<Tabs groupId="blob-create-table">

<TabItem value="flink-sql" label="Flink SQL">

```sql
CREATE TABLE image_table (
    id INT,
    name STRING,
    image BYTES COMMENT '__BLOB_FIELD; product image'
) WITH (
    'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true'
);

-- Multiple blob columns with different storage modes
CREATE TABLE media_table (
    id INT,
    photo BYTES COMMENT '__BLOB_FIELD; original photo',
    thumbnail BYTES COMMENT '__BLOB_DESCRIPTOR_FIELD; thumbnail descriptor',
    preview BYTES COMMENT '__BLOB_VIEW_FIELD; preview from upstream'
) WITH (
    'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true'
);
```

</TabItem>

<TabItem value="spark-sql" label="Spark SQL">

```sql
CREATE TABLE image_table (
    id INT,
    name STRING,
    image BINARY COMMENT '__BLOB_FIELD; product image'
) TBLPROPERTIES (
    'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true'
);
```

</TabItem>

<TabItem value="java" label="Java API">

```java
// Java API uses BlobType directly, with comment directive for auto option registration
Schema schema = Schema.newBuilder()
        .column("id", DataTypes.INT())
        .column("name", DataTypes.STRING())
        .column("image", DataTypes.BYTES(), "__BLOB_FIELD; product image")
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .build();
```

</TabItem>

<TabItem value="python" label="Python API">

```python
import pyarrow as pa
from pypaimon import Schema

# pa.large_binary() is automatically recognized as BLOB type
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
    }
)
```

</TabItem>

</Tabs>

The comment directive format is `__DIRECTIVE; optional user comment`. Paimon converts the `BYTES`/`BINARY` type to `BLOB`, registers the field in the corresponding option, and stores the text after `;` as the column's real comment.

Supported directives:

| Directive | Storage mode | Option |
|-----------|-------------|--------|
| `__BLOB_FIELD` | Raw bytes in `.blob` files | `blob-field` |
| `__BLOB_DESCRIPTOR_FIELD` | Descriptor bytes inline | `blob-descriptor-field` |
| `__BLOB_VIEW_FIELD` | View reference inline | `blob-view-field` |
| `__BLOB_EXTERNAL_STORAGE_FIELD` | Raw data to external path, descriptor inline | `blob-external-storage-field` + `blob-descriptor-field` |

## Adding a Blob Column

The same comment directive works with `ALTER TABLE ADD COLUMN`:

<Tabs groupId="blob-add-column">

<TabItem value="flink-sql" label="Flink SQL">

```sql
ALTER TABLE image_table ADD picture BYTES COMMENT '__BLOB_FIELD';

ALTER TABLE image_table
    ADD video BYTES COMMENT '__BLOB_DESCRIPTOR_FIELD; promotional video';
```

</TabItem>

<TabItem value="spark-sql" label="Spark SQL">

```sql
ALTER TABLE image_table ADD COLUMN picture BINARY COMMENT '__BLOB_FIELD';

ALTER TABLE image_table
    ADD COLUMN video BINARY COMMENT '__BLOB_DESCRIPTOR_FIELD; promotional video';
```

</TabItem>

<TabItem value="java" label="Java API">

```java
// Java API: add column with BlobType directly
schemaManager.commitChanges(
    SchemaChange.addColumn("picture", DataTypes.BLOB()));

// Or use comment directive like SQL
schemaManager.commitChanges(
    SchemaChange.addColumn("video", DataTypes.BYTES(),
        "__BLOB_DESCRIPTOR_FIELD; promotional video", null));
```

</TabItem>

<TabItem value="python" label="Python API">

```python
# pa.large_binary() is automatically recognized as BLOB type
catalog.alter_table(
    'default.image_table',
    [('add', 'picture', pa.large_binary())]
)
```

</TabItem>

</Tabs>

## Inserting Blob Data

<Tabs groupId="blob-insert">

<TabItem value="flink-sql" label="Flink SQL">

```sql
INSERT INTO image_table VALUES (1, 'sample', X'89504E470D0A1A0A');

INSERT INTO image_table
SELECT id, name, content FROM source_table;
```

</TabItem>

<TabItem value="spark-sql" label="Spark SQL">

```sql
INSERT INTO image_table VALUES (1, 'sample', X'89504E470D0A1A0A');
```

</TabItem>

<TabItem value="java" label="Java API">

```java
GenericRow row = GenericRow.of(
        1,
        BinaryString.fromString("sample"),
        new BlobData(imageBytes)
);
write.write(row);
```

</TabItem>

<TabItem value="python" label="Python API">

```python
data = pa.table({
    'id': pa.array([1], type=pa.int32()),
    'name': pa.array(['sample']),
    'image': pa.array([b'\x89PNG\r\n\x1a\n'], type=pa.large_binary()),
})
writer.write_arrow(data)
```

</TabItem>

</Tabs>

## Querying Blob Data

<Tabs groupId="blob-query">

<TabItem value="sql" label="SQL">

```sql
-- Select all columns including blob
SELECT * FROM image_table;

-- Select only non-blob columns (efficient - doesn't load blob data)
SELECT id, name FROM image_table;

-- Return descriptor bytes instead of actual blob bytes
ALTER TABLE image_table SET ('blob-as-descriptor' = 'true');
SELECT image FROM image_table;
```

</TabItem>

<TabItem value="java" label="Java API">

```java
Blob blob = row.getBlob(2);

// Load into memory
byte[] data = blob.toData();

// Stream (recommended for large blobs)
try (SeekableInputStream in = blob.newInputStream()) {
    in.seek(100);  // random access
    byte[] buffer = new byte[1024];
    int bytesRead = in.read(buffer);
}

// Get descriptor reference (for descriptor-based blobs)
BlobDescriptor descriptor = blob.toDescriptor();
```

</TabItem>

</Tabs>

## Blob Construct Sources (Java API)

```java
Blob blob = Blob.fromData(imageBytes);                                         // byte array
Blob blob = Blob.fromLocal("/path/to/image.png");                              // local file
Blob blob = Blob.fromFile(fileIO, "s3://bucket/path/to/image.png");            // any FileIO
Blob blob = Blob.fromFile(fileIO, "s3://bucket/large-file.bin", 1024, 2048);   // partial file
Blob blob = Blob.fromHttp("https://example.com/image.png");                    // HTTP URL
Blob blob = Blob.fromInputStream(() -> new FileInputStream("..."));            // InputStream
Blob blob = Blob.fromDescriptor(uriReader, descriptor);                        // BlobDescriptor
```

## Descriptor-Only Storage

If you want downstream tables to **reuse** upstream blob files (no copying and no new `.blob` files), use `__BLOB_DESCRIPTOR_FIELD`:

```sql
CREATE TABLE descriptor_table (
    id INT,
    image BYTES COMMENT '__BLOB_DESCRIPTOR_FIELD; reused image'
) WITH (
    'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true'
);
```

Paimon stores only serialized `BlobDescriptor` bytes in normal data files. Reading the blob follows the descriptor URI to access bytes, and writing requires descriptor input for those fields.

## External Storage

If you want Paimon to write raw blob data to a separate external location while keeping only descriptor bytes inline, use `__BLOB_EXTERNAL_STORAGE_FIELD`:

```sql
CREATE TABLE external_table (
    id INT,
    image BYTES COMMENT '__BLOB_EXTERNAL_STORAGE_FIELD'
) WITH (
    'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true',
    'blob-external-storage-path' = 'oss://bucket/path/'
);
```

For these fields:

- raw blob data is written to the configured external storage path
- normal data files keep only serialized `BlobDescriptor` bytes
- writes can still start from raw BLOB input
- the field is treated as descriptor-based for operations such as `MERGE INTO`

## Blob View

Blob view is useful when a downstream table should reference BLOB values already stored in an upstream table, without copying the bytes or creating new `.blob` files. A blob view field stores only a small `BlobViewStruct` inline. When the field is read, Paimon resolves the referenced BLOB from the upstream table.

Blob view requires:

- the upstream table to have row tracking enabled, so each row has a stable `_ROW_ID`
- the downstream field to be declared with `__BLOB_VIEW_FIELD` comment directive
- writes to provide a serialized `BlobViewStruct`; in Flink SQL, use the built-in `sys.blob_view` function

The Flink SQL function signature is:

```sql
sys.blob_view(table_name, field_name, row_id)
```

Arguments:

- `table_name`: the upstream table name. It must be fully qualified as `database.table` or `catalog.database.table`. Unqualified table names are rejected.
- `field_name`: the upstream BLOB field name.
- `row_id`: the `_ROW_ID` value from the upstream row-tracking table.

The following example writes a downstream table whose `image_ref` field views the `image` field in `image_table`:

```sql
CREATE TABLE image_table (
    id INT,
    name STRING,
    image BYTES COMMENT '__BLOB_FIELD'
) WITH (
    'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true'
);

CREATE TABLE image_view_table (
    id INT,
    label STRING,
    image_ref BYTES COMMENT '__BLOB_VIEW_FIELD'
) WITH (
    'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true'
);

INSERT INTO image_view_table
SELECT
    id,
    name AS label,
    sys.blob_view('default.image_table', 'image', _ROW_ID)
FROM `image_table$row_tracking`;
```

If the current Paimon catalog name is included in the table name, the function also accepts `catalog.database.table`:

```sql
SELECT sys.blob_view('my_catalog.default.image_table', 'image', _ROW_ID)
FROM `image_table$row_tracking`;
```

Reads from `image_view_table.image_ref` return the referenced BLOB bytes in the same way as normal blob fields. The referenced upstream table and row must remain available for the view to be resolved.

**Forward Blob View References**

By default, reading a blob view field resolves the `BlobViewStruct` and returns the upstream BLOB
content. If you want to import data from one blob view table into another blob view table without
copying the BLOB bytes, read the source table with `blob-view.resolve.enabled=false` and write the
result into a target field declared with `__BLOB_VIEW_FIELD`.

With this option disabled, Paimon preserves the serialized `BlobViewStruct` during reads. When the
preserved value is written to another blob view field, the target table stores the same upstream
reference instead of creating a chained view reference.

For example, if table `T1` contains blob view references to BLOBs in table `T0`, importing `T1` into
`T2` with `blob-view.resolve.enabled=false` makes `T2` keep referencing `T0` directly.

```sql
CREATE TABLE t2 (
    id INT,
    image_ref BYTES COMMENT '__BLOB_VIEW_FIELD'
) WITH (
    'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true'
);

-- Flink SQL example: the source table is read with blob view resolution disabled.
INSERT INTO t2
SELECT id, image_ref
FROM t1 /*+ OPTIONS('blob-view.resolve.enabled'='false') */;
```

## MERGE INTO Support

For Data Evolution writes in Flink and Spark:

- raw-data BLOB columns are still rejected in partial-column `MERGE INTO` updates
- descriptor-based BLOB columns are allowed

For the Python equivalent, see [Blob Storage in pypaimon](../pypaimon/blob).

## Limitations

1. **Append Table Only**: Blob type is designed for append-only tables. Primary key tables are not supported.
2. **No Predicate Pushdown**: Blob columns cannot be used in filter predicates.
3. **No Statistics**: Statistics collection is not supported for blob columns.
4. **Required Options**: `row-tracking.enabled` and `data-evolution.enabled` must be set to `true`.
5. **External Storage Cleanup**: Files written through `blob-external-storage-path` are outside Paimon's orphan file cleanup scope.
6. **Blob View Dependency**: Blob view fields depend on the referenced upstream table and row. If the upstream data is removed or no longer readable, the view cannot be resolved.

## Best Practices

1. **Use Column Projection**: Always select only the columns you need. Avoid `SELECT *` if you don't need blob data.

2. **Set Appropriate Target File Size**: Configure `blob.target-file-size` based on your blob sizes. Larger values mean fewer files but larger individual files.

3. **Use Descriptor Fields When Reusing External Blob Files**: Use `__BLOB_DESCRIPTOR_FIELD` for fields that should keep descriptor references instead of writing new `.blob` files.

4. **Use External-Storage Fields When Accepting Raw Input But Storing Descriptors**: Use `__BLOB_EXTERNAL_STORAGE_FIELD` with `blob-external-storage-path` when upstream writes raw blob bytes but you want descriptor-based storage.

5. **Manage External Storage Lifecycle Separately**: Files written to `blob-external-storage-path` are not cleaned up by Paimon, so retention and deletion should be managed externally.

6. **Use Blob View to Avoid Copying BLOB Data**: Use `__BLOB_VIEW_FIELD` when a downstream table only needs to reference BLOB values from an upstream table.

7. **Use Partitioning**: Partition your blob tables by date or other dimensions to improve query performance and data management.
