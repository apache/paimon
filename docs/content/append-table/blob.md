---
title: "Blob Storage"
weight: 7
type: docs
aliases:
- /append-table/blob-storage.html
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

For details about the blob file format structure, see [File Format - BLOB]({{< ref "concepts/spec/fileformat#blob" >}}).

## Table Options

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-center" style="width: 8%">Required</th>
        <th class="text-center" style="width: 7%">Default</th>
        <th class="text-center" style="width: 10%">Type</th>
        <th class="text-center" style="width: 50%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>blob-field</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specifies column names that should be stored as blob type. This is used when you want to treat a BYTES column as a BLOB.</td>
    </tr>
    <tr>
      <td><h5>blob-as-descriptor</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Controls read output format for blob fields. When set to true, queries return serialized BlobDescriptor bytes; when false, queries return actual blob bytes. This option is dynamic and can be changed with <code>ALTER TABLE ... SET</code>.</td>
    </tr>
    <tr>
      <td><h5>blob.stored-descriptor-fields</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>
        Comma-separated BLOB field names stored as serialized <code>BlobDescriptor</code> bytes inline in normal data files.
        By default, all blob fields store blob bytes in separate <code>.blob</code> files.
        If configured, one table can mix:
        some BLOB fields in <code>.blob</code> files and some as descriptor references.
      </td>
    </tr>
    <tr>
      <td><h5>blob.target-file-size</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">(same as target-file-size)</td>
      <td>MemorySize</td>
      <td>Target size for blob files. When a blob file reaches this size, a new file is created. If not specified, uses the same value as <code>target-file-size</code>.</td>
    </tr>
    <tr>
      <td><h5>row-tracking.enabled</h5></td>
      <td>Yes*</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Must be enabled for blob tables to support row-level operations.</td>
    </tr>
    <tr>
      <td><h5>data-evolution.enabled</h5></td>
      <td>Yes*</td>
      <td style="word-wrap: break-word;">false</td>
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
    image BYTES
) WITH (
    'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true',
    'blob-field' = 'image',
    'fs.oss.endpoint' = 'aaa',                   -- This is for Paimon's own config
    'blob-descriptor.fs.oss.endpoint' = 'bbb'    -- This is for input blob descriptors' config
);
```

## SQL Usage

### Creating a Table

{{< tabs "blob-create-table" >}}

{{< tab "Flink SQL" >}}
```sql
-- Create a table with a blob field
-- Note: In Flink SQL, use BYTES type and specify blob-field option
CREATE TABLE image_table (
    id INT,
    name STRING,
    image BYTES
) WITH (
    'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true',
    'blob-field' = 'image'
);
```
{{< /tab >}}

{{< tab "Spark SQL" >}}
```sql
-- Create a table with a blob field
-- Note: In Spark SQL, use BINARY type and specify blob-field option
CREATE TABLE image_table (
    id INT,
    name STRING,
    image BINARY
) TBLPROPERTIES (
    'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true',
    'blob-field' = 'image'
);
```
{{< /tab >}}

{{< /tabs >}}

### Inserting Blob Data

{{< tabs "blob-insert" >}}

{{< tab "Flink SQL" >}}
```sql
-- Insert data with inline blob bytes
INSERT INTO image_table VALUES (1, 'sample', X'89504E470D0A1A0A');

-- Insert from another table
INSERT INTO image_table
SELECT id, name, content FROM source_table;
```
{{< /tab >}}

{{< tab "Spark SQL" >}}
```sql
-- Insert data with inline blob bytes
INSERT INTO image_table VALUES (1, 'sample', X'89504E470D0A1A0A');
```
{{< /tab >}}

{{< /tabs >}}

### Querying Blob Data

```sql
-- Select all columns including blob
SELECT * FROM image_table;

-- Select only non-blob columns (efficient - doesn't load blob data)
SELECT id, name FROM image_table;

-- Select specific rows with blob
SELECT * FROM image_table WHERE id = 1;
```

### Blob Read Output Mode (`blob-as-descriptor`)

`blob-as-descriptor` only controls how blob values are returned when reading.

```sql
-- Return descriptor bytes
ALTER TABLE blob_table SET ('blob-as-descriptor' = 'true');
SELECT image FROM blob_table;

-- Return actual blob bytes
ALTER TABLE blob_table SET ('blob-as-descriptor' = 'false');
SELECT image FROM blob_table;
```

## Java API Usage

### Creating a Table

The following example demonstrates how to create a table with a blob column, write blob data, and read it back using Paimon's Java API.

```java
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;

public class BlobTableExample {

    public static void main(String[] args) throws Exception {
        // 1. Create catalog
        Path warehouse = new Path("/tmp/paimon-warehouse");
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(warehouse));
        catalog.createDatabase("my_db", true);

        // 2. Define schema with BLOB column
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .column("image", DataTypes.BLOB())  // Blob column for storing images
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .build();

        // 3. Create table
        Identifier tableId = Identifier.create("my_db", "image_table");
        catalog.createTable(tableId, schema, true);
        Table table = catalog.getTable(tableId);

        // 4. Write blob data
        writeBlobData(table);

        // 5. Read blob data back
        readBlobData(table);
    }

    private static void writeBlobData(Table table) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();

        try (BatchTableWrite write = writeBuilder.newWrite();
             BatchTableCommit commit = writeBuilder.newCommit()) {

            // Method 1: Create blob from byte array
            byte[] imageBytes = loadImageBytes("/path/to/image1.png");
            GenericRow row1 = GenericRow.of(
                    1,
                    BinaryString.fromString("image1"),
                    new BlobData(imageBytes)
            );
            write.write(row1);

            // Method 2: Create blob from local file
            GenericRow row2 = GenericRow.of(
                    2,
                    BinaryString.fromString("image2"),
                    Blob.fromLocal("/path/to/image2.png")
            );
            write.write(row2);

            // Method 3: Create blob from InputStream (useful for streaming data)
            byte[] streamData = loadImageBytes("/path/to/image3.png");
            ByteArrayInputStream inputStream = new ByteArrayInputStream(streamData);
            GenericRow row3 = GenericRow.of(
                    3,
                    BinaryString.fromString("image3"),
                    Blob.fromInputStream(() -> SeekableInputStream.wrap(inputStream))
            );
            write.write(row3);

            // Method 4: Create blob from HTTP URL
            GenericRow row4 = GenericRow.of(
                    4,
                    BinaryString.fromString("remote_image"),
                    Blob.fromHttp("https://example.com/image.png")
            );
            write.write(row4);

            // Commit all writes
            commit.commit(write.prepareCommit());
        }

        System.out.println("Successfully wrote 4 rows with blob data");
    }

    private static void readBlobData(Table table) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());

        reader.forEachRemaining(row -> {
            int id = row.getInt(0);
            String name = row.getString(1).toString();
            Blob blob = row.getBlob(2);

            // Method 1: Read blob as byte array (loads entire blob into memory)
            byte[] data = blob.toData();
            System.out.println("Row " + id + ": " + name + ", blob size: " + data.length);

            // Method 2: Read blob as stream (better for large blobs)
            try (SeekableInputStream in = blob.newInputStream()) {
                // Process stream without loading entire blob into memory
                byte[] buffer = new byte[1024];
                int bytesRead;
                long totalSize = 0;
                while ((bytesRead = in.read(buffer)) != -1) {
                    totalSize += bytesRead;
                    // Process buffer...
                }
                System.out.println("Streamed " + totalSize + " bytes");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private static byte[] loadImageBytes(String path) throws Exception {
        return Files.readAllBytes(java.nio.file.Path.of(path));
    }
}
```

### Inserting Blob Data

```java
// From byte array (data already in memory)
Blob blob = Blob.fromData(imageBytes);

// From local file system
Blob blob = Blob.fromLocal("/path/to/image.png");

// From any FileIO (supports HDFS, S3, OSS, etc.)
FileIO fileIO = FileIO.get(new Path("s3://bucket"), catalogContext);
Blob blob = Blob.fromFile(fileIO, "s3://bucket/path/to/image.png");

// From FileIO with offset and length (read partial file)
Blob blob = Blob.fromFile(fileIO, "s3://bucket/large-file.bin", 1024, 2048);

// From HTTP/HTTPS URL
Blob blob = Blob.fromHttp("https://example.com/image.png");

// From InputStream supplier (lazy loading)
Blob blob = Blob.fromInputStream(() -> new FileInputStream("/path/to/image.png"));

// From BlobDescriptor (reconstruct blob reference from descriptor)
BlobDescriptor descriptor = new BlobDescriptor("s3://bucket/path/to/image.png", 0, 1024);
UriReader uriReader = UriReader.fromFile(fileIO);
Blob blob = Blob.fromDescriptor(uriReader, descriptor);
```

### Querying Blob Data

```java
// Get blob from row (column index 2 in this example)
Blob blob = row.getBlob(2);

// Read as byte array (simple but loads entire blob into memory)
byte[] data = blob.toData();

// Read as stream (recommended for large blobs)
try (SeekableInputStream in = blob.newInputStream()) {
    // SeekableInputStream supports random access
    in.seek(100);  // Jump to position 100
    byte[] buffer = new byte[1024];
    int bytesRead = in.read(buffer);
}

// Get blob descriptor (for reference-based blobs)
// Note: Only works for BlobRef, not BlobData
BlobDescriptor descriptor = blob.toDescriptor();
String uri = descriptor.uri();      // e.g., "s3://bucket/path/to/blob"
long offset = descriptor.offset();  // Starting position in the file
long length = descriptor.length();  // Length of the blob data
```

### Descriptor-Aware Write Behavior

Paimon write path is descriptor-aware automatically:

1. For blob fields stored in `.blob` files, input can be either blob bytes or a `BlobDescriptor`.
2. For fields configured in `blob.stored-descriptor-fields`, Paimon stores descriptor bytes inline in data files (no `.blob` files for those fields), and input must be a descriptor.
3. This behavior does not depend on `blob-as-descriptor`.

```java
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;

public class BlobDescriptorExample {

    public static void main(String[] args) throws Exception {
        Path warehouse = new Path("s3://my-bucket/paimon-warehouse");
        CatalogContext catalogContext = CatalogContext.create(warehouse);
        Catalog catalog = CatalogFactory.createCatalog(catalogContext);
        catalog.createDatabase("my_db", true);

        // Create table: store "video" as descriptor bytes inline
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .column("video", DataTypes.BLOB())
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .option(CoreOptions.BLOB_STORED_DESCRIPTOR_FIELDS.key(), "video")
                .build();

        Identifier tableId = Identifier.create("my_db", "video_table");
        catalog.createTable(tableId, schema, true);
        Table table = catalog.getTable(tableId);

        // Write blob using descriptor reference
        writeLargeBlobWithDescriptor(table);

        // Read blob data
        readBlobData(table);
    }

    private static void writeLargeBlobWithDescriptor(Table table) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();

        try (BatchTableWrite write = writeBuilder.newWrite();
             BatchTableCommit commit = writeBuilder.newCommit()) {

            // For a very large file (e.g., 2GB video), instead of loading into memory:
            //   byte[] hugeVideo = Files.readAllBytes(...);  // This would cause OutOfMemoryError!
            //
            // Create a descriptor reference to external blob
            String externalUri = "s3://my-bucket/videos/large_video.mp4";
            long fileSize = 2L * 1024 * 1024 * 1024;  // 2GB

            BlobDescriptor descriptor = new BlobDescriptor(externalUri, 0, fileSize);
            // file io should be accessable to externalUri
            FileIO fileIO = Table.fileIO();
            UriReader uriReader = UriReader.fromFile(fileIO);
            Blob blob = Blob.fromDescriptor(uriReader, descriptor);

            GenericRow row = GenericRow.of(
                    1,
                    BinaryString.fromString("large_video"),
                    blob);
            write.write(row);

            commit.commit(write.prepareCommit());
        }

        System.out.println("Successfully wrote large blob using descriptor reference");
    }

    private static void readBlobData(Table table) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());

        reader.forEachRemaining(row -> {
            int id = row.getInt(0);
            String name = row.getString(1).toString();
            Blob blob = row.getBlob(2);

            // Field is configured in blob.stored-descriptor-fields, so descriptor is stored inline
            BlobDescriptor descriptor = blob.toDescriptor();
            System.out.println("Row " + id + ": " + name);
            System.out.println("  Blob URI: " + descriptor.uri());
            System.out.println("  Length: " + descriptor.length());
        });
    }
}
```

**Reading blob data with different output modes:**

The `blob-as-descriptor` option affects only read output:

```sql
-- When blob-as-descriptor = true: Returns BlobDescriptor bytes (reference to Paimon blob file)
ALTER TABLE video_table SET ('blob-as-descriptor' = 'true');
SELECT * FROM video_table;  -- Returns serialized BlobDescriptor

-- When blob-as-descriptor = false: Returns actual blob bytes
ALTER TABLE video_table SET ('blob-as-descriptor' = 'false');
SELECT * FROM video_table;  -- Returns actual blob bytes from Paimon storage
```

### Descriptor Fields: Reuse by Descriptor (No Copy)

If you want downstream tables to **reuse** upstream blob files (no copying and no new <code>.blob</code> files), configure the target blob field(s):

```sql
'blob.stored-descriptor-fields' = 'image'
```

For these configured fields, Paimon stores only serialized <code>BlobDescriptor</code> bytes in normal data files. Reading the blob follows the descriptor URI to access bytes, and writing requires descriptor input for those fields.

## Limitations

1. **Append Table Only**: Blob type is designed for append-only tables. Primary key tables are not supported.
2. **No Predicate Pushdown**: Blob columns cannot be used in filter predicates.
3. **No Statistics**: Statistics collection is not supported for blob columns.
4. **Required Options**: `row-tracking.enabled` and `data-evolution.enabled` must be set to `true`.

## Best Practices

1. **Use Column Projection**: Always select only the columns you need. Avoid `SELECT *` if you don't need blob data.

2. **Set Appropriate Target File Size**: Configure `blob.target-file-size` based on your blob sizes. Larger values mean fewer files but larger individual files.

3. **Use Descriptor Fields When Reusing External Blob Files**: Configure `blob.stored-descriptor-fields` for fields that should keep descriptor references instead of writing new `.blob` files.

4. **Use Partitioning**: Partition your blob tables by date or other dimensions to improve query performance and data management.

```sql
CREATE TABLE partitioned_blob_table (
    id INT,
    name STRING,
    image BYTES,
    dt STRING
) PARTITIONED BY (dt) WITH (
    'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true',
    'blob-field' = 'image'
);
```
