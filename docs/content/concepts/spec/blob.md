---
title: "Blob"
weight: 10
type: docs
aliases:
- /concepts/spec/blob.html
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

# Blob Type

## Overview

The `BLOB` (Binary Large Object) type is a data type designed for storing multimodal data such as images, videos, audio files, and other large binary objects in Paimon tables. Unlike traditional `BYTES` type which stores binary data inline with other columns, `BLOB` type stores large binary data in separate files and maintains references to them, providing better performance for large objects.

## Use Cases

The Blob type is ideal for:
- **Image Storage**: Store product images, user avatars, medical imaging data
- **Video Content**: Store video clips, surveillance footage, multimedia content
- **Audio Files**: Store voice recordings, music files, podcast episodes
- **Document Storage**: Store PDF documents, office files, large text files
- **Machine Learning**: Store embeddings, model weights, feature vectors
- **Any Large Binary Data**: Any data that is too large to store efficiently inline

## Architecture

### Storage Layout

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

## Configuration Options

### Table Options

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
      <td>Specifies the column name that should be stored as a blob. This is used when you want to treat a BYTES column as a BLOB. Currently, only one blob field per table is supported.</td>
    </tr>
    <tr>
      <td><h5>blob-as-descriptor</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>When set to true, the blob field input is treated as a serialized BlobDescriptor. Paimon reads from the descriptor's URI and streams the data into Paimon's blob files in small chunks, avoiding loading the entire blob into memory. This is useful for writing very large blobs that cannot fit in memory. When reading, if set to true, returns the BlobDescriptor bytes; if false, returns actual blob bytes.</td>
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

## SQL Usage

### Creating a Table with Blob Field

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

### Using Blob Descriptor Mode

When you want to store references from external blob data (stored in object storage) without loading the entire blob into memory, you can use the `blob-as-descriptor` option:

```sql
-- Create table in descriptor mode
CREATE TABLE blob_table (
    id INT,
    name STRING,
    image BYTES
) WITH (
    'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true',
    'blob-field' = 'image',
    'blob-as-descriptor' = 'true'
);

-- Insert with serialized BlobDescriptor bytes
-- The BlobDescriptor contains: version (1 byte) + uri_length (4 bytes) + uri_bytes + offset (8 bytes) + length (8 bytes)
-- Paimon will read from the descriptor's URI and stream data into Paimon storage
INSERT INTO blob_table VALUES (1, 'photo', X'<serialized_blob_descriptor_hex>');

-- Toggle this setting to control read output format:
ALTER TABLE blob_table SET ('blob-as-descriptor' = 'false');
SELECT * FROM blob_table;  -- Returns actual blob bytes from Paimon storage
```

## Java API Usage

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

### Creating Blob from Different Sources

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

### Reading Blob Data

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

### Using Blob Descriptor Mode (blob-as-descriptor)

The `blob-as-descriptor` option enables **memory-efficient writing** for very large blobs. When enabled, you provide a `BlobDescriptor` pointing to external data, and Paimon streams the data from the external source into Paimon's `.blob` files without loading the entire blob into memory.

**How it works:**
1. **Writing**: You provide a serialized `BlobDescriptor` (containing URI, offset, length) as the blob field value
2. **Paimon copies the data**: Paimon reads from the descriptor's URI in small chunks (e.g., 1024 bytes at a time) and writes to Paimon's `.blob` files
3. **Data is stored in Paimon**: The blob data IS copied to Paimon storage, but in a streaming fashion

**Key benefit:**
- **Memory efficiency**: For very large blobs (e.g., gigabyte-sized videos), you don't need to load the entire file into memory. Paimon streams the data incrementally.

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

        // Create table with blob-as-descriptor enabled
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .column("video", DataTypes.BLOB())
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .option(CoreOptions.BLOB_AS_DESCRIPTOR.key(), "true")  // This is not necessary in java api
                .build();

        Identifier tableId = Identifier.create("my_db", "video_table");
        catalog.createTable(tableId, schema, true);
        Table table = catalog.getTable(tableId);

        // Write large blob using descriptor (memory-efficient)
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
            // Use BlobDescriptor to let Paimon stream the data:
            String externalUri = "s3://my-bucket/videos/large_video.mp4";
            long fileSize = 2L * 1024 * 1024 * 1024;  // 2GB

            BlobDescriptor descriptor = new BlobDescriptor(externalUri, 0, fileSize);
            // file io should be accessable to externalUri
            FileIO fileIO = Table.fileIO();
            UriReader uriReader = UriReader.fromFile(fileIO);
            Blob blob = Blob.fromDescriptor(uriReader, descriptor);

            // Write the serialized descriptor as blob data
            // Paimon will read from the URI and copy data to .blob files in chunks
            GenericRow row = GenericRow.of(
                    1,
                    BinaryString.fromString("large_video"),
                    blob);
            write.write(row);

            commit.commit(write.prepareCommit());
        }

        System.out.println("Successfully wrote large blob using descriptor mode");
    }

    private static void readBlobData(Table table) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());

        reader.forEachRemaining(row -> {
            int id = row.getInt(0);
            String name = row.getString(1).toString();
            Blob blob = row.getBlob(2);

            // The blob data is now stored in Paimon's .blob files
            // blob.toDescriptor() returns a descriptor pointing to Paimon's internal storage
            BlobDescriptor descriptor = blob.toDescriptor();
            System.out.println("Row " + id + ": " + name);
            System.out.println("  Paimon blob URI: " + descriptor.uri());
            System.out.println("  Length: " + descriptor.length());
        });
    }
}
```

**Reading blob data with different modes:**

The `blob-as-descriptor` option also affects how data is returned when reading:

```sql
-- When blob-as-descriptor = true: Returns BlobDescriptor bytes (reference to Paimon blob file)
ALTER TABLE video_table SET ('blob-as-descriptor' = 'true');
SELECT * FROM video_table;  -- Returns serialized BlobDescriptor

-- When blob-as-descriptor = false: Returns actual blob bytes
ALTER TABLE video_table SET ('blob-as-descriptor' = 'false');
SELECT * FROM video_table;  -- Returns actual blob bytes from Paimon storage
```

## Limitations

1. **Single Blob Field**: Currently, only one blob field per table is supported.
2. **Append Table Only**: Blob type is designed for append-only tables. Primary key tables are not supported.
3. **No Predicate Pushdown**: Blob columns cannot be used in filter predicates.
4. **No Statistics**: Statistics collection is not supported for blob columns.
5. **Required Options**: `row-tracking.enabled` and `data-evolution.enabled` must be set to `true`.

## Best Practices

1. **Use Column Projection**: Always select only the columns you need. Avoid `SELECT *` if you don't need blob data.

2. **Set Appropriate Target File Size**: Configure `blob.target-file-size` based on your blob sizes. Larger values mean fewer files but larger individual files.

3. **Consider Descriptor Mode**: For very large blobs that cannot fit in memory, use `blob-as-descriptor` mode to stream data from external sources into Paimon without loading the entire blob into memory.

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
