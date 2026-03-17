---
title: "Vector Storage"
weight: 7
type: docs
aliases:
- /append-table/vector-storage.html
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

# Vector Storage

## Overview

With the explosive growth of AI scenarios, vector storage has become increasingly important.

Paimon provides optimized storage solutions specifically designed for vector data to meet the needs of various scenarios.

## Vector Data Type

Vector data comes in many types, among which dense vectors are the most commonly used. They are typically expressed as fixed-length, densely packed arrays, generally without `null` elements.

Paimon supports defining columns of type `VECTOR<t, n>`, which represents a fixed-length, dense vector column, where:
 - **`t`**: The element type of the vector. Currently supports seven primitive types: `BOOLEAN`, `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE`;
 - **`n`**: The vector dimension, must be a positive integer not exceeding `2,147,483,647`;
 - **`null constraint`**: `VECTOR` type supports defining `NOT NULL` or the default nullable. However, if a specific `VECTOR` value itself is not `null`, its elements are not allowed to be `null`.

Compared to variable-length arrays, these features make dense vectors more concise in storage and memory representation, with benefits including:
 - More natural semantic constraints, preventing mismatched lengths, `null` elements, and other anomalies at the data storage layer;
 - Better point-lookup performance, eliminating offset array storage and access;
 - Closer alignment with type representations in specialized vector engines, often avoiding memory copies and type conversions during queries.

Example: Define a table with a `VECTOR` column using Java API and write one row of data.
```java
public class CreateTableWithVector {

    public static void main(String[] args) throws Exception {
        // Schema
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("id", DataTypes.BIGINT());
        schemaBuilder.column("embed", DataTypes.VECTOR(3, DataTypes.FLOAT()));
        schemaBuilder.option(CoreOptions.FILE_FORMAT.key(), "lance");
        schemaBuilder.option(CoreOptions.FILE_COMPRESSION.key(), "none");
        Schema schema = schemaBuilder.build();

        // Create catalog
        String database = "default";
        String tempPath = System.getProperty("java.io.tmpdir") + UUID.randomUUID();
        Path warehouse = new Path(TraceableFileIO.SCHEME + "://" + tempPath);
        Identifier identifier = Identifier.create("default", "my_table");
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(warehouse))) {

            // Create table
            catalog.createDatabase(database, true);
            catalog.createTable(identifier, schema, true);
            FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);

            // Write data
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            InternalVector vector = BinaryVector.fromPrimitiveArray(new float[] {1.0f, 2.0f, 3.0f});
            try (BatchTableWrite batchTableWrite = builder.newWrite()) {
                try (BatchTableCommit commit = builder.newCommit()) {
                    batchTableWrite.write(GenericRow.of(1L, vector));
                    commit.commit(batchTableWrite.prepareCommit());
                }
            }

            // Read data
            ReadBuilder readBuilder = table.newReadBuilder();
            TableScan.Plan plan = readBuilder.newScan().plan();
            try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
                reader.forEachRemaining(row -> {
                    float[] readVector = row.getVector(1).toFloatArray();
                    System.out.println(Arrays.toString(readVector));
                });
            }
        }
    }
}
```

**Notes**:
 - Columns of `VECTOR` type cannot be used as primary key columns, partition columns, or for sorting.

## Engine-Level Representation

Since engine layers typically don't have dedicated vector types, to support `VECTOR` type in engine SQL, Paimon provides a separate configuration to convert the engine's `ARRAY` type to Paimon's `VECTOR` type.

Usage:
 - **`'vector-field'`**: Declare columns as `VECTOR` type, multiple columns separated by commas (`,`);
 - **`'field.{field-name}.vector-dim'`**: Declare the dimension of the vector column.

Example: Define a table with a `VECTOR` column using Flink SQL.
```sql
CREATE TABLE IF NOT EXISTS ts_table (
    id BIGINT,
    embed1 ARRAY<FLOAT>,
    embed2 ARRAY<FLOAT>
) WITH (
    'file.format' = 'lance',
    'vector-field' = 'embed1,embed2',
    'field.embed1.vector-dim' = '128',
    'field.embed2.vector-dim' = '768'
);
```

**Notes**:
 - When defining `vector-field` columns, you must provide the vector dimension; otherwise, the CREATE TABLE statement will fail;
 - Currently, only Flink SQL supports this configuration; other engines have not been implemented yet.

## Dedicated File Format for Vector

When mapping `VECTOR` type to the file format layer, the ideal storage format is `FixedSizeList`. Currently, this is only supported for certain file formats (such as `lance`) through the `paimon-arrow` integration. This means that to use `VECTOR` type, you must specify a particular format via `file.format`, which has a global impact. In particular, this may be unfavorable for scalars and multimodal (Blob) data.

Therefore, Paimon provides a solution to store vector columns separately within Data Evolution tables.

Layout:
```
table/
├── bucket-0/
│   ├── data-uuid-0.parquet      # Contains id, name columns
│   ├── data-uuid-1.blob         # Contains blob data
│   ├── data-uuid-2.vector.lance # Contains vector data using lance format
│   └── ...
├── manifest/
├── schema/
└── snapshot/
```

Usage:
 - **`vector.file.format`**: Store `VECTOR` type columns separately in the specified file format;
 - **`vector.target-file-size`**: If stored separately, specifies the target file size for vector data, defaulting to `10 * 'target-file-size'`.

Example: Store `VECTOR` columns separately using Flink SQL.
```sql
CREATE TABLE IF NOT EXISTS ts_table (
    id BIGINT,
    embed ARRAY<FLOAT>
) WITH (
    'file.format' = 'parquet',
    'vector.file.format' = 'lance',
    'vector-field' = 'embed',
    'field.embed.vector-dim' = '128',
    'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true'
);
```
