---
title: "Mosaic"
weight: 9
type: docs
aliases:
- /concepts/spec/mosaic.html
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

# Mosaic File Format

Mosaic is a columnar-bucket hybrid format optimized for wide tables (10,000+ columns). Columns are hashed into buckets
by name, stored row-oriented within each bucket, and independently compressed. This enables efficient projection
pushdown at bucket granularity — reading 10 columns out of 10,000 only decompresses the buckets that contain those
10 columns.

## File Layout

```
+--------------------------------------------+
|  Row Group 0: Bucket Data                  |
|    [Bucket 0 compressed block]             |
|    [Bucket 3 compressed block]             |
|    ...  (only non-empty buckets)           |
+--------------------------------------------+
|  Row Group 1: Bucket Data                  |
|    ...                                     |
+--------------------------------------------+
|  Schema Block                              |
|    [4 bytes: uncompressed size (BE int)]   |
|    [schema data (possibly compressed)]     |
+--------------------------------------------+
|  Row Group Index (varint encoded)          |
+--------------------------------------------+
|  Footer (32 bytes, fixed)                  |
+--------------------------------------------+
```

## Footer (32 bytes, big-endian)

| Offset | Size | Field             | Description                        |
|--------|------|-------------------|------------------------------------|
| 0      | 8    | indexOffset        | Absolute offset of Row Group Index |
| 8      | 8    | schemaBlockOffset  | Absolute offset of Schema Block    |
| 16     | 4    | numBuckets         | Total number of buckets            |
| 20     | 4    | numRowGroups       | Total number of row groups         |
| 24     | 1    | compression        | 0 = none, 1 = zstd                |
| 25     | 1    | version            | Format version (currently 1)       |
| 26     | 2    | (reserved)         | Padding, set to 0                  |
| 28     | 4    | magic              | `MOSA` (0x4D4F5341)               |

## Row Group Index

Varint-encoded, only non-empty buckets are stored. For each row group:

```
varint   numRows
varint   nonEmptyCount
repeated nonEmptyCount times:
    varint    bucketId
    8 bytes   bucketOffset       (big-endian, absolute file offset)
    varint    compressedSize
    varint    uncompressedSize
```

## Schema Block

Prefixed with a 4-byte big-endian int (uncompressed size), followed by the schema data (compressed with the file's
compression method):

```
varint   numColumns
varint   numBuckets
repeated numColumns times:
    varint   fieldId
    varint   bucketId
    varint   indexInBucket
    varint   nameLength
    bytes    name (UTF-8)
    TypeDescriptor
```

### TypeDescriptor

```
1 byte   typeId
1 byte   nullable      (0 = not null, 1 = nullable)
[type-specific params]
```

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">typeId</th>
        <th class="text-left">Type</th>
        <th class="text-left">Params</th>
      </tr>
    </thead>
    <tbody>
    <tr><td>0</td><td>BOOLEAN</td><td>(none)</td></tr>
    <tr><td>1</td><td>TINYINT</td><td>(none)</td></tr>
    <tr><td>2</td><td>SMALLINT</td><td>(none)</td></tr>
    <tr><td>3</td><td>INTEGER</td><td>(none)</td></tr>
    <tr><td>4</td><td>BIGINT</td><td>(none)</td></tr>
    <tr><td>5</td><td>FLOAT</td><td>(none)</td></tr>
    <tr><td>6</td><td>DOUBLE</td><td>(none)</td></tr>
    <tr><td>7</td><td>DATE</td><td>(none)</td></tr>
    <tr><td>8</td><td>CHAR</td><td>varint length</td></tr>
    <tr><td>9</td><td>VARCHAR</td><td>varint length</td></tr>
    <tr><td>10</td><td>STRING</td><td>(none) — VARCHAR with MAX_LENGTH</td></tr>
    <tr><td>11</td><td>BINARY</td><td>varint length</td></tr>
    <tr><td>12</td><td>VARBINARY</td><td>varint length</td></tr>
    <tr><td>13</td><td>BYTES</td><td>(none) — VARBINARY with MAX_LENGTH</td></tr>
    <tr><td>14</td><td>DECIMAL</td><td>varint precision, varint scale</td></tr>
    <tr><td>15</td><td>TIME</td><td>varint precision</td></tr>
    <tr><td>16</td><td>TIMESTAMP</td><td>varint precision</td></tr>
    <tr><td>17</td><td>TIMESTAMP_LTZ</td><td>varint precision</td></tr>
    </tbody>
</table>

Complex types (ARRAY, MAP, ROW, etc.), VARIANT, and BLOB are not supported.

## Bucket Data

Each bucket's data block (before compression):

```
+----------------------------------+
|  Row Size Table (delta varints)  |
+----------------------------------+
|  Row 0                           |
|  Row 1                           |
|  ...                             |
+----------------------------------+
```

**Row Size Table**: Delta-varint encoded. Each entry is the byte size of one row (null bitmap + column values).
Used to compute absolute offsets for row access within the bucket.

**Row format**:

```
[null bitmap]    ceil(numColumnsInBucket / 8) bytes
                 bit i = 1 means column i is null
[column values]  non-null columns written in order, no padding
```

## Column Value Encoding

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Type</th>
        <th class="text-left">Encoding</th>
      </tr>
    </thead>
    <tbody>
    <tr><td>BOOLEAN</td><td>1 byte (0 or 1)</td></tr>
    <tr><td>TINYINT</td><td>1 byte</td></tr>
    <tr><td>SMALLINT</td><td>2 bytes big-endian</td></tr>
    <tr><td>INTEGER / DATE / TIME</td><td>4 bytes big-endian</td></tr>
    <tr><td>BIGINT</td><td>8 bytes big-endian</td></tr>
    <tr><td>FLOAT</td><td>4 bytes IEEE 754</td></tr>
    <tr><td>DOUBLE</td><td>8 bytes IEEE 754</td></tr>
    <tr><td>DECIMAL (compact, precision &le; 18)</td><td>8 bytes big-endian (unscaled long)</td></tr>
    <tr><td>DECIMAL (large, precision &gt; 18)</td><td>varint length + unscaled BigInteger bytes</td></tr>
    <tr><td>TIMESTAMP (precision &le; 3)</td><td>8 bytes (epoch millis)</td></tr>
    <tr><td>TIMESTAMP (precision &gt; 3)</td><td>8 bytes (epoch millis) + 4 bytes (nanos of millis)</td></tr>
    <tr><td>CHAR / VARCHAR / STRING</td><td>varint length + UTF-8 bytes</td></tr>
    <tr><td>BINARY / VARBINARY / BYTES</td><td>varint length + raw bytes</td></tr>
    </tbody>
</table>

## Column-to-Bucket Assignment

Columns are assigned to buckets by hashing the column name:

```
bucketId = Math.floorMod(fieldName.hashCode(), numBuckets)
```

Default number of buckets: `min(100, numColumns)`.

## Compression

Compression is applied independently to each bucket data block and to the schema block. Supported methods:

- `0` — No compression
- `1` — Zstd (configurable level)

## Benchmark

Test setup: 10,000 columns (90% STRING, 10% INT), column names ~80 bytes each, Zstd compression (level 9).

### File Size (10 rows)

| Format  | Size       | vs Mosaic |
|---------|------------|-----------|
| Parquet | 9,696 KB   | 14.8x     |
| ORC     | 6,377 KB   | 9.7x      |
| Mosaic  | 654 KB     | 1x        |

Mosaic's compact binary schema and bucket-level compression produce significantly smaller files for wide tables,
since columnar formats like Parquet and ORC store per-column metadata that scales linearly with column count.

### Projection Read (500 rows, ~57 MB Parquet)

| Projected Columns | Parquet    | ORC        | Mosaic    |
|-------------------|------------|------------|-----------|
| 10 / 10,000       | 53,170 us  | 72,729 us  | 25,081 us |
| 1 / 10,000        | 50,919 us  | 70,712 us  | 2,374  us |

File size — Parquet: 57.4 MB, ORC: 95.4 MB, Mosaic: 13.5 MB

### Projection Read (4,500 rows, ~458 MB Parquet)

| Projected Columns | Parquet     | ORC        | Mosaic     |
|-------------------|-------------|------------|------------|
| 10 / 10,000       | 369,627 us  | 89,344 us  | 67,314 us  |
| 1 / 10,000        | 360,458 us  | 81,934 us  | 26,924 us  |

File size — Parquet: 458.4 MB, ORC: 827.9 MB, Mosaic: 115.8 MB

When projecting a small subset of columns, Mosaic only decompresses the buckets containing the requested columns,
avoiding I/O on the remaining data. Mosaic's file size advantage is consistent across both scales (~4x smaller than
Parquet, ~7x smaller than ORC), and single-column projection read performance is comparable to ORC and significantly
faster than Parquet.

## Limitations

1. Complex types (ARRAY, MAP, MULTISET, ROW) are not supported.
2. Mosaic format is designed for wide tables and may not be efficient for narrow tables with few columns.
