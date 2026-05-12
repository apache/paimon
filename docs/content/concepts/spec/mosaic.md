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

Mosaic is a columnar-bucket hybrid format optimized for wide tables (10,000+ columns). Columns are sorted by name and
evenly distributed into buckets using range-based assignment, stored column-oriented within each bucket, and
independently compressed. This enables efficient projection pushdown at bucket granularity — reading 10 columns out of
10,000 only decompresses the buckets that contain those 10 columns. Range-based assignment ensures that columns with
similar name prefixes land in the same bucket, improving both compression ratio and projection locality.

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
compression method).

Columns are serialized in **name-sorted order**. Column names are compressed using one of two encodings, chosen
dynamically by the writer based on which produces smaller output:

- **Front coding** (mode 0): Each name shares a prefix with the previous name; only the suffix is stored. The same
  technique used by Lucene, LevelDB, and RocksDB.
- **BPE + front coding** (mode 1): Byte Pair Encoding is applied first to compress repeated substrings across column
  names (e.g., `_status`, `_value`, `_timestamp`), then front coding is applied to the BPE-encoded names. BPE uses
  token bytes 0x80-0xFF (up to 128 merge rules), and is only applicable when all column names are ASCII-only.

### Schema Block Layout

```
varint   numColumns
varint   numBuckets
1 byte   nameEncoding          (0 = front coding, 1 = BPE + front coding)

--- if nameEncoding == 1 (BPE) ---
varint   numRules
repeated numRules times:
    1 byte   left               (left token of merge rule)
    1 byte   right              (right token of merge rule)

--- per column (repeated numColumns times, name-sorted order) ---
varint   fieldId
varint   sharedPrefixLen       (bytes shared with previous column name)
varint   suffixLen             (bytes of new suffix)
bytes    suffix                (suffixLen bytes, raw or BPE-encoded)
TypeDescriptor
```

The first column has `sharedPrefixLen = 0`. To reconstruct a column name, take the first `sharedPrefixLen` bytes from
the previous name and append the suffix. If BPE is used, decode the reconstructed byte sequence by recursively expanding
tokens >= 0x80 using the merge rules.

Bucket assignment is not stored per column — it is derived from the column's position in the sorted order (see
Column-to-Bucket Assignment below).

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

Each bucket is stored as a **column-oriented** block. Within a bucket, each column is independently encoded using one
of four encodings (PLAIN, CONST, DICT, or ALL_NULL), chosen automatically based on the column's value distribution.

### Bucket Block Layout (before compression)

```
+--------------------------------------------+
|  Encoding Flags                            |
|    2 bits per column, packed into bytes     |
+--------------------------------------------+
|  Has-Nulls Flags                           |
|    1 bit per column, packed into bytes      |
+--------------------------------------------+
|  Const Metadata (CONST columns only)       |
|    serialized value for each CONST column   |
+--------------------------------------------+
|  Dict Metadata (DICT columns only)         |
|    for each DICT column:                   |
|      varint    numEntries                  |
|      repeated: serialized value per entry  |
+--------------------------------------------+
|  Null Bitmaps                              |
|    ceil(numRows/8) bytes per column        |
|    (only for columns with nulls,           |
|     excluding ALL_NULL columns)            |
+--------------------------------------------+
|  Column Data                               |
|    PLAIN: raw serialized values            |
|    DICT: bit-packed indices (see below)    |
|    CONST/ALL_NULL: (nothing)               |
+--------------------------------------------+
```

**Encoding Flags**: 2 bits per column, packed left-to-right. Encoding values:

| Value | Encoding | Description |
|-------|----------|-------------|
| 0     | PLAIN    | Raw serialized values for each non-null cell |
| 1     | CONST    | All non-null values are identical; the single value is stored in metadata |
| 2     | DICT     | 2-255 distinct values; each non-null cell stores a bit-packed dictionary index |
| 3     | ALL_NULL | Every cell in this column is null; no data or null bitmap stored |

**Has-Nulls Flags**: 1 bit per column. If set, a null bitmap exists for that column. ALL_NULL columns always have
this flag cleared (no bitmap is stored for them).

**Null Bitmap**: `ceil(numRows / 8)` bytes per column. Bit `i` = 1 means row `i` is null. Only present for columns
where has-nulls flag is set.

### Column Encoding Selection

The encoding for each column is chosen automatically during writing based on value distribution and cost:

- **ALL_NULL**: 0 non-null values
- **CONST**: exactly 1 distinct non-null value (any number of nulls allowed)
- **DICT**: 2-255 distinct non-null values, **and** the dictionary-encoded size is smaller than plain — the writer
  compares `varint(numEntries) + sum(entryBytes) + ceil(nonNullCount * bitWidth / 8)` against the raw value buffer size
- **PLAIN**: 256+ distinct values, dict tracking was abandoned, or dict encoding would be larger than plain

CONST detection is independent of dictionary tracking — it uses a lightweight byte comparison against the first non-null
value, so it works for all types and value sizes (including long strings).

Dictionary encoding works for all data types including variable-width types (VARCHAR, VARBINARY, DECIMAL). The writer
uses primitive long keys for fixed-width types (<=8 bytes) and byte-array keys for variable-width types. Variable-width
dictionary tracking is bounded by a configurable cumulative byte budget (default 32KB) and abandoned when cardinality
exceeds 255 or total dictionary entry bytes exceed the budget.

### Bit-packed Dictionary Indices

Dictionary indices are bit-packed using `bitWidth = ceil(log2(numEntries))` bits per non-null cell, packed LSB-first
within each byte. The reader derives `bitWidth` from `numEntries` (already stored in dict metadata).

Examples: 2 distinct values -> 1 bit/cell, 4 -> 2 bits, 16 -> 4 bits, 256 -> 8 bits, 1000 -> 10 bits.

For a column with 1500 non-null rows and 2 distinct values, bit-packed indices use 188 bytes instead of 1500 bytes
(8x savings pre-compression).

## Value Serialization

Values are serialized in the same format for PLAIN data, CONST metadata, and DICT entries:

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
    <tr><td>FLOAT</td><td>4 bytes IEEE 754 (big-endian)</td></tr>
    <tr><td>DOUBLE</td><td>8 bytes IEEE 754 (big-endian)</td></tr>
    <tr><td>DECIMAL (compact, precision &le; 18)</td><td>8 bytes big-endian (unscaled long)</td></tr>
    <tr><td>DECIMAL (large, precision &gt; 18)</td><td>varint length + unscaled BigInteger bytes</td></tr>
    <tr><td>TIMESTAMP (precision &le; 3)</td><td>8 bytes (epoch millis, big-endian)</td></tr>
    <tr><td>TIMESTAMP (precision &gt; 3)</td><td>8 bytes (epoch millis) + 4 bytes (nanos of millis)</td></tr>
    <tr><td>CHAR / VARCHAR / STRING</td><td>varint length + UTF-8 bytes</td></tr>
    <tr><td>BINARY / VARBINARY / BYTES</td><td>varint length + raw bytes</td></tr>
    </tbody>
</table>

## Column-to-Bucket Assignment

Columns are sorted by name, then evenly distributed across buckets using range-based assignment:

```
bucketId = sortedPosition * numBuckets / numColumns
```

This ensures columns with similar name prefixes (e.g., `engine_coolant_*`, `engine_oil_*`) land in the same bucket,
which improves both compression ratio (related values compress better together) and projection locality (querying
related columns touches fewer buckets).

Default number of buckets: `min(100, numColumns)`.

The bucket assignment is deterministic and derived from the sorted column order — it is not stored in the file. The
reader reconstructs bucket assignments from the schema block, where columns are always in name-sorted order.

## Compression

Compression is applied independently to each bucket data block and to the schema block. Supported methods:

- `0` — No compression
- `1` — Zstd (configurable level)

## Limitations

1. Complex types (ARRAY, MAP, MULTISET, ROW) are not supported.
2. Mosaic format is designed for wide tables and may not be efficient for narrow tables with few columns.
