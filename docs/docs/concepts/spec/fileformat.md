---
title: "File Format"
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

# File Format

Paimon stores records in files using the configured `file.format`. Parquet is the default.
Choose a format supported by your table features and the engines that will read the table.

Use this page for format-specific type mappings and configuration. The
[Data Files](./datafile) specification describes the surrounding partition, bucket, and record
layout; [Data Types](../data-types) describes Paimon's logical types.

| Format | Reference |
| --- | --- |
| [Parquet](#parquet) | Default columnar format and type mappings |
| [Avro](#avro) | Row-oriented format and type mappings |
| [ORC](#orc) | Columnar format and type mappings |
| [CSV](#csv) | Delimited text and type mappings |
| [Text](#text) | Text records and line delimiters |
| [JSON](#json) | JSON records and type mappings |
| [Lance](#lance) | Format integration for ML and vector workloads |
| [Vortex](#vortex) | Columnar format integration |
| [Mosaic](#mosaic) | Column bucketing for wide tables |
| [Row](#row) | Row-oriented blocks with row-number lookup; see the [binary specification](./rowformat) |
| [BLOB](#blob) | Binary-object storage and [video](#video) handling |

## Parquet

Parquet is the default file format for Paimon.

The following table lists the type mapping from Paimon type to Parquet type.

| Paimon Type | Parquet type | Parquet logical type |
| --- | --- | --- |
| CHAR / VARCHAR / STRING | BINARY | UTF8 |
| BOOLEAN | BOOLEAN |  |
| BINARY / VARBINARY | BINARY |  |
| GEOMETRY(crs) | BINARY | GEOMETRY(crs) |
| GEOGRAPHY(crs, algorithm) | BINARY | GEOGRAPHY(crs, algorithm) |
| DECIMAL(P, S) | P <= 9: INT32, P <= 18: INT64, P > 18: FIXED_LEN_BYTE_ARRAY | DECIMAL(P, S) |
| TINYINT | INT32 | INT_8 |
| SMALLINT | INT32 | INT_16 |
| INT | INT32 |  |
| BIGINT | INT64 |  |
| FLOAT | FLOAT |  |
| DOUBLE | DOUBLE |  |
| DATE | INT32 | DATE |
| TIME | INT32 | TIME_MILLIS |
| TIMESTAMP(P) | P <= 3: INT64, P <= 6: INT64, P > 6: INT96 | P <= 3: MILLIS, P <= 6: MICROS, P > 6: NONE |
| TIMESTAMP_LOCAL_ZONE(P) | P <= 3: INT64, P <= 6: INT64, P > 6: INT96 | P <= 3: MILLIS, P <= 6: MICROS, P > 6: NONE |
| ARRAY | 3-LEVEL LIST | LIST |
| MAP | 3-LEVEL MAP | MAP |
| MULTISET | 3-LEVEL MAP | MAP |
| ROW | GROUP |  |

Limitations:

1. [Parquet does not support nullable map keys](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps).
2. Parquet TIMESTAMP type with precision 9 will use INT96, but this int96 is a time zone converted value and requires additional adjustments.
3. Tables containing `GEOMETRY` or `GEOGRAPHY` columns must use Parquet for `file.format`, every entry in `file.format.per.level`, and `changelog-file.format` when configured.

## Avro

The following table lists the type mapping from Paimon type to Avro type.

| Paimon type | Avro type | Avro logical type |
| --- | --- | --- |
| CHAR / VARCHAR / STRING | string |  |
| `BOOLEAN` | `boolean` |  |
| `BINARY / VARBINARY` | `bytes` |  |
| `DECIMAL` | `bytes` | `decimal` |
| `TINYINT` | `int` |  |
| `SMALLINT` | `int` |  |
| `INT` | `int` |  |
| `BIGINT` | `long` |  |
| `FLOAT` | `float` |  |
| `DOUBLE` | `double` |  |
| `DATE` | `int` | `date` |
| `TIME` | `int` | `time-millis` |
| `TIMESTAMP` | P <= 3: long, P <= 6: long, P > 6: unsupported | P <= 3: timestampMillis, P <= 6: timestampMicros, P > 6: unsupported |
| `TIMESTAMP_LOCAL_ZONE` | P <= 3: long, P <= 6: long, P > 6: unsupported | P <= 3: localTimestampMillis, P <= 6: localTimestampMicros, P > 6: unsupported |
| `ARRAY` | `array` |  |
| `MAP` | string/char/varchar key: `map`<br> other key: `array` of key-value `record` | other key: `map` |
| `MULTISET` | string/char/varchar element: `map`<br> other element: `array` of element-count `record` | other element: `map` |
| `ROW` | `record` |  |

Note: 

In addition to the types listed above, for nullable types. Paimon maps nullable types to Avro `union(something, null)`,
where `something` is the Avro type converted from Paimon type.

You can refer to [Avro Specification](https://avro.apache.org/docs/1.12.0/specification/) for more information about Avro types.

## ORC

The following table lists the type mapping from Paimon type to Orc type.

| Paimon Type | Orc physical type | Orc logical type |
| --- | --- | --- |
| CHAR | bytes | CHAR |
| VARCHAR | bytes | VARCHAR |
| STRING | bytes | STRING |
| BOOLEAN | long | BOOLEAN |
| BYTES | bytes | BINARY |
| DECIMAL | decimal | DECIMAL |
| TINYINT | long | BYTE |
| SMALLINT | long | SHORT |
| INT | long | INT |
| BIGINT | long | LONG |
| FLOAT | double | FLOAT |
| DOUBLE | double | DOUBLE |
| DATE | long | DATE |
| TIMESTAMP | timestamp | TIMESTAMP |
| TIMESTAMP_LOCAL_ZONE | timestamp | TIMESTAMP_INSTANT |
| ARRAY | - | LIST |
| MAP | - | MAP |
| ROW | - | STRUCT |

Limitations:
1. ORC has a time zone bias when mapping `TIMESTAMP_LOCAL_ZONE` type, saving the millis value corresponding to the UTC
   literal time. Due to compatibility issues, this behavior cannot be modified.

## CSV

Experimental feature, not recommended for production.

Format Options:

| Option | Default | Type | Description |
| --- | --- | --- | --- |
| `csv.field-delimiter` | `,` | String | Field delimiter character (`','` by default), must be single character. You can use backslash to specify special characters, e.g. `'\t'` represents the tab character. |
| `csv.line-delimiter` | `\n` | String | The line delimiter for CSV format |
| `csv.quote-character` | `"` | String | Quote character for enclosing field values (`"` by default). |
| `csv.escape-character` | `\` | String | The escape character for CSV format. |
| `csv.include-header` | false | Boolean | Whether to include header in CSV files. |
| `csv.null-literal` | `""` | String | Null literal string that is interpreted as a null value (disabled by default). |
| `csv.mode` | `PERMISSIVE` | String | Allows a mode for dealing with corrupt records during reading. Currently supported values are `'PERMISSIVE'`, `'DROPMALFORMED'` and `'FAILFAST'`: <ul> <li>Option `'PERMISSIVE'` sets malformed fields to null.</li> <li>Option `'DROPMALFORMED'` ignores the whole corrupted records.</li> <li>Option `'FAILFAST'` throws an exception when it meets corrupted records.</li> </ul> |

Paimon CSV format uses [jackson databind API](https://github.com/FasterXML/jackson-databind) to parse and generate CSV string.

The following table lists the type mapping from Paimon type to CSV type.

| Paimon type | CSV type |
| --- | --- |
| `CHAR / VARCHAR / STRING` | `string` |
| `BOOLEAN` | `boolean` |
| `BINARY / VARBINARY` | `string with encoding: base64` |
| `DECIMAL` | `number` |
| `TINYINT` | `number` |
| `SMALLINT` | `number` |
| `INT` | `number` |
| `BIGINT` | `number` |
| `FLOAT` | `number` |
| `DOUBLE` | `number` |
| `DATE` | `string with format: date` |
| `TIME` | `string with format: time` |
| `TIMESTAMP` | `string with format: date-time` |
| `TIMESTAMP_LOCAL_ZONE` | `string with format: date-time` |

## Text

Experimental feature, not recommended for production.

Format Options:

| Option | Default | Type | Description |
| --- | --- | --- | --- |
| `text.line-delimiter` | `\n` | String | The line delimiter for TEXT format |

The Paimon text table contains only one field, and it is of string type.

## JSON

Experimental feature, not recommended for production.

Format Options:

| Option | Default | Type | Description |
| --- | --- | --- | --- |
| `json.ignore-parse-errors` | false | Boolean | Whether to ignore parse errors for JSON format. Skip fields and rows with parse errors instead of failing. Fields are set to null in case of errors. |
| `json.map-null-key-mode` | `FAIL` | String | How to handle map keys that are null. Currently supported values are `'FAIL'`, `'DROP'` and `'LITERAL'`: <ul> <li>Option `'FAIL'` will throw exception when encountering map with null key.</li> <li>Option `'DROP'` will drop null key entries for map.</li> <li>Option `'LITERAL'` will replace null key with string literal. The string literal is defined by `json.map-null-key-literal` option.</li> </ul> |
| `json.map-null-key-literal` | `null` | String | Literal to use for null map keys when `json.map-null-key-mode` is LITERAL. |
| `json.line-delimiter` | `\n` | String | The line delimiter for JSON format. |

Paimon JSON format uses [jackson databind API](https://github.com/FasterXML/jackson-databind) to parse and generate JSON string.

The following table lists the type mapping from Paimon type to JSON type.

| Paimon type | JSON type |
| --- | --- |
| `CHAR / VARCHAR / STRING` | `string` |
| `BOOLEAN` | `boolean` |
| `BINARY / VARBINARY` | `string with encoding: base64` |
| `DECIMAL` | `number` |
| `TINYINT` | `number` |
| `SMALLINT` | `number` |
| `INT` | `number` |
| `BIGINT` | `number` |
| `FLOAT` | `number` |
| `DOUBLE` | `number` |
| `DATE` | `string with format: date` |
| `TIME` | `string with format: time` |
| `TIMESTAMP` | `string with format: date-time` |
| `TIMESTAMP_LOCAL_ZONE` | `string with format: date-time (with UTC time zone)` |
| `ARRAY` | `array` |
| `MAP` | `object` |
| `MULTISET` | `object` |
| `ROW` | `object` |

## Lance

Lance is a modern columnar data format optimized for machine learning and vector search workloads. It provides high-performance read and write operations with native support for Apache Arrow.

The following table lists the type mapping from Paimon type to Lance (Arrow) type.

| Paimon Type | Lance (Arrow) type |
| --- | --- |
| CHAR / VARCHAR / STRING | UTF8 |
| BOOLEAN | BOOL |
| BINARY / VARBINARY | BINARY |
| DECIMAL(P, S) | DECIMAL128(P, S) |
| TINYINT | INT8 |
| SMALLINT | INT16 |
| INT | INT32 |
| BIGINT | INT64 |
| FLOAT | FLOAT |
| DOUBLE | DOUBLE |
| DATE | DATE32 |
| TIME | TIME32 / TIME64 |
| TIMESTAMP(P) | TIMESTAMP (unit based on precision) |
| ARRAY | LIST |
| MULTISET | LIST |
| ROW | STRUCT |

Limitations:
1. Lance file format does not support `MAP` type.
2. Lance file format does not support `TIMESTAMP_LOCAL_ZONE` type.

## Vortex

[Vortex](https://github.com/spiraldb/vortex) is a columnar file format that uses adaptive, data-dependent encodings to achieve high compression ratios while maintaining fast scan performance. It supports native predicate pushdown and efficient column projection.

Key features:
- **Adaptive Encoding**: Automatically selects the best encoding per column based on data distribution
- **Native Predicate Pushdown**: Supports filter expressions pushed down to the scan layer
- **Column Projection**: Only reads requested columns from disk

Limitations:
1. Vortex does not support `MAP` or `MULTISET` types.

## Mosaic

[Mosaic](https://paimon.apache.org/docs/mosaic/) is a columnar-bucket hybrid format optimized for wide tables. It groups columns into buckets and compresses each bucket independently with ZSTD, enabling efficient column projection that only reads the buckets containing requested columns.

Key features:
- **Column Bucketing**: Columns are grouped into configurable buckets for parallel I/O, significantly reducing read amplification on wide tables
- **Row Group Statistics**: Per-row-group min/max/null_count statistics enable row group skipping during scan
- **ZSTD Compression**: All data is compressed with ZSTD (configurable level)
- **Arrow-native**: Uses Apache Arrow as the in-memory representation for zero-copy integration

Format Options:

| Option | Default | Type | Description |
| --- | --- | --- | --- |
| `mosaic.num-buckets` | auto | Integer | Number of column buckets for parallel I/O. When set to 0 or not specified, the format auto-determines the bucket count. |
| `mosaic.stats-columns` | (empty) | String | Comma-separated column names to collect min/max statistics for filter pushdown. Empty means no statistics are collected. |

Limitations:
1. Mosaic does not support complex types: ARRAY, MAP, MULTISET, ROW, VARIANT, BLOB, VECTOR.

For more details, see the [Mosaic documentation](https://paimon.apache.org/docs/mosaic/).

## Row

The Row format stores complete rows in independently compressed ZSTD blocks. Each decompressed
block contains a row-offset array for direct positioning. The compression level defaults to `1`
and is configured with `file.compression.zstd-level`.

- **Row positioning:** locating a row within a decompressed block is O(1); selecting and loading
  its block adds index, I/O, and decompression work.
- **Compact encoding:** a null bitmap precedes sequentially encoded field values.
- **Row selection:** the reader selects blocks and rows from requested row positions, avoiding
  decompression of unselected blocks. Vectored I/O can still read intervening bytes.

For field encodings, projection behavior, and configuration, see [Row Format](./rowformat).

## BLOB

The BLOB format is a specialized format for storing large binary objects such as images, videos, and other multimodal data. Unlike other formats that store data inline, BLOB format stores large binary data in separate files with an optimized layout for random access.

BLOB files use the `.blob` extension and have the following structure:

```
+------------------+
| Blob Entry 1     |
|   Magic Number   |  4 bytes (1481511375, Little Endian)
|   Blob Data      |  Variable length
|   Length         |  8 bytes (Little Endian)
|   CRC32          |  4 bytes (Little Endian)
+------------------+
| Blob Entry 2     |
|   ...            |
+------------------+
| Index            |  Variable (Delta-Varint compressed)
+------------------+
| Index Length     |  4 bytes (Little Endian)
| Version          |  1 byte
+------------------+
```

Each physical BLOB file stores one logical field. The field can be `BLOB`,
`ARRAY<BLOB>`, or `MAP<K, BLOB>`. For `ARRAY<BLOB>`, the variable-length data area in
an entry uses the following nested payload:

```
+----------------------+-----------------------------------------------+
| Array Magic Number   | 4 bytes (1094861634, Little Endian)           |
| Array Version        | 1 byte                                        |
| Element Count        | 4 bytes (Little Endian)                       |
| Element Data         | Concatenated bytes of all non-null elements   |
| Element Length Index | Delta-Varint compressed element lengths       |
| Index Length         | 4 bytes (Little Endian)                       |
+----------------------+-----------------------------------------------+
```

An element length of `-1` represents a null array element. An empty array is encoded
with an element count of zero and an empty element index; it is distinct from a null
array.

For `MAP<K, BLOB>`, the variable-length data area uses the following nested payload:

```
+----------------------+-----------------------------------------------+
| Map Magic Number     | 4 bytes (1296188226, Little Endian)           |
| Map Version          | 1 byte                                        |
| Entry Count          | 4 bytes (Little Endian)                       |
| Key Data             | Concatenated bytes of all non-null keys       |
| Blob Data            | Concatenated bytes of all non-null values     |
| Key Length Index     | Delta-Varint compressed key lengths           |
| Blob Length Index    | Delta-Varint compressed Blob lengths          |
| Key Index Length     | 4 bytes (Little Endian)                       |
| Blob Index Length    | 4 bytes (Little Endian)                       |
+----------------------+-----------------------------------------------+
```

The key and Blob length indexes are aligned by entry position. A length of `-1`
represents null, while zero represents an empty key or Blob. Supported key types and
their encodings are:

| Key type | Encoding |
|----------|----------|
| `TINYINT`, `SMALLINT`, `INT`, `BIGINT` | Signed integer in little-endian byte order using the type's fixed width |
| `BOOLEAN` | One byte: `0` for false and `1` for true |
| `DECIMAL(p, s)`, `p <= 18` | Eight-byte little-endian signed unscaled integer |
| `DECIMAL(p, s)`, `p > 18` | Minimal-length signed big-endian two's-complement unscaled integer |
| `DATE` | Four-byte little-endian signed count of days since 1970-01-01 |
| `TIME(p)` | Four-byte little-endian signed count of milliseconds since midnight |
| `BINARY`, `VARBINARY` (`BYTES`) | Raw bytes |
| `CHAR`, `VARCHAR` | UTF-8 bytes |

The DECIMAL scale is defined by the field type and is not stored in each key. `BINARY`
and `VARBINARY` keys are not padded, truncated, or validated against the declared length.
An empty map has an entry count of zero and is distinct from a null map. The `TIME(p)`
encoding uses Paimon's millisecond internal representation and does not add nanosecond
precision.

At the outer file index level, `-1` represents a null field and `-2` represents a
field placeholder used by data evolution.

Key features:
- **CRC32 Checksums**: Each blob entry has a CRC32 checksum for data integrity verification
- **Indexed Access**: The index at the end enables efficient random access to any blob in the file
- **Delta-Varint Compression**: The index uses delta-varint compression for space efficiency

Limitations:
1. BLOB format only supports a single `BLOB`, `ARRAY<BLOB>`, or `MAP<K, BLOB>` field per physical file.
2. BLOB format does not support predicate pushdown.
3. Statistics collection is not supported for BLOB columns.

### Video

Video is an independent, versioned format with the `.video` extension. It packs one or more
complete encoded-video payloads and logical frame runs. The payloads are raw byte ranges without
the ordinary BLOB entry header, length trailer, or per-entry CRC:

```
+----------------------------+
| Encoded Video Payload 1    |  Raw complete video bytes
+----------------------------+
| Encoded Video Payload 2    |
+----------------------------+
| ...                        |
+----------------------------+
| Physical Length Index      |  Delta-Varint video lengths
+----------------------------+
| Run Length Index           |  Delta-Varint logical row counts
+----------------------------+
| Run Reference Index        |  Delta-Varint physical video ordinals
+----------------------------+
| Run First-Frame Index      |  Delta-Varint frame ordinals
+----------------------------+
| Physical Index Length      |  4 bytes (Little Endian)
| Run-Length Index Length    |  4 bytes (Little Endian)
| Run-Reference Index Length |  4 bytes (Little Endian)
| First-Frame Index Length   |  4 bytes (Little Endian)
| Magic Number               |  4 bytes (0x4F454449, Little Endian)
| Version                    |  1 byte
+----------------------------+
```

The run arrays have equal element counts. A non-negative run reference is an ordinal in the
physical length index. For logical row `r` in a run beginning at logical row `s`, the returned
`VideoFrameDescriptor` identifies the referenced raw video range and frame ordinal
`run_first_frame + (r - s)`. `-1` is a NULL run and `-2` is a data-evolution placeholder run.
Non-negative runs have fixed frame stride one in version 1; a discontinuity starts another run.

The serialized `VideoFrameDescriptor` stored in an Arrow/data-file cell has its own versioned
wire layout. All numeric values are little endian:

| Field | Size | Description |
| --- | ---: | --- |
| Version | 1 byte | Descriptor version, currently `1` |
| Magic | 8 bytes | `0x564944454F46524D` (`VIDEOFRM`) |
| URI length | 4 bytes | UTF-8 URI byte length |
| URI | variable | URI of the containing `.video` file |
| Offset | 8 bytes | Start of the complete encoded-video payload |
| Length | 8 bytes | Encoded-video payload length |
| Frame index | 8 bytes | Zero-based presentation-order frame ordinal |

Descriptor bytes are independently versioned from the `.video` container. Java and Python share
canonical descriptor and container fixtures to keep both implementations byte-compatible.

Readers validate footer and index bounds, positive physical lengths, full coverage of the payload
region, equal run-index counts, positive run lengths, physical ordinals, and non-negative first
frames. The format currently supports one scalar BLOB field per file. Physical video reuse uses
exact input payload `BlobDescriptor` identity and is file-local; there are no cross-file payload
references. Ordinary `.blob` files keep their existing version, wrappers, checksums, and layout.

For usage details, configuration options, and examples, see [Blob Type](../../multimodal-table/blob).
