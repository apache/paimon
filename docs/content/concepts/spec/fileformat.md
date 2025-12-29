---
title: "FileFormat"
weight: 7
type: docs
aliases:
- /concepts/spec/fileformat.html
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

Currently, supports Parquet, Avro, ORC, CSV, JSON, and Lance file formats.
- Recommended column format is Parquet, which has a high compression rate and fast column projection queries.
- Recommended row based format is Avro, which has good performance n reading and writing full row (all columns).
- Recommended testing format is CSV, which has better readability but the worst read-write performance.
- Recommended format for ML workloads is Lance, which is optimized for vector search and machine learning use cases.

## PARQUET

Parquet is the default file format for Paimon.

The following table lists the type mapping from Paimon type to Parquet type.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Paimon Type</th>
        <th class="text-center">Parquet type</th>
        <th class="text-center">Parquet logical type</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>CHAR / VARCHAR / STRING</td>
      <td>BINARY</td>
      <td>UTF8</td>
    </tr>
    <tr>
      <td>BOOLEAN</td>
      <td>BOOLEAN</td>
      <td></td>
    </tr>
    <tr>
      <td>BINARY / VARBINARY</td>
      <td>BINARY</td>
      <td></td>
    </tr>
    <tr>
      <td>DECIMAL(P, S)</td>
      <td>P <= 9: INT32, P <= 18: INT64, P > 18: FIXED_LEN_BYTE_ARRAY</td>
      <td>DECIMAL(P, S)</td>
    </tr>
    <tr>
      <td>TINYINT</td>
      <td>INT32</td>
      <td>INT_8</td>
    </tr>
    <tr>
      <td>SMALLINT</td>
      <td>INT32</td>
      <td>INT_16</td>
    </tr>
    <tr>
      <td>INT</td>
      <td>INT32</td>
      <td></td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>INT64</td>
      <td></td>
    </tr>
    <tr>
      <td>FLOAT</td>
      <td>FLOAT</td>
      <td></td>
    </tr>
    <tr>
      <td>DOUBLE</td>
      <td>DOUBLE</td>
      <td></td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>INT32</td>
      <td>DATE</td>
    </tr>
    <tr>
      <td>TIME</td>
      <td>INT32</td>
      <td>TIME_MILLIS</td>
    </tr>
    <tr>
      <td>TIMESTAMP(P)</td>
      <td>P <= 3: INT64, P <= 6: INT64, P > 6: INT96</td>
      <td>P <= 3: MILLIS, P <= 6: MICROS, P > 6: NONE</td>
    </tr>
    <tr>
      <td>TIMESTAMP_LOCAL_ZONE(P)</td>
      <td>P <= 3: INT64, P <= 6: INT64, P > 6: INT96</td>
      <td>P <= 3: MILLIS, P <= 6: MICROS, P > 6: NONE</td>
    </tr>
    <tr>
      <td>ARRAY</td>
      <td>3-LEVEL LIST</td>
      <td>LIST</td>
    </tr>
    <tr>
      <td>MAP</td>
      <td>3-LEVEL MAP</td>
      <td>MAP</td>
    </tr>
    <tr>
      <td>MULTISET</td>
      <td>3-LEVEL MAP</td>
      <td>MAP</td>
    </tr>
    <tr>
      <td>ROW</td>
      <td>GROUP</td>
      <td></td>
    </tr>
    </tbody>
</table>

Limitations:
1. [Parquet does not support nullable map keys](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps).
2. Parquet TIMESTAMP type with precision 9 will use INT96, but this int96 is a time zone converted value and requires additional adjustments.

## AVRO

The following table lists the type mapping from Paimon type to Avro type.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Paimon type</th>
        <th class="text-left">Avro type</th>
        <th class="text-left">Avro logical type</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>CHAR / VARCHAR / STRING</td>
      <td>string</td>
      <td></td>
    </tr>
    <tr>
      <td><code>BOOLEAN</code></td>
      <td><code>boolean</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>BINARY / VARBINARY</code></td>
      <td><code>bytes</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>DECIMAL</code></td>
      <td><code>bytes</code></td>
      <td><code>decimal</code></td>
    </tr>
    <tr>
      <td><code>TINYINT</code></td>
      <td><code>int</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>SMALLINT</code></td>
      <td><code>int</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>INT</code></td>
      <td><code>int</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>BIGINT</code></td>
      <td><code>long</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>FLOAT</code></td>
      <td><code>float</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>DOUBLE</code></td>
      <td><code>double</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>DATE</code></td>
      <td><code>int</code></td>
      <td><code>date</code></td>
    </tr>
    <tr>
      <td><code>TIME</code></td>
      <td><code>int</code></td>
      <td><code>time-millis</code></td>
    </tr>
    <tr>
      <td><code>TIMESTAMP</code></td>
      <td>P <= 3: long, P <= 6: long, P > 6: unsupported</td>
      <td>P <= 3: timestampMillis, P <= 6: timestampMicros, P > 6: unsupported</td>
    </tr>
    <tr>
      <td><code>TIMESTAMP_LOCAL_ZONE</code></td>
      <td>P <= 3: long, P <= 6: long, P > 6: unsupported</td>
      <td>P <= 3: timestampMillis, P <= 6: timestampMicros, P > 6: unsupported</td>
    </tr>
    <tr>
      <td><code>ARRAY</code></td>
      <td><code>array</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>MAP</code><br>
      (key must be string/char/varchar type)</td>
      <td><code>map</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>MULTISET</code><br>
      (element must be string/char/varchar type)</td>
      <td><code>map</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>ROW</code></td>
      <td><code>record</code></td>
      <td></td>
    </tr>
    </tbody>
</table>

In addition to the types listed above, for nullable types. Paimon maps nullable types to Avro `union(something, null)`,
where `something` is the Avro type converted from Paimon type.

You can refer to [Avro Specification](https://avro.apache.org/docs/1.12.0/specification/) for more information about Avro types.

## ORC

The following table lists the type mapping from Paimon type to Orc type.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Paimon Type</th>
        <th class="text-center">Orc physical type</th>
        <th class="text-center">Orc logical type</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>CHAR</td>
      <td>bytes</td>
      <td>CHAR</td>
    </tr>
    <tr>
      <td>VARCHAR</td>
      <td>bytes</td>
      <td>VARCHAR</td>
    </tr>
    <tr>
      <td>STRING</td>
      <td>bytes</td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>BOOLEAN</td>
      <td>long</td>
      <td>BOOLEAN</td>
    </tr>
    <tr>
      <td>BYTES</td>
      <td>bytes</td>
      <td>BINARY</td>
    </tr>
    <tr>
      <td>DECIMAL</td>
      <td>decimal</td>
      <td>DECIMAL</td>
    </tr>
    <tr>
      <td>TINYINT</td>
      <td>long</td>
      <td>BYTE</td>
    </tr>
    <tr>
      <td>SMALLINT</td>
      <td>long</td>
      <td>SHORT</td>
    </tr>
    <tr>
      <td>INT</td>
      <td>long</td>
      <td>INT</td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>long</td>
      <td>LONG</td>
    </tr>
    <tr>
      <td>FLOAT</td>
      <td>double</td>
      <td>FLOAT</td>
    </tr>
    <tr>
      <td>DOUBLE</td>
      <td>double</td>
      <td>DOUBLE</td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>long</td>
      <td>DATE</td>
    </tr>
    <tr>
      <td>TIMESTAMP</td>
      <td>timestamp</td>
      <td>TIMESTAMP</td>
    </tr>
    <tr>
      <td>TIMESTAMP_LOCAL_ZONE</td>
      <td>timestamp</td>
      <td>TIMESTAMP_INSTANT</td>
    </tr>
    <tr>
      <td>ARRAY</td>
      <td>-</td>
      <td>LIST</td>
    </tr>
    <tr>
      <td>MAP</td>
      <td>-</td>
      <td>MAP</td>
    </tr>
    <tr>
      <td>ROW</td>
      <td>-</td>
      <td>STRUCT</td>
    </tr>
    </tbody>
</table>

Limitations:
1. ORC has a time zone bias when mapping `TIMESTAMP_LOCAL_ZONE` type, saving the millis value corresponding to the UTC
   literal time. Due to compatibility issues, this behavior cannot be modified.

## CSV

Experimental feature, not recommended for production.

Format Options:

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-center" style="width: 7%">Default</th>
        <th class="text-center" style="width: 10%">Type</th>
        <th class="text-center" style="width: 42%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>csv.field-delimiter</h5></td>
      <td style="word-wrap: break-word;"><code>,</code></td>
      <td>String</td>
      <td>Field delimiter character (<code>','</code> by default), must be single character. You can use backslash to specify special characters, e.g. <code>'\t'</code> represents the tab character.
      </td>
    </tr>
    <tr>
      <td><h5>csv.line-delimiter</h5></td>
      <td style="word-wrap: break-word;"><code>\n</code></td>
      <td>String</td>
      <td>The line delimiter for CSV format</td>
    </tr>
    <tr>
      <td><h5>csv.quote-character</h5></td>
      <td style="word-wrap: break-word;"><code>"</code></td>
      <td>String</td>
      <td>Quote character for enclosing field values (<code>"</code> by default).</td>
    </tr>
    <tr>
      <td><h5>csv.escape-character</h5></td>
      <td style="word-wrap: break-word;">\</td>
      <td>String</td>
      <td>The escape character for CSV format.</td>
    </tr>
   <tr>
      <td><h5>csv.include-header</h5></td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Whether to include header in CSV files.</td>
    </tr>
    <tr>
      <td><h5>csv.null-literal</h5></td>
      <td style="word-wrap: break-word;"><code>""</code></td>
      <td>String</td>
      <td>Null literal string that is interpreted as a null value (disabled by default).</td>
    </tr>
    <tr>
      <td><h5>csv.mode</h5></td>
      <td style="word-wrap: break-word;"><code>PERMISSIVE</code></td>
      <td>String</td>
      <td>Allows a mode for dealing with corrupt records during reading. Currently supported values are <code>'PERMISSIVE'</code>, <code>'DROPMALFORMED'</code> and <code>'FAILFAST'</code>:
      <ul>
      <li>Option <code>'PERMISSIVE'</code> sets malformed fields to null.</li>
      <li>Option <code>'DROPMALFORMED'</code> ignores the whole corrupted records.</li>
      <li>Option <code>'FAILFAST'</code> throws an exception when it meets corrupted records.</li>
      </ul>
      </td>
    </tr>
    </tbody>
</table>

Paimon CSV format uses [jackson databind API](https://github.com/FasterXML/jackson-databind) to parse and generate CSV string.

The following table lists the type mapping from Paimon type to CSV type.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Paimon type</th>
        <th class="text-left">CSV type</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>CHAR / VARCHAR / STRING</code></td>
      <td><code>string</code></td>
    </tr>
    <tr>
      <td><code>BOOLEAN</code></td>
      <td><code>boolean</code></td>
    </tr>
    <tr>
      <td><code>BINARY / VARBINARY</code></td>
      <td><code>string with encoding: base64</code></td>
    </tr>
    <tr>
      <td><code>DECIMAL</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>TINYINT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>SMALLINT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>INT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>BIGINT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>FLOAT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>DOUBLE</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>DATE</code></td>
      <td><code>string with format: date</code></td>
    </tr>
    <tr>
      <td><code>TIME</code></td>
      <td><code>string with format: time</code></td>
    </tr>
    <tr>
      <td><code>TIMESTAMP</code></td>
      <td><code>string with format: date-time</code></td>
    </tr>
    <tr>
      <td><code>TIMESTAMP_LOCAL_ZONE</code></td>
      <td><code>string with format: date-time</code></td>
    </tr>
    </tbody>
</table>

## TEXT

Experimental feature, not recommended for production.

Format Options:

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-center" style="width: 7%">Default</th>
        <th class="text-center" style="width: 10%">Type</th>
        <th class="text-center" style="width: 42%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>text.line-delimiter</h5></td>
      <td style="word-wrap: break-word;"><code>\n</code></td>
      <td>String</td>
      <td>The line delimiter for TEXT format</td>
    </tr>
    </tbody>
</table>

The Paimon text table contains only one field, and it is of string type.

## JSON

Experimental feature, not recommended for production.

Format Options:

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-center" style="width: 7%">Default</th>
        <th class="text-center" style="width: 10%">Type</th>
        <th class="text-center" style="width: 42%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>json.ignore-parse-errors</h5></td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Whether to ignore parse errors for JSON format. Skip fields and rows with parse errors instead of failing. Fields are set to null in case of errors.</td>
    </tr>
    <tr>
      <td><h5>json.map-null-key-mode</h5></td>
      <td style="word-wrap: break-word;"><code>FAIL</code></td>
      <td>String</td>
      <td>How to handle map keys that are null. Currently supported values are <code>'FAIL'</code>, <code>'DROP'</code> and <code>'LITERAL'</code>:
      <ul>
      <li>Option <code>'FAIL'</code> will throw exception when encountering map with null key.</li>
      <li>Option <code>'DROP'</code> will drop null key entries for map.</li>
      <li>Option <code>'LITERAL'</code> will replace null key with string literal. The string literal is defined by <code>json.map-null-key-literal</code> option.</li>
      </ul>
      </td>
    </tr>
    <tr>
      <td><h5>json.map-null-key-literal</h5></td>
      <td style="word-wrap: break-word;"><code>null</code></td>
      <td>String</td>
      <td>Literal to use for null map keys when <code>json.map-null-key-mode</code> is LITERAL.</td>
    </tr>
    <tr>
      <td><h5>json.line-delimiter</h5></td>
      <td style="word-wrap: break-word;"><code>\n</code></td>
      <td>String</td>
      <td>The line delimiter for JSON format.</td>
    </tr>
    </tbody>
</table>

Paimon JSON format uses [jackson databind API](https://github.com/FasterXML/jackson-databind) to parse and generate JSON string.

The following table lists the type mapping from Paimon type to JSON type.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Paimon type</th>
        <th class="text-left">JSON type</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>CHAR / VARCHAR / STRING</code></td>
      <td><code>string</code></td>
    </tr>
    <tr>
      <td><code>BOOLEAN</code></td>
      <td><code>boolean</code></td>
    </tr>
    <tr>
      <td><code>BINARY / VARBINARY</code></td>
      <td><code>string with encoding: base64</code></td>
    </tr>
    <tr>
      <td><code>DECIMAL</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>TINYINT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>SMALLINT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>INT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>BIGINT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>FLOAT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>DOUBLE</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>DATE</code></td>
      <td><code>string with format: date</code></td>
    </tr>
    <tr>
      <td><code>TIME</code></td>
      <td><code>string with format: time</code></td>
    </tr>
    <tr>
      <td><code>TIMESTAMP</code></td>
      <td><code>string with format: date-time</code></td>
    </tr>
    <tr>
      <td><code>TIMESTAMP_LOCAL_ZONE</code></td>
      <td><code>string with format: date-time (with UTC time zone)</code></td>
    </tr>
    <tr>
      <td><code>ARRAY</code></td>
      <td><code>array</code></td>
    </tr>
    <tr>
      <td><code>MAP</code></td>
      <td><code>object</code></td>
    </tr>
    <tr>
      <td><code>MULTISET</code></td>
      <td><code>object</code></td>
    </tr>
    <tr>
      <td><code>ROW</code></td>
      <td><code>object</code></td>
    </tr>
    </tbody>
</table>

## LANCE

Lance is a modern columnar data format optimized for machine learning and vector search workloads. It provides high-performance read and write operations with native support for Apache Arrow.

The following table lists the type mapping from Paimon type to Lance (Arrow) type.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Paimon Type</th>
        <th class="text-center">Lance (Arrow) type</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>CHAR / VARCHAR / STRING</td>
      <td>UTF8</td>
    </tr>
    <tr>
      <td>BOOLEAN</td>
      <td>BOOL</td>
    </tr>
    <tr>
      <td>BINARY / VARBINARY</td>
      <td>BINARY</td>
    </tr>
    <tr>
      <td>DECIMAL(P, S)</td>
      <td>DECIMAL128(P, S)</td>
    </tr>
    <tr>
      <td>TINYINT</td>
      <td>INT8</td>
    </tr>
    <tr>
      <td>SMALLINT</td>
      <td>INT16</td>
    </tr>
    <tr>
      <td>INT</td>
      <td>INT32</td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>INT64</td>
    </tr>
    <tr>
      <td>FLOAT</td>
      <td>FLOAT</td>
    </tr>
    <tr>
      <td>DOUBLE</td>
      <td>DOUBLE</td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DATE32</td>
    </tr>
    <tr>
      <td>TIME</td>
      <td>TIME32 / TIME64</td>
    </tr>
    <tr>
      <td>TIMESTAMP(P)</td>
      <td>TIMESTAMP (unit based on precision)</td>
    </tr>
    <tr>
      <td>ARRAY</td>
      <td>LIST</td>
    </tr>
    <tr>
      <td>MULTISET</td>
      <td>LIST</td>
    </tr>
    <tr>
      <td>ROW</td>
      <td>STRUCT</td>
    </tr>
    </tbody>
</table>

Limitations:
1. Lance file format does not support `MAP` type.
2. Lance file format does not support `TIMESTAMP_LOCAL_ZONE` type.

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

Key features:
- **CRC32 Checksums**: Each blob entry has a CRC32 checksum for data integrity verification
- **Indexed Access**: The index at the end enables efficient random access to any blob in the file
- **Delta-Varint Compression**: The index uses delta-varint compression for space efficiency

Limitations:
1. BLOB format only supports a single BLOB type column per file.
2. BLOB format does not support predicate pushdown.
3. Statistics collection is not supported for BLOB columns.

For usage details, configuration options, and examples, see [Blob Type]({{< ref "concepts/spec/blob" >}}).
