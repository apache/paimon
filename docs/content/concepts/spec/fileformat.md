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

Currently, supports Parquet, Avro, ORC, JSON, CSV file formats.
- Recommended column format is Parquet, which has a high compression rate and fast column projection queries.
- Recommended row based format is Avro, which has good performance n reading and writing full row (all columns).
- Recommended testing format is JSON, which has better readability but the worst storage and read-write performance.

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

## ORC

TODO

Limitations:
1. ORC has a time zone bias when mapping `TIMESTAMP_LOCAL_ZONE` type, saving the millis value corresponding to the UTC
   literal time. Due to compatibility issues, this behavior cannot be modified.

## AVRO

TODO

## JSON

Experimental feature, not recommended for production.

The JSON format supports several configuration options to customize JSON parsing and serialization behavior:

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Option</th>
        <th class="text-center">Type</th>
        <th class="text-center">Default</th>
        <th class="text-left">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>json.ignore-parse-errors</td>
      <td>Boolean</td>
      <td>false</td>
      <td>Whether to ignore parse errors for JSON format. When enabled, malformed JSON lines will be skipped and returned as null instead of throwing exceptions.</td>
    </tr>
    <tr>
      <td>json.map-null-key-mode</td>
      <td>Enum</td>
      <td>FAIL</td>
      <td>How to handle map keys that are null. Available values: FAIL (throw exception), DROP (drop entries with null keys), LITERAL (replace null keys with literal string).</td>
    </tr>
    <tr>
      <td>json.map-null-key-literal</td>
      <td>String</td>
      <td>"null"</td>
      <td>Literal to use for null map keys when map-null-key-mode is set to LITERAL.</td>
    </tr>
    <tr>
      <td>json.line-delimiter</td>
      <td>String</td>
      <td>"\n"</td>
      <td>The line delimiter for JSON format. Each JSON record should be separated by this delimiter.</td>
    </tr>
    </tbody>
</table>

## CSV

Experimental feature, not recommended for production.

TODO