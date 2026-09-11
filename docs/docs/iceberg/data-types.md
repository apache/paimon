---
title: "Data Types"
sidebar_position: 9
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

# Data Types


The following mappings describe the metadata Paimon publishes. The Iceberg reader must also
support the table's file format, format version, and column types.

| Paimon Data Type | Iceberg Data Type |
|----------------|-------------------|
| `BOOLEAN`      | `boolean`         |
| `INT`          | `int`             |
| `BIGINT`       | `long`            |
| `FLOAT`        | `float`           |
| `DOUBLE`       | `double`          |
| `DECIMAL`      | `decimal`         |
| `CHAR`         | `string`          |
| `VARCHAR`      | `string`          |
| `BINARY`       | `binary`          |
| `VARBINARY`    | `binary`          |
| `DATE`         | `date`            |
| `TIME` (precision 0-3) | `time`            |
| `TIME` (other precisions) | not supported  |
| `TIMESTAMP` (precision 3-6)   | `timestamp`       |
| `TIMESTAMP_LTZ` (precision 3-6) | `timestamptz`     |
| `TIMESTAMP` (other precisions)  | not supported     |
| `TIMESTAMP_LTZ` (other precisions) | not supported  |
| `GEOMETRY(crs)` | `geometry(crs)` |
| `GEOGRAPHY(crs, algorithm)` | `geography(crs, algorithm)` |
| `ARRAY`        | `list`            |
| `MAP`          | `map`             |
| `ROW`          | `struct`          |

:::info

**Note on Timestamp Types:**
- `TIMESTAMP` and `TIMESTAMP_LTZ` types with precision from 3 to 6 are mapped to standard Iceberg timestamp types
- Any other precision is rejected while Iceberg metadata is enabled. A precision above 6 is written
  as Parquet INT96, which Iceberg reads as a microsecond zoned timestamp rather than the
  nanoseconds the column declares. Use a precision from 3 to 6.

**Note on Time Types:**
`TIME` types with a precision above 3 are rejected while Iceberg metadata is enabled: Iceberg
compatibility publishes only millisecond time values.

**Note on Geospatial Types:**
- `GEOMETRY` and `GEOGRAPHY` values use OGC Well-Known Binary (WKB). The default CRS is `OGC:CRS84`, and the default geography edge algorithm is `spherical`.
- Geospatial columns require Parquet for data, per-level, and changelog files. When Iceberg metadata is enabled, set `metadata.iceberg.format-version` to `3`.
- Spark SQL supports geospatial columns in Spark 4.1 when `spark.sql.geospatial.enabled=true`, for CRSs recognized by Spark, with the `spherical` geography edge algorithm. Spark 3.x, Spark 4.0, and Flink SQL reject these columns instead of exposing them as binary and losing the CRS or edge algorithm.
- When Iceberg metadata is enabled, a `GEOGRAPHY` CRS cannot contain a comma, including in nested columns, because Iceberg's geospatial type grammar uses commas to separate parameters.
- Iceberg REST catalog publication does not yet support geospatial columns. Use `table-location`, `hadoop-catalog`, or `hive-catalog` metadata storage instead.

:::


## Existing Tables

Enabling Iceberg metadata validates historical schemas as well as the current schema. Changing or
dropping an incompatible column in the latest schema alone may therefore be insufficient. Check
schema history when enabling publication fails with a timestamp, time, or geospatial type error.

For connector-specific restrictions, see [query engines](./ecosystem.mdx).
