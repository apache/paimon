---
title: "Data Types"
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

Use this table to check the mapping between Spark and Paimon types. Spark types are in
`org.apache.spark.sql.types`; Paimon types are in `org.apache.paimon.types`.
Version-specific rows apply only to the indicated Spark versions. The binary row lists Paimon
types Spark can read as `BinaryType`; it does not imply a unique mapping in both directions.

## Type Mapping

| Spark type | Paimon type | Atomic |
| --- | --- | --- |
| `StructType` | `RowType` | false |
| `MapType` | `MapType` | false |
| `ArrayType` | `ArrayType` | false |
| `BooleanType` | `BooleanType` | true |
| `ByteType` | `TinyIntType` | true |
| `ShortType` | `SmallIntType` | true |
| `IntegerType` | `IntType` | true |
| `LongType` | `BigIntType` | true |
| `FloatType` | `FloatType` | true |
| `DoubleType` | `DoubleType` | true |
| `StringType` | `VarCharType(Integer.MAX_VALUE)` | true |
| `VarCharType(length)` | `VarCharType(length)` | true |
| `CharType(length)` | `CharType(length)` | true |
| `DateType` | `DateType` | true |
| `TimestampType` | `LocalZonedTimestampType` | true |
| `TimestampNTZType (Spark 3.4+)` | `TimestampType` | true |
| `DecimalType(precision, scale)` | `DecimalType(precision, scale)` | true |
| `BinaryType` | `VarBinaryType`, `BinaryType` | true |
| `GeometryType (Spark 4.1)` | `GeometryType` | true |
| `GeographyType (Spark 4.1)` | `GeographyType` | true |
| `VariantType (Spark 4.0+)` | `VariantType` | true |

## Timestamps

On Spark 3.3 and earlier, Paimon maps both `TimestampType` and `LocalZonedTimestampType`
to Spark `TimestampType`. Only Paimon `TimestampType` is handled correctly by this legacy mapping.

Reading `LocalZonedTimestampType` values written by another engine, such as Flink, can therefore
produce a time zone offset that needs to be adjusted manually.

Spark 3.4 and later distinguish the two timestamp types.

## Geospatial Types

Native `GeometryType` and `GeographyType` conversion is supported only in Spark 4.1 and only for CRSs recognized by Spark. Enable it explicitly in production with `--conf spark.sql.geospatial.enabled=true`; Spark enables it automatically only in its test environment. Spark 4.1 supports only the `spherical` geography edge algorithm, so Paimon geography types using `vincenty`, `thomas`, `andoyer`, or `karney` cannot be converted. Spark 3.x and Spark 4.0 reject Paimon geospatial columns instead of exposing them as `BinaryType`, which would lose the CRS or edge algorithm. Paimon does not support Spark geospatial types with mixed SRIDs.
