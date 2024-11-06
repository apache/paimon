---
title: "Doris"
weight: 3
type: docs
aliases:
- /engines/doris.html
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

# Doris

This documentation is a guide for using Paimon in Doris.

> More details can be found in [Apache Doris Website](https://doris.apache.org/docs/lakehouse/datalake-analytics/paimon/)

## Version

Paimon currently supports Apache Doris 2.0.6 and above.

## Create Paimon Catalog

Use `CREATE CATALOG` statement in Apache Doris to create Paimon Catalog.

Doris support multi types of Paimon Catalogs. Here are some examples:

```sql
-- HDFS based Paimon Catalog
CREATE CATALOG `paimon_hdfs` PROPERTIES (
    "type" = "paimon",
    "warehouse" = "hdfs://172.21.0.1:8020/user/paimon",
    "hadoop.username" = "hadoop"
);

-- Aliyun OSS based Paimon Catalog
CREATE CATALOG `paimon_oss` PROPERTIES (
    "type" = "paimon",
    "warehouse" = "oss://paimon-bucket/paimonoss",
    "oss.endpoint" = "oss-cn-beijing.aliyuncs.com",
    "oss.access_key" = "ak",
    "oss.secret_key" = "sk"
);

-- Hive Metastore based Paimon Catalog
CREATE CATALOG `paimon_hms` PROPERTIES (
    "type" = "paimon",
    "paimon.catalog.type" = "hms",
    "warehouse" = "hdfs://172.21.0.1:8020/user/zhangdong/paimon2",
    "hive.metastore.uris" = "thrift://172.21.0.44:7004",
    "hadoop.username" = "hadoop"
);
```

See [Apache Doris Website](https://doris.apache.org/docs/lakehouse/datalake-analytics/paimon/) for more examples.

## Access Paimon Catalog

1. Query Paimon table with full qualified name

    ```sql
    SELECT * FROM paimon_hdfs.paimon_db.paimon_table;
    ```

2. Switch to Paimon Catalog and query

    ```sql
    SWITCH paimon_hdfs;
    USE paimon_db;
    SELECT * FROM paimon_table;
    ```

## Query Optimization

- Read optimized for Primary Key Table

    Doris can utilize the [Read optimized](https://paimon.apache.org/releases/release-0.6/#read-optimized) feature for Primary Key Table(release in Paimon 0.6), by reading base data files using native Parquet/ORC reader and delta file using JNI.

- Deletion Vectors

    Doris(2.1.4+) natively supports [Deletion Vectors](https://paimon.apache.org/releases/release-0.8/#deletion-vectors)(released in Paimon 0.8).

## Doris to Paimon type mapping

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 10%">Doris Data Type</th>
      <th class="text-left" style="width: 10%">Paimon Data Type</th>
      <th class="text-left" style="width: 5%">Atomic Type</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>Boolean</code></td>
      <td><code>BooleanType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>TinyInt</code></td>
      <td><code>TinyIntType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>SmallInt</code></td>
      <td><code>SmallIntType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>Int</code></td>
      <td><code>IntType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>BigInt</code></td>
      <td><code>BigIntType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>Float</code></td>
      <td><code>FloatType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>Double</code></td>
      <td><code>DoubleType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>Varchar</code></td>
      <td><code>VarCharType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>Char</code></td>
      <td><code>CharType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>Binary</code></td>
      <td><code>VarBinaryType, BinaryType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>Decimal(precision, scale)</code></td>
      <td><code>DecimalType(precision, scale)</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>Datetime</code></td>
      <td><code>TimestampType,LocalZonedTimestampType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>Date</code></td>
      <td><code>DateType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>Array</code></td>
      <td><code>ArrayType</code></td>
      <td>false</td>
    </tr>
    <tr>
      <td><code>Map</code></td>
      <td><code>MapType</code></td>
      <td>false</td>
    </tr>
    <tr>
      <td><code>Struct</code></td>
      <td><code>RowType</code></td>
      <td>false</td>
    </tr>
    </tbody>
</table>

