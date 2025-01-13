---
title: "StarRocks"
weight: 2
type: docs
aliases:
- /engines/starrocks.html
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

# StarRocks

This documentation is a guide for using Paimon in StarRocks.

## Version

Paimon currently supports StarRocks 3.1 and above. Recommended version is StarRocks 3.2.6 or above.

## Create Paimon Catalog

Paimon catalogs are registered by executing a `CREATE EXTERNAL CATALOG` SQL in StarRocks.
For example, you can use the following SQL to create a Paimon catalog named paimon_catalog.

```sql
CREATE EXTERNAL CATALOG paimon_catalog
PROPERTIES
(
"type" = "paimon",
"paimon.catalog.type" = "filesystem",
"paimon.catalog.warehouse" = "oss://<your_bucket>/user/warehouse/"
);
```

More catalog types and configures can be seen in [Paimon catalog](https://docs.starrocks.io/docs/data_source/catalog/paimon_catalog/).

## Query
Suppose there already exists a database named `test_db` and a table named `test_tbl` in `paimon_catalog`,
you can query this table using the following SQL:
```sql
SELECT * FROM paimon_catalog.test_db.test_tbl;
```

## Query System Tables

You can access all kinds of Paimon system tables by StarRocks. For example, you can read the `ro` 
(read-optimized) system table to improve reading performance of primary-key tables.

```sql
SELECT * FROM paimon_catalog.test_db.test_tbl$ro;
```

For another example, you can query partition files of the table using the following SQL:

```sql
SELECT * FROM paimon_catalog.test_db.partition_tbl$partitions;
/*
+-----------+--------------+--------------------+------------+----------------------------+
| partition | record_count | file_size_in_bytes | file_count | last_update_time           |
+-----------+--------------+--------------------+------------+----------------------------+
| [1]       |            1 |                645 |          1 | 2024-01-01 00:00:00.000000 |
+-----------+--------------+--------------------+------------+----------------------------+
*/
```

## StarRocks to Paimon type mapping

This section lists all supported type conversion between StarRocks and Paimon. 
All StarRocks’s data types can be found in this doc [StarRocks Data type overview](https://docs.starrocks.io/docs/sql-reference/data-types/).

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 10%">StarRocks Data Type</th>
      <th class="text-left" style="width: 10%">Paimon Data Type</th>
      <th class="text-left" style="width: 5%">Atomic Type</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>STRUCT</code></td>
      <td><code>RowType</code></td>
      <td>false</td>
    </tr>
    <tr>
      <td><code>MAP</code></td>
      <td><code>MapType</code></td>
      <td>false</td>
    </tr>
    <tr>
      <td><code>ARRAY</code></td>
      <td><code>ArrayType</code></td>
      <td>false</td>
    </tr>
    <tr>
      <td><code>BOOLEAN</code></td>
      <td><code>BooleanType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>TINYINT</code></td>
      <td><code>TinyIntType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>SMALLINT</code></td>
      <td><code>SmallIntType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>INT</code></td>
      <td><code>IntType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>BIGINT</code></td>
      <td><code>BigIntType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>FLOAT</code></td>
      <td><code>FloatType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>DOUBLE</code></td>
      <td><code>DoubleType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>CHAR(length)</code></td>
      <td><code>CharType(length)</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>VARCHAR(MAX_VARCHAR_LENGTH)</code></td>
      <td><code>VarCharType(VarCharType.MAX_LENGTH)</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>VARCHAR(length)</code></td>
      <td><code>VarCharType(length), length is less than VarCharType.MAX_LENGTH</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>DATE</code></td>
      <td><code>DateType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>DATETIME</code></td>
      <td><code>TimestampType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>DECIMAL(precision, scale)</code></td>
      <td><code>DecimalType(precision, scale)</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>VARBINARY(length)</code></td>
      <td><code>VarBinaryType(length)</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>DATETIME</code></td>
      <td><code>LocalZonedTimestampType</code></td>
      <td>true</td>
    </tr>
    </tbody>
</table>
