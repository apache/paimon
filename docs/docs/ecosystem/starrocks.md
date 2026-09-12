---
title: "StarRocks"
sidebar_position: 2
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

Query existing Paimon tables through a StarRocks external catalog. Start with
[Connecting Engines](./connecting-engines) if you need to identify the catalog and warehouse.

## Version

Paimon catalogs are available in StarRocks 3.1 and later. Individual features depend on the
StarRocks release. Use the [StarRocks Paimon catalog documentation](https://docs.starrocks.io/docs/data_source/catalog/paimon_catalog/)
for the supported catalog backends, storage configuration, and release-specific limitations.

This integration reads Paimon tables. Creating an external catalog does not create or copy the
tables, and the catalog does not support inserting, updating, or deleting Paimon data.

## Prerequisites

Prepare an existing Paimon database and table. Ensure the StarRocks processes that access metadata
and data can reach the metastore and warehouse. Configure authentication and filesystem access
using the StarRocks documentation for your storage system.

## Create Paimon Catalog

Choose the backend used by the writer. These examples assume HDFS access is already configured;
replace the host names and warehouse path with your deployment values.

### Filesystem Catalog

```sql
CREATE EXTERNAL CATALOG paimon_catalog PROPERTIES (
    "type" = "paimon",
    "paimon.catalog.type" = "filesystem",
    "paimon.catalog.warehouse" = "hdfs://namenode:8020/warehouse/paimon"
);
```

### Hive Metastore Catalog

```sql
CREATE EXTERNAL CATALOG paimon_hms PROPERTIES (
    "type" = "paimon",
    "paimon.catalog.type" = "hive",
    "paimon.catalog.warehouse" = "hdfs://namenode:8020/warehouse/paimon",
    "hive.metastore.uris" = "thrift://metastore:9083"
);
```

For object storage, use the warehouse URI and authentication properties from the
[StarRocks catalog examples](https://docs.starrocks.io/docs/data_source/catalog/paimon_catalog/#examples).

## Query

Assume `test_db.test_tbl` already exists in the configured warehouse:

```sql
SELECT * FROM paimon_catalog.test_db.test_tbl LIMIT 10;
```

## Query System Tables

On StarRocks versions supporting Paimon system tables, append the system-table suffix to the
table name. Quote the complete table identifier containing `$`:

```sql
SELECT * FROM paimon_catalog.test_db.`test_tbl$partitions`;
SELECT * FROM paimon_catalog.test_db.`test_tbl$ro`;
```

The [`$ro` table](../concepts/system-tables#read-optimized-table) reads compacted data from a
primary-key table. Its results can lag behind the latest snapshot until a full compaction
completes. Use the ordinary table query when you need the latest state supported by the reader.
See [MOR Read Optimized](../primary-key-table/table-mode#mor-read-optimized) for maintenance settings.

## Type Mapping {#starrocks-to-paimon-type-mapping}

The following table summarizes common Paimon types as exposed by StarRocks. It describes the
**read direction**; it does not imply support for creating these types in Paimon from StarRocks.
Refer to the [upstream type mapping](https://docs.starrocks.io/docs/data_source/catalog/paimon_catalog/#paimon-to-starrocks-data-types)
for your release, including precision and type limits.

| Paimon type | StarRocks type |
| --- | --- |
| `BOOLEAN` | `BOOLEAN` |
| `TINYINT`, `SMALLINT`, `INT`, `BIGINT` | Corresponding integer type |
| `FLOAT`, `DOUBLE` | `FLOAT`, `DOUBLE` |
| `DECIMAL(p, s)` | `DECIMAL(p, s)` |
| `CHAR(n)` | `CHAR(n)` |
| `VARCHAR(n)`, `STRING` | `VARCHAR` |
| `BINARY(n)`, `VARBINARY(n)` | `VARBINARY` |
| `DATE` | `DATE` |
| `TIMESTAMP`, `TIMESTAMP WITH LOCAL TIME ZONE` | `DATETIME` |
| `ARRAY` | `ARRAY` |
| `MAP` | `MAP` |
| `ROW` | `STRUCT` |

## Next Steps

- Review [query performance](../primary-key-table/query-performance) and [table modes](../primary-key-table/table-mode).
- Diagnose catalog and storage issues with [Connecting Engines](./connecting-engines#troubleshooting).
