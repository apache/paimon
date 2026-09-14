---
title: "Doris"
sidebar_position: 3
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

Query existing Paimon tables through a Doris external catalog. Use
[Connecting Engines](./connecting-engines) to identify the catalog backend and warehouse first.

## Version

Select a Doris release that supports your catalog backend and Paimon table features.
The [Doris Paimon catalog documentation](https://doris.apache.org/docs/3.x/lakehouse/catalogs/paimon-catalog/)
provides version-specific settings. The REST catalog example below requires Doris 3.1 or later.

This integration reads Paimon tables. An external catalog exposes existing tables without copying
them into Doris; it does not enable writes to Paimon.

## Prerequisites

Prepare an existing Paimon table and configure metastore and storage access for the Doris
processes that perform metadata and data reads. The examples below use placeholder hosts and
credentials; replace them with your deployment's settings.

## Create Paimon Catalog

Choose one catalog definition for the backend used by the writer. HDFS examples assume the
cluster's Hadoop access is already configured.

### Filesystem Catalog on HDFS

```sql
CREATE CATALOG paimon_hdfs PROPERTIES (
    'type' = 'paimon',
    'paimon.catalog.type' = 'filesystem',
    'warehouse' = 'hdfs://namenode:8020/warehouse/paimon',
    'hadoop.username' = 'hadoop'
);
```

### Filesystem Catalog on OSS

```sql
CREATE CATALOG paimon_oss PROPERTIES (
    'type' = 'paimon',
    'paimon.catalog.type' = 'filesystem',
    'warehouse' = 'oss://paimon-bucket/warehouse',
    'oss.endpoint' = 'oss-cn-beijing.aliyuncs.com',
    'oss.access_key' = '<access-key-id>',
    'oss.secret_key' = '<access-key-secret>'
);
```

### Hive Metastore Catalog

```sql
CREATE CATALOG paimon_hms PROPERTIES (
    'type' = 'paimon',
    'paimon.catalog.type' = 'hms',
    'warehouse' = 'hdfs://namenode:8020/warehouse/paimon',
    'hive.metastore.uris' = 'thrift://metastore:9083',
    'hadoop.username' = 'hadoop'
);
```

### DLF 1.0 Catalog

```sql
CREATE CATALOG paimon_dlf PROPERTIES (
    'type' = 'paimon',
    'paimon.catalog.type' = 'dlf',
    'warehouse' = 'oss://paimon-bucket/warehouse',
    'dlf.proxy.mode' = 'DLF_ONLY',
    'dlf.uid' = '<account-id>',
    'dlf.region' = 'cn-beijing',
    'dlf.access_key' = '<access-key-id>',
    'dlf.secret_key' = '<access-key-secret>'
);
```

### DLF REST Catalog

For Doris 3.1+, configure the REST backend explicitly. Here, `warehouse` is the DLF catalog
name, rather than an object-storage path.

```sql
CREATE CATALOG dlf_paimon_rest PROPERTIES (
    'type' = 'paimon',
    'paimon.catalog.type' = 'rest',
    'uri' = 'http://cn-beijing-vpc.dlf.aliyuncs.com',
    'warehouse' = '<catalog-name>',
    'paimon.rest.token.provider' = 'dlf',
    'paimon.rest.dlf.access-key-id' = '<access-key-id>',
    'paimon.rest.dlf.access-key-secret' = '<access-key-secret>'
);
```

See the [Doris catalog examples](https://doris.apache.org/docs/3.x/lakehouse/catalogs/paimon-catalog/#examples)
for storage authentication and differences between Doris releases.

## Access Paimon Catalog

Query a fully qualified table name:

```sql
SELECT * FROM paimon_hdfs.paimon_db.paimon_table LIMIT 10;
```

Alternatively, select the catalog and database for the session:

```sql
SWITCH paimon_hdfs;
USE paimon_db;
SELECT * FROM paimon_table LIMIT 10;
```

## Query Optimization

Primary-key tables can require merging multiple row versions. Review the reader capabilities in
your Doris release before selecting a [table mode](../primary-key-table/table-mode).

- **Read optimized:** reading compacted base data can reduce merge work, but a
  [read-optimized view](../concepts/system-tables#read-optimized-table) can omit recent changes.
- **Deletion vectors:** readers must apply deletion vectors to suppress obsolete rows. See
  [Merge On Write](../primary-key-table/table-mode#merge-on-write) for the Paimon configuration
  and read semantics, and check Doris support before enabling it.

## Type Mapping {#doris-to-paimon-type-mapping}

This is a summary of common **Paimon-to-Doris read mappings**. Check the
[Doris type mapping](https://doris.apache.org/docs/3.x/lakehouse/catalogs/paimon-catalog/#column-type-mapping)
for the installed version; binary and timestamp mappings can depend on release and catalog options.

| Paimon type | Doris type |
| --- | --- |
| `BOOLEAN` | `BOOLEAN` |
| `TINYINT`, `SMALLINT`, `INT`, `BIGINT` | Corresponding integer type |
| `FLOAT`, `DOUBLE` | `FLOAT`, `DOUBLE` |
| `DECIMAL(p, s)` | `DECIMAL(p, s)` |
| `CHAR`, `VARCHAR` | `STRING` |
| `BINARY`, `VARBINARY` | See the version-specific binary mapping |
| `DATE` | `DATE` |
| `TIMESTAMP`, `TIMESTAMP WITH LOCAL TIME ZONE` | See the version-specific timestamp mapping and precision limits |
| `ARRAY` | `ARRAY` |
| `MAP` | `MAP` |
| `ROW` | `STRUCT` |

## Next Steps

Use the Doris guide for time-travel and system-table syntax supported by your release. For
connection or visibility problems, follow [Connecting Engines](./connecting-engines#troubleshooting).
