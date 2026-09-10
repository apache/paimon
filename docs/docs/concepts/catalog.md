---
title: "Catalog"
sidebar_position: 4
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

# Catalog

A catalog organizes databases, tables, and their metadata. It lets engines access tables by name
and centralizes operations such as creating a table or changing its schema. Use a Paimon catalog
to share tables across jobs and engines.

## Catalogs

Choose a metastore based on where you want to manage catalog metadata and how clients connect.
Data files remain in the configured filesystem or object store.

| Metastore | Metadata integration | Typical reason to choose it |
| --- | --- | --- |
| `filesystem` (default) | Uses the warehouse filesystem. | Set up a catalog without an external metastore service. |
| `hive` | Registers databases and tables in Hive metastore. | Share table metadata with Hive-compatible tools. |
| `jdbc` | Stores catalog metadata in a relational database. | Use a database-backed catalog, such as MySQL or PostgreSQL. |
| `rest` | Sends catalog operations to a REST service. | Access a remote catalog with server-managed backend logic and authentication. |

Catalog capabilities differ. See [Views](./views#catalog-support) for view support and
[Concurrency Control](./concurrency-control#atomic-publication) for snapshot publication and
locking requirements, especially with multiple writers on object storage.

The examples below use Flink SQL. See [Spark catalog configuration](../spark/sql-ddl#catalog)
for Spark syntax and [Catalog API](../program-api/catalog-api) for programmatic access.

## Filesystem Catalog

Store catalog metadata and table files under the warehouse directory:

```sql
CREATE CATALOG my_catalog WITH (
    'type' = 'paimon',
    'metastore' = 'filesystem',
    'warehouse' = 'hdfs:///path/to/warehouse'
);
```

## REST Catalog

Connect to a catalog service with `metastore = rest`, its URI, warehouse identifier, and an
authentication provider. The service implements the catalog API and manages its backend.

See [REST Catalog](./rest/) for the architecture, connection guides, and API references.

## Hive Catalog

The Hive catalog registers table metadata in Hive metastore while storing Paimon files in the
warehouse. Tables can also be accessed through the [Paimon Hive integration](../ecosystem/hive).

```sql
CREATE CATALOG my_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'warehouse' = 'hdfs:///path/to/warehouse'
);
```

If `warehouse` is omitted, the catalog uses `hive.metastore.warehouse.dir` from `HiveConf`.

By default, Paimon does not synchronize newly created partitions into Hive metastore. Hive sees
an unpartitioned table, and partition pruning is handled through filter pushdown. To register
partitions in Hive metastore, set the table option `metastore.partitioned-table = true`.

## JDBC Catalog

Store catalog metadata in a relational database and table files in the warehouse:

```sql
CREATE CATALOG my_jdbc WITH (
    'type' = 'paimon',
    'metastore' = 'jdbc',
    'uri' = 'jdbc:mysql://<host>:<port>/<databaseName>',
    'jdbc.user' = '<user>',
    'jdbc.password' = '<password>',
    'catalog-key' = 'jdbc',
    'warehouse' = 'hdfs:///path/to/warehouse'
);
```

The JDBC catalog also persists views in an automatically created `paimon_views` table. Tables
and views share one identifier namespace within each database. See
[JDBC catalog notes](./views#jdbc-catalog-notes) for upgrade permissions, locking scope, and
`DROP DATABASE` behavior.

## Next Steps

- Define fields with [Data Types](./data-types), and manage [Views](./views) and [Functions](./functions).
- Inspect table state and catalog metadata through [System Tables](./system-tables).
- Find backend-specific options in [Configurations](../maintenance/configurations).
