---
title: "Catalog"
weight: 4
type: docs
aliases:
- /concepts/catalog.html
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

Paimon provides a Catalog abstraction to manage the table of contents and metadata. The Catalog abstraction provides
a series of ways to help you better integrate with computing engines. We always recommend that you use Catalog to
access the Paimon table.

## Catalogs

Paimon catalogs currently support three types of metastores:

* `filesystem` metastore (default), which stores both metadata and table files in filesystems.
* `hive` metastore, which additionally stores metadata in Hive metastore. Users can directly access the tables from Hive.
* `jdbc` metastore, which additionally stores metadata in relational databases such as MySQL, Postgres, etc.

## Filesystem Catalog

Metadata and table files are stored under `hdfs:///path/to/warehouse`.

```sql
-- Flink SQL
CREATE CATALOG my_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 'hdfs:///path/to/warehouse'
);
```

## Hive Catalog

By using Paimon Hive catalog, changes to the catalog will directly affect the corresponding Hive metastore. Tables
created in such catalog can also be accessed directly from Hive. Metadata and table files are stored under
`hdfs:///path/to/warehouse`. In addition, schema is also stored in Hive metastore.

```sql
-- Flink SQL
CREATE CATALOG my_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    -- 'warehouse' = 'hdfs:///path/to/warehouse', default use 'hive.metastore.warehouse.dir' in HiveConf
);
```

By default, Paimon does not synchronize newly created partitions into Hive metastore. Users will see an unpartitioned
table in Hive. Partition push-down will be carried out by filter push-down instead.

If you want to see a partitioned table in Hive and also synchronize newly created partitions into Hive metastore,
please set the table option `metastore.partitioned-table` to true.

## JDBC Catalog

By using the Paimon JDBC catalog, changes to the catalog will be directly stored in relational databases such as SQLite,
MySQL, postgres, etc.

```sql
-- Flink SQL
CREATE CATALOG my_jdbc WITH (
    'type' = 'paimon',
    'metastore' = 'jdbc',
    'uri' = 'jdbc:mysql://<host>:<port>/<databaseName>',
    'jdbc.user' = '...', 
    'jdbc.password' = '...', 
    'catalog-key'='jdbc',
    'warehouse' = 'hdfs:///path/to/warehouse'
);
```
