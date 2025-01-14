---
title: "Migration From Iceberg"
weight: 1
type: docs
aliases:
- /migration/migration-from-iceberg.html
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

# Iceberg Migration

Apache Iceberg data with parquet file format could be migrated to Apache Paimon.
When migrating an iceberg table to a paimon table, the origin iceberg table will be permanently disappeared. **So please back up your data if you
still need the original table.** The migrated paimon table will be a [append table]({{< ref "append-table/overview" >}}).

<span style="color: red; "> **We highly recommend to back up iceberg table data before migrating, because migrating action is not atomic. If been interrupted while migrating, you may lose your data.** </span>

## Migrate Iceberg Table
Currently, we can use paimon catalog with MigrateTableProcedure or MigrateTableAction to migrate the data used by **latest iceberg snapshot** 
in an iceberg table to a paimon table. 

Iceberg tables managed by hadoop-catalog or hive-catalog are supported to be migrated to paimon.
As for the type of paimon catalog, it only needs to have access to the file system where the iceberg metadata and data files are located. 
This means we could migrate an iceberg table managed by hadoop-catalog to a paimon table in hive catalog if their warehouses are in the same file system.

When migrating, the iceberg data files which were marked by DELETED will be ignored. Only the data files referenced by manifest entries with 'EXISTING' and 'ADDED' content will be migrated to paimon.
Notably, now we don't support migrating iceberg tables with delete files(deletion vectors, position delete files, equality delete files etc.)

Now only parquet format is supported in iceberg migration.

### MigrateTableProcedure
You can run the following command to migrate an iceberg table to a paimon table.
```sql
-- Use named argument
CALL sys.migrate_table(connector => 'iceberg', source_table => '${database_name.table_name}', options => '${paimon_options}', parallelism => ${parallelism}, iceberg_options => '${iceberg_options}');

-- Use indexed argument
CALL sys.migrate_table('connector', 'source_table', 'options', 'parallelism', 'iceberg_options');
```
* `connector` is used to specify the data source, in iceberg migration, it is always `iceberg`.
* `source_table` is used to specify the source iceberg table to migrate, it's required;
* `options` is used to specify the additional options for the target paimon table, it's optional.
* `parallelism`, integer type, is used to specify the parallelism of the migration job, it's optional.
* `iceberg_options` is used to specify the configuration of migration, multiple configuration items are separated by commas. it's required. 

#### hadoop-catalog
To migrate iceberg table managed by hadoop-catalog, you need set `metadata.iceberg.storage=hadoop-catalog` and `iceberg_warehouse`. Example:
```sql
CREATE CATALOG paimon_catalog WITH ('type' = 'paimon', 'warehouse' = '/path/to/paimon/warehouse');

USE CATALOG paimon_catalog;

CALL sys.migrate_table(
    connector => 'iceberg', 
    source_table => 'iceberg_db.iceberg_tbl',
    iceberg_options => 'metadata.iceberg.storage=hadoop-catalog,iceberg_warehouse=/path/to/iceberg/warehouse'
);
```
If you want the metadata of the migrated paimon table to be managed by hive, you can also create a hive catalog of paimon for migration. Example:
```sql
CREATE CATALOG paimon_catalog WITH (
    'type' = 'paimon', 
    'metastore' = 'hive', 
    'uri' = 'thrift://localhost:9083', 
    'warehouse' = '/path/to/paimon/warehouse'
);

USE CATALOG paimon_catalog;

CALL sys.migrate_table(
    connector => 'iceberg', 
    source_table => 'iceberg_db.iceberg_tbl',
    iceberg_options => 'metadata.iceberg.storage=hadoop-catalog,iceberg_warehouse=/path/to/iceberg/warehouse'
);
```

#### hive-catalog
To migrate iceberg table managed by hive-catalog, you need set `metadata.iceberg.storage=hive-catalog` 
and provide information about Hive Metastore used by the iceberg table in `iceberg_options`.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Option</th>
      <th class="text-left" style="width: 5%">Default</th>
      <th class="text-left" style="width: 10%">Type</th>
      <th class="text-left" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>metadata.iceberg.uri</h5></td>
      <td style="word-wrap: break-word;"></td>
      <td>String</td>
      <td>Hive metastore uri for Iceberg Hive catalog.</td>
    </tr>
    <tr>
      <td><h5>metadata.iceberg.hive-conf-dir</h5></td>
      <td style="word-wrap: break-word;"></td>
      <td>String</td>
      <td>hive-conf-dir for Iceberg Hive catalog.</td>
    </tr>
    <tr>
      <td><h5>metadata.iceberg.hadoop-conf-dir</h5></td>
      <td style="word-wrap: break-word;"></td>
      <td>String</td>
      <td>hadoop-conf-dir for Iceberg Hive catalog.</td>
    </tr>
    <tr>
      <td><h5>metadata.iceberg.hive-client-class</h5></td>
      <td style="word-wrap: break-word;">org.apache.hadoop.hive.metastore.HiveMetaStoreClient</td>
      <td>String</td>
      <td>Hive client class name for Iceberg Hive Catalog.</td>
    </tr>
    </tbody>
</table>

Example:
```sql
CREATE CATALOG paimon_catalog WITH (
    'type' = 'paimon', 
    'metastore' = 'hive', 
    'uri' = 'thrift://localhost:9083', 
    'warehouse' = '/path/to/paimon/warehouse'
);

USE CATALOG paimon_catalog;

CALL sys.migrate_table(
    connector => 'iceberg', 
    source_table => 'iceberg_db.iceberg_tbl',
    iceberg_options => 'metadata.iceberg.storage=hive-catalog,metadata.iceberg.uri=thrift://localhost:9083'
);
```

### MigrateTableAction
You can also use flink action for migration:
```bash
<FLINK_HOME>/bin/flink run \
/path/to/paimon-flink-action-{{< version >}}.jar \
migrate_table \
--source_type iceberg \
--table <icebergDatabase.icebergTable> \
--iceberg_options <iceberg-conf  [,iceberg-conf ...]> \
[--parallelism <parallelism>] \
[--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]] \
[--options <paimon-table-conf  [,paimon-table-conf ...]> ]
```

Example:
```bash
<FLINK_HOME>/bin/flink run \
/path/to/paimon-flink-action-{{< version >}}.jar \
migrate_table \
--source_type iceberg \
--table iceberg_db.iceberg_tbl \
--iceberg_options metadata.iceberg.storage=hive-catalog, metadata.iceberg.uri=thrift://localhost:9083 \
--parallelism 6 \
--catalog_conf warehouse=/path/to/paimon/warehouse \
--catalog_conf metastore=hive \
--catalog_conf uri=thrift://localhost:9083
```
