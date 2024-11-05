---
title: "Clone Tables"
weight: 94
type: docs
aliases:
- /flink/clone-tables.html
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

# Clone Tables

Paimon supports cloning tables for data migration.
Currently, only table files used by the latest snapshot will be cloned.

To clone a table, run the following command to submit a clone job.
If the table you clone is not modified at the same time, it is recommended to submit a Flink batch job for better performance.
However, if you want to clone the table while writing it at the same time, submit a Flink streaming job for automatic failure recovery.

{{< tabs "clone" >}}

{{< tab "Flink SQL" >}}

```sql
CALL sys.clone(
    warehouse => 'source_warehouse_path`,
    [`database` => 'source_database_name',]
    [`table` => 'source_table_name',] 
    target_warehouse => 'target_warehouse_path`,
    [target_database => 'target_database_name',]
    [target_table => 'target_table_name',]
    [parallelism => <parallelism>]
);
```

{{< hint info >}}
1. If `database` is not specified, all tables in all databases of the specified warehouse will be cloned.
2. If `table` is not specified, all tables of the specified database will be cloned.
   {{< /hint >}}

Example: Clone `test_db.test_table` from source warehouse to target warehouse.

```sql
CALL sys.clone(
    'warehouse' => 's3:///path/to/warehouse_source',
    'database' => 'test_db',
    'table' => 'test_table',
    'catalog_conf' => 's3.endpoint=https://****.com;s3.access-key=*****;s3.secret-key=*****',
    'target_warehouse' => 's3:///path/to/warehouse_target',
    'target_database' => 'test_db',
    'target_table' => 'test_table',
    'target_catalog_conf' => 's3.endpoint=https://****.com;s3.access-key=*****;s3.secret-key=*****'
);
```

{{< /tab >}}

{{< tab "Flink Action" >}}

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    clone \
    --warehouse <source-warehouse-path> \
    [--database <source-database-name>] \
    [--table <source-table-name>] \
    [--catalog_conf <source-paimon-catalog-conf> [--catalog_conf <source-paimon-catalog-conf> ...]] \
    --target_warehouse <target-warehouse-path> \
    [--target_database <target-database>] \
    [--target_table <target-table-name>] \
    [--target_catalog_conf <target-paimon-catalog-conf> [--target_catalog_conf <target-paimon-catalog-conf> ...]]
    [--parallelism <parallelism>]
```

{{< hint info >}}
1. If `database` is not specified, all tables in all databases of the specified warehouse will be cloned.
2. If `table` is not specified, all tables of the specified database will be cloned.
{{< /hint >}}

Example: Clone `test_db.test_table` from source warehouse to target warehouse.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    clone \
    --warehouse s3:///path/to/warehouse_source \
    --database test_db \
    --table test_table \
    --catalog_conf s3.endpoint=https://****.com \
    --catalog_conf s3.access-key=***** \
    --catalog_conf s3.secret-key=***** \
    --target_warehouse s3:///path/to/warehouse_target \
    --target_database test_db \
    --target_table test_table \
    --target_catalog_conf s3.endpoint=https://****.com \
    --target_catalog_conf s3.access-key=***** \
    --target_catalog_conf s3.secret-key=*****
```

For more usage of the clone action, see

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    clone --help
```

{{< /tab >}}

{{< /tabs >}}
