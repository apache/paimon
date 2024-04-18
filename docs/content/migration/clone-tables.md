---
title: "Clone Tables"
weight: 3
type: docs
aliases:
- /migration/clone-tables.html
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

Paimon supports clone tables of various granularities for data migration.

{{< hint info >}}
1、Clone Tables only support batch mode yet. Please use -D execution.runtime-mode=batch or -yD execution.runtime-mode=batch (for the ON-YARN scenario) to run clone job.

2、If you want clone job runs quickly, you can add parameter parallelism.
{{< /hint >}}

3、Only support Flink now.

## Clone Table
The target table needs to be a non-existent table, and it will have the exact same schema (only the schema for current snapshot) as the source table.

To run a Flink batch job for clone, follow these instructions.

### LatestSnapshot
Clone the latest snapshot of the source table, copying all the files required for the snapshot to the new target table.

{{< tabs "clone-tables" >}}

{{< tab "Flink" >}}

Flink SQL currently does not support statements related to clone, so we have to submit the clone job through `flink run`.

Run the following command to submit a clone job for the table's latest Snapshot.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    clone \
    --warehouse <source-warehouse-path> \
    --database <source-database-name> \
    --table <source-table-name> \
    [--catalog_conf <source-paimon-catalog-conf> [--catalog_conf <source-paimon-catalog-conf> ...]] \
    --target_warehouse <target-warehouse-path> \
    --target_database <target-database> \
    --target_table <target-table-name> \
    [--target_catalog_conf <target-paimon-catalog-conf> [--target_catalog_conf <target-paimon-catalog-conf> ...]] \
    --clone_type LastestSnapshot \
    [--parallelism 128 ]
```

Example: clone table latest Snapshot.

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
    --target_catalog_conf s3.secret-key=***** \
    --clone_type LastestSnapshot
```

For more usage of the clone action, see

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    clone --help
```

{{< /tab >}}

{{< tab "Flink Procedure" >}}

Run the following command to submit a clone job for the table's latest Snapshot.

```bash
CALL sys.clone('source_warehouse', 'source_database', 'source_table', '', 'target_warehouse', 'target_database', 'target_table', '', '', 'LatestSnapshot')
```

{{< /tab >}}

{{< /tabs >}}

### SpecificSnapshot
You can also specify a snapshot of the source table, copying all the files required for the snapshot to the new target table.

{{< tabs "clone-tables" >}}

{{< tab "Flink" >}}

Run the following command to submit a clone job for the table's Snapshot-10.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    clone \
    --warehouse <source-warehouse-path> \
    --database <source-database-name> \
    --table <source-table-name> \
    [--catalog_conf <source-paimon-catalog-conf> [--catalog_conf <source-paimon-catalog-conf> ...]] \
    --target_warehouse <target-warehouse-path> \
    --target_database <target-database> \
    --target_table <target-table-name> \
    [--target_catalog_conf <target-paimon-catalog-conf> [--target_catalog_conf <target-paimon-catalog-conf> ...]] \
    --clone_type SpecificSnapshot \
    --snapshot_id 10 \
    [--parallelism 128 ]
```
Snapshot-10 will be cloned into the target table.

{{< /tab >}}

{{< tab "Flink Procedure" >}}

Run the following command to submit a clone job for the table's Snapshot-10.

```bash
CALL sys.clone('source_warehouse', 'source_database', 'source_table', '', 'target_warehouse', 'target_database', 'target_table', '', '', 'SpecificSnapshot', 10)
```

{{< /tab >}}

{{< /tabs >}}

### FromTimestamp
Snapshot around timestamp (If there is no snapshot earlier than this time, the earliest snapshot will be chosen) will be cloned from source table to the target table.

{{< tabs "clone-tables" >}}

{{< tab "Flink" >}}

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    clone \
    --warehouse <source-warehouse-path> \
    --database <source-database-name> \
    --table <source-table-name> \
    [--catalog_conf <source-paimon-catalog-conf> [--catalog_conf <source-paimon-catalog-conf> ...]] \
    --target_warehouse <target-warehouse-path> \
    --target_database <target-database> \
    --target_table <target-table-name> \
    [--target_catalog_conf <target-paimon-catalog-conf> [--target_catalog_conf <target-paimon-catalog-conf> ...]] \
    --clone_type FromTimestamp \
    --timestamp 1672599841000 \
    [--parallelism 128 ]
```
Snapshot around timestamp '2023-01-02 03:04:01' (If there is no snapshot earlier than this time, the earliest snapshot will be chosen) will be cloned into the target table.

{{< /tab >}}

{{< tab "Flink Procedure" >}}

Run the following command to submit a clone job for the Snapshot around timestamp '2023-01-02 03:04:01' (If there is no snapshot earlier than this time, the earliest snapshot will be chosen) will be cloned into the target table.

```bash
CALL sys.clone('source_warehouse', 'source_database', 'source_table', '', 'target_warehouse', 'target_database', 'target_table', '', '', 'FromTimestamp', 1672599841000)
```

{{< /tab >}}

{{< /tabs >}}

### Tag
Clone a tag from source table to the target table.

{{< tabs "clone-tables" >}}

{{< tab "Flink" >}}

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    clone \
    --warehouse <source-warehouse-path> \
    --database <source-database-name> \
    --table <source-table-name> \
    [--catalog_conf <source-paimon-catalog-conf> [--catalog_conf <source-paimon-catalog-conf> ...]] \
    --target_warehouse <target-warehouse-path> \
    --target_database <target-database> \
    --target_table <target-table-name> \
    [--target_catalog_conf <target-paimon-catalog-conf> [--target_catalog_conf <target-paimon-catalog-conf> ...]] \
    --clone_type Tag \
    --tag_name 20240318 \
    [--parallelism 128 ]
```
Tag '20240318' will be cloned into the target table, this tag will also be retained in the target table.

{{< /tab >}}

{{< tab "Flink Procedure" >}}

Run the following command to submit a clone job for the Tag 20240318.

```bash
CALL sys.clone('source_warehouse', 'source_database', 'source_table', '', 'target_warehouse', 'target_database', 'target_table', '', '', 'Tag', '20240318')
```

{{< /tab >}}

{{< /tabs >}}

### Table
You can also clone all snapshots and tags from source table to target table before the command starts, this is mainly used for data migration.

{{< tabs "clone-tables" >}}

{{< tab "Flink" >}}

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    clone \
    --warehouse <source-warehouse-path> \
    --database <source-database-name> \
    --table <source-table-name> \
    [--catalog_conf <source-paimon-catalog-conf> [--catalog_conf <source-paimon-catalog-conf> ...]] \
    --target_warehouse <target-warehouse-path> \
    --target_database <target-database> \
    --target_table <target-table-name> \
    [--target_catalog_conf <target-paimon-catalog-conf> [--target_catalog_conf <target-paimon-catalog-conf> ...]] \
    --clone_type Table \
    [--parallelism 2048 ]
```

{{< /tab >}}

{{< tab "Flink Procedure" >}}

Run the following command to submit a clone job for the whole table.

```bash
CALL sys.clone('source_warehouse', 'source_database', 'source_table', '', 'target_warehouse', 'target_database', 'target_table', '', '', 'Table')
```

{{< /tab >}}

{{< /tabs >}}

## Clone Database

Because user may have hundreds even thousands of tables need migration, so Paimon supports clone the whole database.

You can run the following command to submit a clone job for all tables of the database.

{{< tabs "clone-tables" >}}

{{< tab "Flink" >}}

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    clone_database \
    --warehouse <source-warehouse-path> \
    --database <source-database-name> \
    --table <source-table-name> \
    [--catalog_conf <source-paimon-catalog-conf> [--catalog_conf <source-paimon-catalog-conf> ...]] \
    --target_warehouse <target-warehouse-path> \
    --target_database <target-database> \
    --target_table <target-table-name> \
    [--target_catalog_conf <target-paimon-catalog-conf> [--target_catalog_conf <target-paimon-catalog-conf> ...]] \
    [--parallelism 32768 ]
```

{{< /tab >}}

{{< tab "Flink Procedure" >}}

Run the following command to submit a clone job for the whole database.

```bash
CALL sys.clone_database('source_warehouse', 'source_database', '', 'target_warehouse', 'target_database', '', '')
```

{{< /tab >}}

{{< /tabs >}}
