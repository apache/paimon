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

Paimon supports clone tables of the latest Snapshot for data migration.

{{< hint info >}}
1、Clone Tables only support batch mode yet. Please use -D execution.runtime-mode=batch or -yD execution.runtime-mode=batch (for the ON-YARN scenario) to run clone job.

2、If you want clone job runs quickly, you can add parameter parallelism.

3、Only support Flink now.
{{< /hint >}}

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
    [--database <source-database-name>] \
    [--table <source-table-name>] \
    [--catalog_conf <source-paimon-catalog-conf> [--catalog_conf <source-paimon-catalog-conf> ...]] \
    --target_warehouse <target-warehouse-path> \
    [--target_database <target-database>] \
    [--target_table <target-table-name>] \
    [--target_catalog_conf <target-paimon-catalog-conf> [--target_catalog_conf <target-paimon-catalog-conf> ...]]
    [--parallelism 128 ]
```

{{< hint info >}}
1、If the database parameter is not passed, then all tables of all databases will be cloned.
2、If the table parameter is not passed, then all tables of the database will be cloned.
{{< /hint >}}

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
    --target_catalog_conf s3.secret-key=*****
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
CALL sys.clone('source_warehouse', 'source_database', 'source_table', '', 'target_warehouse', 'target_database', 'target_table', '', '')
```

{{< /tab >}}

{{< /tabs >}}

