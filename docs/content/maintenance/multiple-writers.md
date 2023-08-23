---
title: "Multiple Writers"
weight: 3
type: docs
aliases:
- /maintenance/multiple-writers.html
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

# Multiple Writers

Paimon's snapshot management supports writing with multiple writers.

{{< hint info >}}
For S3-like object store, its `'RENAME'` does not have atomic semantic. We need to configure Hive metastore and
enable `'lock.enabled'` option for the catalog.
{{< /hint >}}

By default, Paimon supports concurrent writing to different partitions. A recommended mode is that streaming
job writes records to Paimon's latest partition; Simultaneously batch job (overwrite) writes records to the
historical partition.

{{< img src="/img/multiple-writers.png">}}

So far, everything works very well, but if you need multiple writers to write records to the same partition, it will 
be a bit more complicated. For example, you don't want to use `UNION ALL`, you have multiple
streaming jobs to write records to a `'partial-update'` table. Please refer to the `'Dedicated Compaction Job'` below.

## Dedicated Compaction Job

By default, Paimon writers will perform compaction as needed during writing records. This is sufficient for most use cases, but there are two downsides:

* This may result in unstable write throughput because throughput might temporarily drop when performing a compaction.
* Compaction will mark some data files as "deleted" (not really deleted, see [expiring snapshots]({{< ref "maintenance/manage-snapshots#expiring-snapshots" >}}) for more info). If multiple writers mark the same file, a conflict will occur when committing the changes. Paimon will automatically resolve the conflict, but this may result in job restarts.

To avoid these downsides, users can also choose to skip compactions in writers, and run a dedicated job only for compaction. As compactions are performed only by the dedicated job, writers can continuously write records without pausing and no conflicts will ever occur.

To skip compactions in writers, set the following table property to `true`.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Option</th>
      <th class="text-left" style="width: 5%">Required</th>
      <th class="text-left" style="width: 5%">Default</th>
      <th class="text-left" style="width: 10%">Type</th>
      <th class="text-left" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>write-only</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>If set to true, compactions and snapshot expiration will be skipped. This option is used along with dedicated compact jobs.</td>
    </tr>
    </tbody>
</table>

To run a dedicated job for compaction, follow these instructions.

{{< tabs "dedicated-compaction-job" >}}

{{< tab "Flink" >}}

Flink SQL currently does not support statements related to compactions, so we have to submit the compaction job through `flink run`.

Run the following command to submit a compaction job for the table.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    compact \
    --warehouse <warehouse-path> \
    --database <database-name> \ 
    --table <table-name> \
    [--partition <partition-name>] \
    [--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] \
```

Or run the following command to submit a compaction job for multiple database.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    compact-database \
    --warehouse <warehouse-path> \
    --database <database-name> \ 
    [--including-tables <paimon-table-name|name-regular-expr>] \
    [--excluding-tables <paimon-table-name|name-regular-expr>] \
    [--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] 
```

* `--database` is used to specify which database is to be compacted. In compact mode, you need to specify a database name, in compact-database mode, you could specify multiple database, regular expression is supported.
* `--including-tables` is used to specify which source tables are to be compacted, you must use '|' to separate multiple tables, the format is `databaseName.tableName`, regular expression is supported. For example, specifying "--including-tables db1.t1|db2.+" means to compact table 'db1.t1' and all tables in the db2 database.
* `--excluding-tables`  is used to specify which source tables are not to be compacted. The usage is same as "--including-tables". "--excluding-tables" has higher priority than "--including-tables" if you specified both.
* `--catalog-conf` is the configuration for Paimon catalog. Each configuration should be specified in the format `key=value`. See [here]({{< ref "maintenance/configurations" >}}) for a complete list of catalog configurations.

If you submit a batch job (set `execution.runtime-mode: batch` in Flink's configuration), all current table files will be compacted. If you submit a streaming job (set `execution.runtime-mode: streaming` in Flink's configuration), the job will continuously monitor new changes to the table and perform compactions as needed.

{{< hint info >}}

If you only want to submit the compaction job and don't want to wait until the job is done, you should submit in [detached mode](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/cli/#submitting-a-job).

{{< /hint >}}

Example1: compact table

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    compact \
    --warehouse s3:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition dt=20221126,hh=08 \
    --partition dt=20221127,hh=09 \
    --catalog-conf s3.endpoint=https://****.com \
    --catalog-conf s3.access-key=***** \
    --catalog-conf s3.secret-key=*****
```

Example2: compact database

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    compact-database \
    --warehouse s3:///path/to/warehouse \
    --database test_db \
    --catalog-conf s3.endpoint=https://****.com \
    --catalog-conf s3.access-key=***** \
    --catalog-conf s3.secret-key=*****
```

For more usage of the compact action, see

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    compact --help
```
or

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    compact-database --help
```

{{< /tab >}}

{{< /tabs >}}
