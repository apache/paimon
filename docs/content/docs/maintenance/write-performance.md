---
title: "Write Performance"
weight: 1
type: docs
aliases:
- /maintenance/write-performance.html
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

# Write Performance

Performance of Table Store writers are related with the following factors.

## Parallelism

It is recommended that the parallelism of sink should be less than or equal to the number of buckets, preferably equal. You can control the parallelism of the sink with the `sink.parallelism` table property.

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
      <td><h5>sink.parallelism</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>Defines the parallelism of the sink operator. By default, the parallelism is determined by the framework using the same parallelism of the upstream chained operator.</td>
    </tr>
    </tbody>
</table>

## Compaction

### Number of Sorted Runs to Trigger Compaction

Table Store uses [LSM tree]({{< ref "docs/concepts/lsm-trees" >}}) which supports a large number of updates. LSM organizes files in several [sorted runs]({{< ref "docs/concepts/lsm-trees#sorted-runs" >}}). When querying records from an LSM tree, all sorted runs must be combined to produce a complete view of all records.

One can easily see that too many sorted runs will result in poor query performance. To keep the number of sorted runs in a reasonable range, Table Store writers will automatically perform [compactions]({{< ref "docs/concepts/lsm-trees#compactions" >}}). The following table property determines the minimum number of sorted runs to trigger a compaction.

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
      <td><h5>num-sorted-run.compaction-trigger</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">5</td>
      <td>Integer</td>
      <td>The sorted run number to trigger compaction. Includes level0 files (one file one sorted run) and high-level runs (one level one sorted run).</td>
    </tr>
    </tbody>
</table>

Compaction will become less frequent when `num-sorted-run.compaction-trigger` becomes larger, thus improving writing performance. However, if this value becomes too large, more memory and CPU time will be needed when querying the table. This is a trade-off between writing and query performance.

### Number of Sorted Runs to Pause Writing

When number of sorted runs is small, Table Store writers will perform compaction asynchronously in separated threads, so records can be continuously written into the table. However to avoid unbounded growth of sorted runs, writers will have to pause writing when the number of sorted runs hits the threshold. The following table property determines the threshold.

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
      <td><h5>num-sorted-run.stop-trigger</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">10</td>
      <td>Integer</td>
      <td>The number of sorted-runs that trigger the stopping of writes.</td>
    </tr>
    </tbody>
</table>

Write stalls will become less frequent when `num-sorted-run.stop-trigger` becomes larger, thus improving writing performance. However, if this value becomes too large, more memory and CPU time will be needed when querying the table. This is a trade-off between writing and query performance.

### Dedicated Compaction Job

By default, Table Store writers will perform compaction as needed when writing records. This is sufficient for most use cases, but there are two downsides:

* This may result in unstable write throughput because throughput might temporarily drop when performing a compaction.
* Compaction will mark some data files as "deleted" (not really deleted, see [expiring snapshots]({{< ref "docs/maintenance/expiring-snapshots" >}}) for more info). If multiple writers mark the same file a conflict will occur when committing the changes. Table Store will automatically resolve the conflict, but this may result in job restarts.

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
    -c org.apache.flink.table.store.connector.action.FlinkActions \
    /path/to/flink-table-store-flink-**-{{< version >}}.jar \
    compact \
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name>
```

If you submit a batch job (set `execution.runtime-mode: batch` in Flink's configuration), all current table files will be compacted. If you submit a streaming job (set `execution.runtime-mode: streaming` in Flink's configuration), the job will continuously monitor new changes to the table and perform compactions as needed.

{{< hint info >}}

If you only want to submit the compaction job and don't want to wait until the job is done, you should submit in [detached mode](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/cli/#submitting-a-job).

{{< /hint >}}

For more usage of the compact action, see

```bash
<FLINK_HOME>/bin/flink run \
    -c org.apache.flink.table.store.connector.action.FlinkActions \
    /path/to/flink-table-store-flink-**-{{< version >}}.jar \
    compact --help
```

{{< /tab >}}

{{< /tabs >}}

## Memory

There are three main places in Table Store writer that takes up memory:

* Writer's memory buffer, shared and preempted by all writers of a single task. This memory value can be adjusted by the `write-buffer-size` table property.
* Memory consumed when merging several sorted runs for compaction. Can be adjusted by the `num-sorted-run.compaction-trigger` option to change the number of sorted runs to be merged.
* The memory consumed by writing columnar (ORC, Parquet, etc.) file, which is not adjustable.
