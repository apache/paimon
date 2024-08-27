---
title: "Metrics"
weight: 9
type: docs
aliases:
- /maintenance/metrics.html
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

# Paimon Metrics

Paimon has built a metrics system to measure the behaviours of reading and writing, like how many manifest files it scanned in the last planning, how long it took in the last commit operation, how many files it deleted in the last compact operation.

In Paimon's metrics system, metrics are updated and reported at table granularity.

There are three types of metrics provided in the Paimon metric system, `Gauge`, `Counter`, `Histogram`.
- `Gauge`: Provides a value of any type at a point in time.
- `Counter`: Used to count values by incrementing and decrementing.
- `Histogram`: Measure the statistical distribution of a set of values including the min, max, mean, standard deviation and percentile.

Paimon has supported built-in metrics to measure operations of **commits**, **scans**, **writes** and **compactions**, which can be bridged to any computing engine that supports, like Flink, Spark etc.

## Metrics List

Below is lists of Paimon built-in metrics. They are summarized into types of scan metrics, commit metrics, write metrics, write buffer metrics and compaction metrics.

### Scan Metrics

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 225pt">Metrics Name</th>
      <th class="text-left" style="width: 70pt">Type</th>
      <th class="text-left" style="width: 300pt">Description</th>
    </tr>
    </thead>
    <tbody>
        <tr>
            <td>lastScanDuration</td>
            <td>Gauge</td>
            <td>The time it took to complete the last scan.</td>
        </tr>
        <tr>
            <td>scanDuration</td>
            <td>Histogram</td>
            <td>Distributions of the time taken by the last few scans.</td>
        </tr>
        <tr>
            <td>lastScannedManifests</td>
            <td>Gauge</td>
            <td>Number of scanned manifest files in the last scan.</td>
        </tr>
        <tr>
            <td>lastSkippedByPartitionAndStats</td>
            <td>Gauge</td>
            <td>Skipped table files by partition filter and value / key stats information in the last scan.</td>
        </tr>
        <tr>
            <td>lastSkippedByBucketAndLevelFilter</td>
            <td>Gauge</td>
            <td>Skipped table files by bucket, bucket key and level filter in the last scan.</td>
        </tr>
        <tr>
            <td>lastSkippedByWholeBucketFilesFilter</td>
            <td>Gauge</td>
            <td>Skipped table files by bucket level value filter (only primary key table) in the last scan.</td>
        </tr>
        <tr>
            <td>lastScanSkippedTableFiles</td>
            <td>Gauge</td>
            <td>Total skipped table files in the last scan.</td>
        </tr>
        <tr>
            <td>lastScanResultedTableFiles</td>
            <td>Gauge</td>
            <td>Resulted table files in the last scan.</td>
        </tr>
    </tbody>
</table>

### Commit Metrics

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 225pt">Metrics Name</th>
      <th class="text-left" style="width: 70pt">Type</th>
      <th class="text-left" style="width: 300pt">Description</th>
    </tr>
    </thead>
    <tbody>
        <tr>
            <td>lastCommitDuration</td>
            <td>Gauge</td>
            <td>The time it took to complete the last commit.</td>
        </tr>
        <tr>
            <td>commitDuration</td>
            <td>Histogram</td>
            <td>Distributions of the time taken by the last few commits.</td>
        </tr>
        <tr>
            <td>lastCommitAttempts</td>
            <td>Gauge</td>
            <td>The number of attempts the last commit made.</td>
        </tr>
        <tr>
            <td>lastTableFilesAdded</td>
            <td>Gauge</td>
            <td>Number of added table files in the last commit, including newly created data files and compacted after.</td>
        </tr>
        <tr>
            <td>lastTableFilesDeleted</td>
            <td>Gauge</td>
            <td>Number of deleted table files in the last commit, which comes from compacted before.</td>
        </tr>
        <tr>
            <td>lastTableFilesAppended</td>
            <td>Gauge</td>
            <td>Number of appended table files in the last commit, which means the newly created data files.</td>
        </tr>
        <tr>
            <td>lastTableFilesCommitCompacted</td>
            <td>Gauge</td>
            <td>Number of compacted table files in the last commit, including compacted before and after.</td>
        </tr>
        <tr>
            <td>lastChangelogFilesAppended</td>
            <td>Gauge</td>
            <td>Number of appended changelog files in last commit.</td>
        </tr>
        <tr>
            <td>lastChangelogFileCommitCompacted</td>
            <td>Gauge</td>
            <td>Number of compacted changelog files in last commit.</td>
        </tr>
        <tr>
            <td>lastGeneratedSnapshots</td>
            <td>Gauge</td>
            <td>Number of snapshot files generated in the last commit, maybe 1 snapshot or 2 snapshots.</td>
        </tr>
        <tr>
            <td>lastDeltaRecordsAppended</td>
            <td>Gauge</td>
            <td>Delta records count in last commit with APPEND commit kind.</td>
        </tr>
        <tr>
            <td>lastChangelogRecordsAppended</td>
            <td>Gauge</td>
            <td>Changelog records count in last commit with APPEND commit kind.</td>
        </tr>
        <tr>
            <td>lastDeltaRecordsCommitCompacted</td>
            <td>Gauge</td>
            <td>Delta records count in last commit with COMPACT commit kind.</td>
        </tr>
        <tr>
            <td>lastChangelogRecordsCommitCompacted</td>
            <td>Gauge</td>
            <td>Changelog records count in last commit with COMPACT commit kind.</td>
        </tr>
        <tr>
            <td>lastPartitionsWritten</td>
            <td>Gauge</td>
            <td>Number of partitions written in the last commit.</td>
        </tr>
        <tr>
            <td>lastBucketsWritten</td>
            <td>Gauge</td>
            <td>Number of buckets written in the last commit.</td>
        </tr>
    </tbody>
</table>

### Write Buffer Metrics

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 225pt">Metrics Name</th>
      <th class="text-left" style="width: 70pt">Type</th>
      <th class="text-left" style="width: 300pt">Description</th>
    </tr>
    </thead>
    <tbody>
        <tr>
            <td>numWriters</td>
            <td>Gauge</td>
            <td>Number of writers in this parallelism.</td>
        </tr>
        <tr>
            <td>bufferPreemptCount</td>
            <td>Gauge</td>
            <td>The total number of memory preempted.</td>
        </tr>
        <tr>
            <td>usedWriteBufferSizeByte</td>
            <td>Gauge</td>
            <td>Current used write buffer size in byte.</td>
        </tr>
        <tr>
            <td>totalWriteBufferSizeByte</td>
            <td>Gauge</td>
            <td>The total write buffer size configured in byte.</td>
        </tr>
    </tbody>
</table>

### Compaction Metrics

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 225pt">Metrics Name</th>
      <th class="text-left" style="width: 70pt">Type</th>
      <th class="text-left" style="width: 300pt">Description</th>
    </tr>
    </thead>
    <tbody>
        <tr>
            <td>maxLevel0FileCount</td>
            <td>Gauge</td>
            <td>The maximum number of level 0 files currently handled by this writer. This value will become larger if asynchronous compaction cannot be done in time.</td>
        </tr>
        <tr>
            <td>avgLevel0FileCount</td>
            <td>Gauge</td>
            <td>The average number of level 0 files currently handled by this writer. This value will become larger if asynchronous compaction cannot be done in time.</td>
        </tr>
        <tr>
            <td>compactionThreadBusy</td>
            <td>Gauge</td>
            <td>The maximum business of compaction threads in this parallelism. Currently, there is only one compaction thread in each parallelism, so value of business ranges from 0 (idle) to 100 (compaction running all the time).</td>
        </tr>
        <tr>
            <td>avgCompactionTime</td>
            <td>Gauge</td>
            <td>The average runtime of compaction threads, calculated based on recorded compaction time data in milliseconds. The value represents the average duration of compaction operations. Higher values indicate longer average compaction times, which may suggest the need for performance optimization.</td>
        </tr>
        <tr>
            <td>minCompactionTime</td>
            <td>Gauge</td>
            <td>The minimum runtime of compaction threads, recorded based on observed compaction time data in milliseconds. The value represents the shortest duration of any single compaction operation. Lower values indicate faster compaction times for certain operations, highlighting the most efficient runs.</td>
        </tr>
        <tr>
            <td>maxCompactionTime</td>
            <td>Gauge</td>
            <td>The maximum runtime of compaction threads, recorded based on observed compaction time data in milliseconds. The value represents the longest duration of any single compaction operation. Higher values indicate slower compaction times for certain operations, potentially identifying performance bottlenecks.</td>
        </tr>
    </tbody>
</table>

## Bridging To Flink

Paimon has implemented bridging metrics to Flink's metrics system, which can be reported by Flink, and the lifecycle of metric groups are managed by Flink.

Please join the `<scope>.<infix>.<metric_name>` to get the complete metric identifier when using Flink to access Paimon, `metric_name` can be got from [Metric List]({{< ref "maintenance/metrics#metrics-list" >}}).

For example, the identifier of metric `lastPartitionsWritten` for table `word_count` in Flink job named `insert_word_count` is:

`localhost.taskmanager.localhost:60340-775a20.insert_word_count.Global Committer : word_count.0.paimon.table.word_count.commit.lastPartitionsWritten`.

From Flink Web-UI, go to the committer operator's metrics, it's shown as:

`0.Global_Committer___word_count.paimon.table.word_count.commit.lastPartitionsWritten`.

{{< hint info >}}
1. Please refer to [System Scope](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/metrics/#system-scope) to understand Flink `scope`
2. Scan metrics are only supported by Flink versions >= 1.18
{{< /hint >}}

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 130pt"></th>
      <th class="text-left" style="width: 280pt">Scope</th>
      <th class="text-left" style="width: 250pt">Infix</th>
    </tr>
    </thead>
    <tbody>
        <tr>
            <td>Scan Metrics</td>
            <td>&lt;host&gt;.jobmanager.&lt;job_name&gt;</td>
            <td>&lt;source_operator_name&gt;.coordinator. enumerator.paimon.table.&lt;table_name&gt;.scan</td>
        </tr>
        <tr>
            <td>Commit Metrics</td>
            <td>&lt;host&gt;.taskmanager.&lt;tm_id&gt;.&lt;job_name&gt;.&lt;committer_operator_name&gt;.&lt;subtask_index&gt;</td>
            <td>paimon.table.&lt;table_name&gt;.commit</td>
        </tr>
        <tr>
            <td>Write Metrics</td>
            <td>&lt;host&gt;.taskmanager.&lt;tm_id&gt;.&lt;job_name&gt;.&lt;writer_operator_name&gt;.&lt;subtask_index&gt;</td>
            <td>paimon.table.&lt;table_name&gt;.partition.&lt;partition_string&gt;.bucket.&lt;bucket_index&gt;.writer</td>
        </tr>
        <tr>
            <td>Write Buffer Metrics</td>
            <td>&lt;host&gt;.taskmanager.&lt;tm_id&gt;.&lt;job_name&gt;.&lt;writer_operator_name&gt;.&lt;subtask_index&gt;</td>
            <td>paimon.table.&lt;table_name&gt;.writeBuffer</td>
        </tr>
        <tr>
            <td>Compaction Metrics</td>
            <td>&lt;host&gt;.taskmanager.&lt;tm_id&gt;.&lt;job_name&gt;.&lt;writer_operator_name&gt;.&lt;subtask_index&gt;</td>
            <td>paimon.table.&lt;table_name&gt;.partition.&lt;partition_string&gt;.bucket.&lt;bucket_index&gt;.compaction</td>
        </tr>
        <tr>
            <td>Flink Source Metrics</td>
            <td>&lt;host&gt;.taskmanager.&lt;tm_id&gt;.&lt;job_name&gt;.&lt;source_operator_name&gt;.&lt;subtask_index&gt;</td>
            <td>-</td>
        </tr>
        <tr>
            <td>Flink Sink Metrics</td>
            <td>&lt;host&gt;.taskmanager.&lt;tm_id&gt;.&lt;job_name&gt;.&lt;committer_operator_name&gt;.&lt;subtask_index&gt;</td>
            <td>-</td>
        </tr>  
    </tbody>
</table>

### Flink Connector Standard Metrics

When using Flink to read and write, Paimon has implemented some key standard Flink connector metrics to measure the source latency and output of sink, see [FLIP-33: Standardize Connector Metrics](https://cwiki.apache.org/confluence/display/FLINK/FLIP-33%3A+Standardize+Connector+Metrics). Flink source / sink metrics implemented are listed here.

#### Source Metrics (Flink)

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 225pt">Metrics Name</th>
      <th class="text-left" style="width: 65pt">Level</th>
      <th class="text-left" style="width: 70pt">Type</th>
      <th class="text-left" style="width: 300pt">Description</th>
    </tr>
    </thead>
    <tbody>
        <tr>
            <td>currentEmitEventTimeLag</td>
            <td>Flink Source Operator</td>
            <td>Gauge</td>
            <td>Time difference between sending the record out of source and file creation.</td>
        </tr>
        <tr>
            <td>currentFetchEventTimeLag</td>
            <td>Flink Source Operator</td>
            <td>Gauge</td>
            <td>Time difference between reading the data file and file creation.</td>
        </tr>
    </tbody>
</table>

{{< hint info >}}
Please note that if you specified `consumer-id` in your streaming query, the level of source metrics should turn into the reader operator, which is behind the `Monitor` operator.
{{< /hint >}}

#### Sink Metrics (Flink)

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 225pt">Metrics Name</th>
      <th class="text-left" style="width: 65pt">Level</th>
      <th class="text-left" style="width: 70pt">Type</th>
      <th class="text-left" style="width: 300pt">Description</th>
    </tr>
    </thead>
    <tbody>
        <tr>
            <td>numBytesOut</td>
            <td>Table</td>
            <td>Counter</td>
            <td>The total number of output bytes.</td>
        </tr>
        <tr>
            <td>numBytesOutPerSecond</td>
            <td>Table</td>
            <td>Meter</td>
            <td>The output bytes per second.</td>
        </tr>
        <tr>
            <td>numRecordsOut</td>
            <td>Table</td>
            <td>Counter</td>
            <td>The total number of output records.</td>
        </tr>
        <tr>
            <td>numRecordsOutPerSecond</td>
            <td>Table</td>
            <td>Meter</td>
            <td>The output records per second.</td>
        </tr>  
    </tbody>
</table>
