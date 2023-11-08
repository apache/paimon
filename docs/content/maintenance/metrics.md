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

# Metrics

Paimon has implemented some key standard flink connector metrics to measure the source latency and output of sink, see [FLIP-33: Standardize Connector Metrics](https://cwiki.apache.org/confluence/display/FLINK/FLIP-33%3A+Standardize+Connector+Metrics).
Besides, paimon also has metrics to measure operations of commits, scans and compactions, these metrics have been bridged to Flink's metrics system, which can be reported and managed by Flink.

## Read Metrics

When reading a paimon table, we care about the latency of reading files, and metrics about scan operation, like the duration, number of manifests scanned, how many files are skipped by data skipping, how many files are finally resulted. Below are metrics list for reading paimon tables.

{{< hint info >}}
NOTE: 
1. Please refer to [System Scope](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/metrics/#system-scope) to understand `scope`.
2. Join the <scope>.<infix>.<metric_name> to get the metric identifier.
{{< /hint >}}

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 5%">Scope</th>
      <th class="text-left" style="width: 20%">Infix</th>
      <th class="text-left" style="width: 10%">Metrics Name</th>
      <th class="text-left" style="width: 5%">Type</th>
      <th class="text-left" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>Source operator</h5></td>
            <td>(none)</td>
            <td>currentFetchEventTimeLag</td>
            <td>Gauge</td>
            <td>Time difference between reading the data file and file creation.</td>
        </tr>
        <tr>
            <td rowspan="8"><h5>Jobmanager-Job</h5></td>
            <td rowspan="8">${source_operator_name}.coordinator.enumerator.paimon.table.${table_name}.scan</td>
            <td>lastScanDuration</td>
            <td>Gauge</td>
            <td>The time it took to complete the last scan. Only Flink 1.18+ is supported.</td>
        </tr>
        <tr>
            <td>scanDuration</td>
            <td>Histogram</td>
            <td>Distributions of the time taken by the last few scans. Only Flink 1.18+ is supported.</td>
        </tr>
        <tr>
            <td>lastScannedManifests</td>
            <td>Gauge</td>
            <td>Number of scanned manifest files in the last scan. Only Flink 1.18+ is supported.</td>
        </tr>
        <tr>
            <td>lastSkippedByPartitionAndStats</td>
            <td>Gauge</td>
            <td>Skipped table files by partition filter and value / key stats information in the last scan. Only Flink 1.18+ is supported.</td>
        </tr>
        <tr>
            <td>lastSkippedByBucketAndLevelFilter</td>
            <td>Gauge</td>
            <td>Skipped table files by bucket, bucket key and level filter in the last scan. Only Flink 1.18+ is supported.</td>
        </tr>
        <tr>
            <td>lastSkippedByWholeBucketFilesFilter</td>
            <td>Gauge</td>
            <td>Skipped table files by bucket level value filter (only primary key table) in the last scan. Only Flink 1.18+ is supported.</td>
        </tr>
        <tr>
            <td>lastScanSkippedTableFiles</td>
            <td>Gauge</td>
            <td>Total skipped table files in the last scan. Only Flink 1.18+ is supported.</td>
        </tr>
        <tr>
            <td>lastScanResultedTableFiles</td>
            <td>Gauge</td>
            <td>Resulted table files in the last scan. Only Flink 1.18+ is supported.</td>
        </tr>
    </tbody>
</table>

## Write Metrics

When writing a paimon table, we care about the total output bytes, records, and metrics about commit, compaction operations. Like the commit duration, number of files and records committed, and compaction duration, number and size of files compacted. Below are metrics list for writing paimon tables.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 5%">Scope</th>
      <th class="text-left" style="width: 20%">Infix</th>
      <th class="text-left" style="width: 10%">Metrics Name</th>
      <th class="text-left" style="width: 5%">Type</th>
      <th class="text-left" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
        <tr>
            <td rowspan="20"><h5>Committer Operator</h5></td>
            <td rowspan="4">(none)</td>
            <td>numBytesOut</td>
            <td>Counter</td>
            <td>The total number of output bytes.</td>
        </tr>
        <tr>
            <td>numBytesOutPerSecond</td>
            <td>Meter</td>
            <td>The output bytes per second.</td>
        </tr>
        <tr>
            <td>numRecordsOut</td>
            <td>Counter</td>
            <td>The total number of output records.</td>
        </tr>
        <tr>
            <td>numRecordsOutPerSecond</td>
            <td>Meter</td>
            <td>The output records per second.</td>
        </tr>
        <tr>
            <td rowspan="16">paimon.table.${table_name}.commit</td>
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
        <tr>
            <td rowspan="8"><h5>Writer / Compactor operator</h5></td>
            <td rowspan="8">paimon.table.${table_name}.partition.${partition_string}.bucket.${bucket_index}.compaction</td>
            <td>lastCompactionDuration</td>
            <td>Gauge</td>
            <td>The time it took to complete the last compaction.</td>
        </tr>
        <tr>
            <td>compactionDuration</td>
            <td>Histogram</td>
            <td>Distributions of the time taken by the last few compaction.</td>
        </tr>
        <tr>
            <td>lastTableFilesCompactedBefore</td>
            <td>Gauge</td>
            <td>Number of deleted files in the last compaction.</td>
        </tr>
        <tr>
            <td>lastTableFilesCompactedAfter</td>
            <td>Gauge</td>
            <td>Number of added files in the last compaction.</td>
        </tr>
        <tr>
            <td>lastChangelogFilesCompacted</td>
            <td>Gauge</td>
            <td>Number of changelog files compacted in last compaction.</td>
        </tr>
        <tr>
            <td>lastRewriteInputFileSize</td>
            <td>Gauge</td>
            <td>Size of deleted files in the last compaction.</td>
        </tr>
        <tr>
            <td>lastRewriteOutputFileSize</td>
            <td>Gauge</td>
            <td>Size of added files in the last compaction.</td>
        </tr>
        <tr>
            <td>lastRewriteChangelogFileSize</td>
            <td>Gauge</td>
            <td>Size of changelog files compacted in last compaction.</td>
        </tr>
    </tbody>
</table>