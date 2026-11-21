---
title: "Metrics"
sidebar_position: 9
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

Paimon has supported built-in metrics to measure operations of **commits**, **scans**, **writes**, **compactions** and **lookups**, which can be bridged to any computing engine that supports, like Flink, Spark etc.

## Bridging To Flink

Paimon has implemented bridging metrics to Flink's metrics system, which can be reported by Flink, and the lifecycle of metric groups are managed by Flink.

Please join the `<scope>.<infix>.<metric_name>` to get the complete metric identifier when using Flink to access Paimon, `metric_name` can be got from [Metric List](./metrics#metrics-list).

For example, the identifier of metric `lastPartitionsWritten` for table `word_count` in Flink job named `insert_word_count` is:

`localhost.taskmanager.localhost:60340-775a20.insert_word_count.Global Committer : word_count.0.paimon.table.word_count.commit.lastPartitionsWritten`.

From Flink Web-UI, go to the committer operator's metrics, it's shown as:

`0.Global_Committer___word_count.paimon.table.word_count.commit.lastPartitionsWritten`.

:::info

1. Please refer to [System Scope](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/metrics/#system-scope) to understand Flink `scope`
2. Scan metrics are only supported by Flink versions >= 1.18

:::

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
            <td>Lookup Metrics</td>
            <td>&lt;host&gt;.taskmanager.&lt;tm_id&gt;.&lt;job_name&gt;.&lt;lookup_operator_name&gt;.&lt;subtask_index&gt;</td>
            <td>paimon.table.&lt;table_name&gt;.lookup</td>
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
            <td>Blob Fetch Metrics</td>
            <td>&lt;host&gt;.taskmanager.&lt;tm_id&gt;.&lt;job_name&gt;.&lt;writer_operator_name&gt;.&lt;subtask_index&gt;</td>
            <td>paimon.table.&lt;table_name&gt;.blobFetch</td>
        </tr>
        <tr>
            <td>Native Blob Ingest Metrics</td>
            <td>&lt;host&gt;.taskmanager.&lt;tm_id&gt;.&lt;job_name&gt;.&lt;operator_name&gt;.&lt;subtask_index&gt;</td>
            <td>paimon.table.&lt;table_name&gt;.blobIngest</td>
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

## Choose Metrics for a Task

Use these groups to identify the operation to investigate. For tuning options, follow the related guide.

| Task | Metrics to inspect | Related guide |
| --- | --- | --- |
| Investigate slow commits | [Commit duration and attempts](#commit-metrics) | [Commit Memory](./write-performance#commit-memory) |
| Investigate writer memory pressure | [Write buffer usage](#write-buffer-metrics) | [Write Memory](./write-performance#write-memory) |
| Check whether compaction is keeping up | [Level 0 file counts and compaction activity](#compaction-metrics) | [Compaction](../primary-key-table/compaction) and [Dedicated Compaction](./dedicated-compaction) |
| Track Flink source latency or sink throughput | [Flink Connector Standard Metrics](#flink-connector-standard-metrics) | [Write Performance](./write-performance) |

## Metrics List

Below is lists of Paimon built-in metrics. They are summarized into types of scan metrics, commit metrics, write metrics, write buffer metrics, blob fetch metrics, compaction metrics and lookup metrics.

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
            <td>lastScannedSnapshotId</td>
            <td>Gauge</td>
            <td>The snapshot ID scanned in the last scan. 0 if no scan has occurred.</td>
        </tr>
        <tr>
            <td>lastScannedManifests</td>
            <td>Gauge</td>
            <td>Number of scanned manifest files in the last scan.</td>
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

### Lookup Metrics

Lookup metrics are available for local partial lookup. They are reported at lookup invocation granularity. <code>partialLookupRemoteAccessCount</code> counts a lookup invocation once when at least one lookup file must be created from table storage because it is not available in the local cache. It counts lookup invocations, not individual OSS or table-storage I/O operations.

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
            <td>partialLookupCount</td>
            <td>Counter</td>
            <td>Total number of partial lookup invocations.</td>
        </tr>
        <tr>
            <td>partialLookupRemoteAccessCount</td>
            <td>Counter</td>
            <td>Number of partial lookup invocations that created at least one lookup file from table storage because it was not available in the local cache. This is not an individual I/O count.</td>
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
        <tr>
            <td>lastCompactionInputFileSize</td>
            <td>Gauge</td>
            <td>Total size of the input files for the last compaction.</td>
        </tr>
        <tr>
            <td>lastCompactionOutputFileSize</td>
            <td>Gauge</td>
            <td>Total size of the output files for the last compaction.</td>
        </tr>
        <tr>
            <td>lastCommittedSnapshotId</td>
            <td>Gauge</td>
            <td>The snapshot ID created by the last commit. -1 if no commit has occurred.</td>
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

### Blob Fetch Metrics

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
            <td>blobFetchTotal</td>
            <td>Counter</td>
            <td>Total number of blob fetch outcomes handled during write, including successful fetches, failures, and missing resources written as NULL.</td>
        </tr>
        <tr>
            <td>blobFetchSuccess</td>
            <td>Counter</td>
            <td>Total number of blob fetches that completed successfully.</td>
        </tr>
        <tr>
            <td>blobFetchSuccessBytes</td>
            <td>Counter</td>
            <td>Total bytes written by successful blob fetches.</td>
        </tr>
        <tr>
            <td>blobFetchNullWritten</td>
            <td>Counter</td>
            <td>Total number of NULL values written because a blob resource was missing or failed to fetch.</td>
        </tr>
        <tr>
            <td>blobFetchMissingFileNullWritten</td>
            <td>Counter</td>
            <td>Total number of NULL values written because <code>blob-write-null-on-missing-file</code> handled a missing blob resource.</td>
        </tr>
        <tr>
            <td>blobFetchFailureNullWritten</td>
            <td>Counter</td>
            <td>Total number of NULL values written because <code>blob-write-null-on-fetch-failure</code> handled a non-404 blob fetch failure.</td>
        </tr>
        <tr>
            <td>blobFetchFailure</td>
            <td>Counter</td>
            <td>Total number of blob fetch failures that were not written as NULL.</td>
        </tr>
        <tr>
            <td>blobFetchHttpNotFound</td>
            <td>Counter</td>
            <td>Total number of HTTP 404 blob fetch failures.</td>
        </tr>
        <tr>
            <td>blobFetchHttpClientError</td>
            <td>Counter</td>
            <td>Total number of non-404 HTTP 4xx blob fetch failures.</td>
        </tr>
        <tr>
            <td>blobFetchHttpServerError</td>
            <td>Counter</td>
            <td>Total number of HTTP 5xx blob fetch failures.</td>
        </tr>
        <tr>
            <td>blobFetchHttpOtherError</td>
            <td>Counter</td>
            <td>Total number of blob fetch failures with HTTP status codes outside 4xx and 5xx.</td>
        </tr>
        <tr>
            <td>blobFetchInvalidUri</td>
            <td>Counter</td>
            <td>Total number of blob fetch failures caused by invalid URIs.</td>
        </tr>
        <tr>
            <td>blobFetchOtherError</td>
            <td>Counter</td>
            <td>Total number of blob fetch failures that are not classified as HTTP status or invalid URI failures.</td>
        </tr>
    </tbody>
</table>

### Native Blob Ingest and Snapshot Publication

Set the table option `blob-ingest-observation.enabled` to `true` to opt into the
`blobIngest` group. It defaults to false, requires `data-evolution.enabled=true`,
and requires a `MetricRegistry` on the native writer and committer. Missing or
invalid observation option values leave observation disabled. Recreate both
writer and committer when changing the option. Disabling the option does not
change the existing `blobFetch` counters.

The table factory configures `TableWriteImpl` and `TableCommitImpl`. The Flink
sink's `StoreSinkWriteImpl` and `StoreCommitter` already supply `FlinkMetricRegistry`;
no additional monitoring service is needed. The registry adds the `paimon`,
`table=<table name>`, `blobIngest` groups. Reporter-specific prefixes and naming
must be verified in the deployed operator. No source/target IDC or network QoS
labels are supplied here.

All metrics below are nonnegative cumulative counters, named `stage_outcome`.
Reset handling must precede aggregation when the exporter preserves producer identity
and lifecycle. The registry bridge reuses the existing engine adapters and does not
add writer identity. The Flink bridge has a native registry test, but this does not
establish deployed reporter coverage or consistent names across all engines. In
particular, same-JVM, same-table Spark writers can share or replace a registry key;
these counters do not establish a concurrent-writer total or per-writer reset-aware
rates for that backend. Check the actual registry and exporter lifecycle before
aggregating multiple writers.

| Stage | Outcomes | Unit and boundary |
| --- | --- | --- |
| `blob_source` | `http`, `inline`, `file`, `custom`, `unknown` | Number of logical reads, classified by actual reader API. |
| `blob_read` | `started`, `completed`, `failed`, `downgraded_null` | One started logical read has one terminal outcome in the synchronous native path. |
| `blob_read_elapsed_nanos` | `completed`, `failed`, `downgraded_null` | Cumulative elapsed ns; divide by the matching terminal counter for a mean. |
| `blob_skipped` | `null`, `premarked_missing`, `placeholder` | Skipped payloads; no HTTP request is invented. |
| `blob_failure` | `source_get`, `source_prepare`, `source_open`, `source_read`, `source_close`, `destination_write`, `record_finish`, `flush` | Failure boundary, independent from the received HTTP response. |
| `http_attempt` | `initial_get`, `range_resume`, `full_replay`, `decoded_retry`, `head_probe`, `range_probe` | Actual calls entering the Paimon HTTP client, not hidden client-internal retries. |
| `http_response` | request kind followed by HTTP status or `no_response` | Actual response at the same client boundary; a 200 body may subsequently fail. |
| `http_retry` | `range_resume`, `full_replay`, `decoded_retry` | Actual subsequent Paimon-visible recovery calls. |
| `payload_read`, `payload_written` | `observed`, `completed`, `incomplete` | Bytes returned by source reads / successful destination writes; completed and incomplete partition observed bytes. |
| `payload_write_unconfirmed` | `requested` | Requested bytes of a throwing write; actual destination change remains unknown. |
| `file_close`, `file_close_elapsed_nanos` | `succeeded`, `failed` | Actual Blob file close count and elapsed ns, distinct from publishing a snapshot. |
| `file_prepared`, `file_prepared_elapsed_nanos` | `succeeded` | Blob files returned in prepareCommit messages and cumulative prepare elapsed ns. |
| `commit_publish` | `attempt`, `committed`, `rejected`, `unknown`, `reconciled` | Calls to the bound atomic SnapshotCommit API, response outcomes, and later identity-matched publication evidence. |
| `commit`, `commit_elapsed_nanos` | `committed`, `no_op`, `unknown`, `failed` | Native logical commit outcomes and cumulative ns; filtered replay is no_op without a new publication. |

`blob_read_elapsed_nanos` measures the logical operation for one non-null Blob,
not just its payload-copy loop. It includes source lookup/preparation/open/read,
the destination payload writes and the per-Blob descriptor callback. For a raw
BLOB it also includes that record's footer and any callback-requested flush. For
ARRAY/MAP elements the scope ends before the containing record's index/footer
and final flush; failures there do not retroactively change completed element
observations. A completed Blob therefore does not prove that the containing
record/file has closed, that prepare has succeeded, or that a snapshot committed.
Use the separate file, prepare, logical-commit and publication outcomes for those
boundaries; do not treat `completed` as `commit_publish_committed`.

Observation preserves `SnapshotCommit`'s existing bound lock and atomic commit
semantics. A publication exception records unknown even if a snapshot may have
been published; a later
retry only records reconciliation after matching commit user, identifier and kind.
A replay filtered before publication is not a second publication. Independent
snapshot-pinned readback belongs to acceptance tooling and is not a live metric.

Payload counters measure application reads/writes, including buffering, and exclude
file framing, protocol overhead and physical NIC bytes. The `completed`/`incomplete`
subsets follow the logical Blob outcome: a full source body can still be classified
as incomplete when a later destination write or callback fails. Observed and its
subsets must not be added together. Reads and writes are different hops, not unique
payload.
The current scope is the native Data Evolution append path; external SDK retries,
other writer implementations and network topology need their own evidence.
Observation adds no business HEAD/GET or commit. Reporter runtime failures are
best effort and do not replace the original data-processing outcome.

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
            <td>The maximum number of level 0 files currently handled by this task. This value will become larger if asynchronous compaction cannot be done in time.</td>
        </tr>
        <tr>
            <td>avgLevel0FileCount</td>
            <td>Gauge</td>
            <td>The average number of level 0 files currently handled by this task. This value will become larger if asynchronous compaction cannot be done in time.</td>
        </tr>
        <tr>
            <td>compactionThreadBusy</td>
            <td>Gauge</td>
            <td>The maximum business of compaction threads in this task. Currently, there is only one compaction thread in each parallelism, so value of business ranges from 0 (idle) to 100 (compaction running all the time).</td>
        </tr>
        <tr>
            <td>avgCompactionTime</td>
            <td>Gauge</td>
            <td>The average runtime of compaction threads, calculated based on recorded compaction time data in milliseconds. The value represents the average duration of compaction operations. Higher values indicate longer average compaction times, which may suggest the need for performance optimization.</td>
        </tr>
       <tr>
            <td>compactionCompletedCount</td>
            <td>Counter</td>
            <td>The total number of compactions that have completed.</td>
        </tr>
        <tr>
            <td>compactionQueuedCount</td>
            <td>Counter</td>
            <td>The total number of compactions that are queued/running.</td>
        </tr>
        <tr>
            <td>compactionTotalCount</td>
            <td>Counter</td>
            <td>The total number of compactions.</td>
        </tr>
        <tr>
            <td>maxCompactionInputSize</td>
            <td>Gauge</td>
            <td>The maximum input file size for this task's compaction.</td>
        </tr>
        <tr>
            <td>avgCompactionInputSize</td>
            <td>Gauge</td>
            <td>The average input file size for this task's compaction.</td>
        </tr>
        <tr>
            <td>maxCompactionOutputSize</td>
            <td>Gauge</td>
            <td>The maximum output file size for this task's compaction.</td>
        </tr>
        <tr>
            <td>avgCompactionOutputSize</td>
            <td>Gauge</td>
            <td>The average output file size for this task's compaction.</td>
        </tr>
        <tr>
            <td>maxTotalFileSize</td>
            <td>Gauge</td>
            <td>The maximum total file size of an active (currently being written) bucket.</td>
        </tr>
        <tr>
            <td>avgTotalFileSize</td>
            <td>Gauge</td>
            <td>The average total file size of all active (currently being written) buckets.</td>
        </tr>
        <tr>
            <td>maxTotalFileCount</td>
            <td>Gauge</td>
            <td>The maximum total file count of an active (currently being written) bucket.</td>
        </tr>
        <tr>
            <td>avgTotalFileCount</td>
            <td>Gauge</td>
            <td>The average total file count of all active (currently being written) buckets.</td>
        </tr>
        <tr>
            <td>minAvgFileSize</td>
            <td>Gauge</td>
            <td>The minimum average file size across all active buckets, computed as total file size divided by total file count per bucket. Directly indicates if any bucket has a small file problem. Only reported for primary-key tables.</td>
        </tr>
        <tr>
            <td>maxSortBufferUsedBytes</td>
            <td>Gauge</td>
            <td>The maximum sort buffer memory currently used across all active compaction buckets, in bytes. High values relative to <code>maxSortBufferTotalBytes</code> indicate memory pressure during compaction; consider lowering <code>sort-spill-threshold</code> or reducing <code>sort-spill-buffer-size</code>.</td>
        </tr>
        <tr>
            <td>avgSortBufferUsedBytes</td>
            <td>Gauge</td>
            <td>The average sort buffer memory used across all active compaction buckets, in bytes.</td>
        </tr>
        <tr>
            <td>maxSortBufferUtilisationPercent</td>
            <td>Gauge</td>
            <td>The maximum sort buffer utilisation percentage (0–100) across all active compaction buckets. A value consistently near 100 indicates the sort buffer pool is exhausted and spilling to disk is occurring or imminent.</td>
        </tr>
        <tr>
            <td>avgSortBufferUtilisationPercent</td>
            <td>Gauge</td>
            <td>The average sort buffer utilisation percentage across all active compaction buckets.</td>
        </tr>
    </tbody>
</table>

## Flink Connector Standard Metrics

When using Flink to read and write, Paimon has implemented some key standard Flink connector metrics to measure the source latency and output of sink, see [FLIP-33: Standardize Connector Metrics](https://cwiki.apache.org/confluence/display/FLINK/FLIP-33%3A+Standardize+Connector+Metrics). Flink source / sink metrics implemented are listed here.

### Source Metrics (Flink)

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
        <tr>
            <td>sourceParallelismUpperBound</td>
            <td>Flink Source Enumerator</td>
            <td>Gauge</td>
            <td>Recommended upper bound of parallelism for auto-scaling systems. Note: This is a recommendation, not a hard limit.</td>
        </tr>
    </tbody>
</table>

:::info

Please note that if you specified `consumer-id` in your streaming query, the level of source metrics should turn into the reader operator, which is behind the `Monitor` operator.

:::

### Sink Metrics (Flink)

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
