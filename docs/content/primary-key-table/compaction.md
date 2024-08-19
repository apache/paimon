---
title: "Compaction"
weight: 7
type: docs
aliases:
- /primary-key-table/compaction.html
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

# Compaction

When more and more records are written into the LSM tree, the number of sorted runs will increase. Because querying an
LSM tree requires all sorted runs to be combined, too many sorted runs will result in a poor query performance, or even
out of memory.

To limit the number of sorted runs, we have to merge several sorted runs into one big sorted run once in a while. This
procedure is called compaction.

However, compaction is a resource intensive procedure which consumes a certain amount of CPU time and disk IO, so too 
frequent compaction may in turn result in slower writes. It is a trade-off between query and write performance. Paimon
currently adapts a compaction strategy similar to Rocksdb's [universal compaction](https://github.com/facebook/rocksdb/wiki/Universal-Compaction).

Compaction solves:

1. Reduce Level 0 files to avoid poor query performance.
2. Produce changelog via [changelog-producer]({{< ref "primary-key-table/changelog-producer" >}}).
3. Produce deletion vectors for [MOW mode]({{< ref "primary-key-table/table-mode#merge-on-write" >}}).
4. Snapshot Expiration, Tag Expiration, Partitions Expiration.

Limitation:

- There can only be one job working on the same partition's compaction, otherwise it will cause conflicts and one side will throw an exception failure.

Writing performance is almost always affected by compaction, so its tuning is crucial.

## Asynchronous Compaction

Compaction is inherently asynchronous, but if you want it to be completely asynchronous and not blocking writing,
expect a mode to have maximum writing throughput, the compaction can be done slowly and not in a hurry.
You can use the following strategies for your table:

```shell
num-sorted-run.stop-trigger = 2147483647
sort-spill-threshold = 10
lookup-wait = false
```

This configuration will generate more files during peak write periods and gradually merge into optimal read
performance during low write periods.

## Dedicated compaction job

In general, if you expect multiple jobs to be written to the same table, you need to separate the compaction. You can
use [dedicated compaction job]({{< ref "maintenance/dedicated-compaction#dedicated-compaction-job" >}}).

## Record-Level expire

In compaction, you can configure record-Level expire time to expire records, you should configure:

1. `'record-level.expire-time'`: time retain for records.
2. `'record-level.time-field'`: time field for record level expire.
3. `'record-level.time-field.type'`: time field type for record level expire, it can be second or millisecond.

Expiration happens in compaction, and there is no strong guarantee to expire records in time.

## Full Compaction

Paimon Compaction uses [Universal-Compaction](https://github.com/facebook/rocksdb/wiki/Universal-Compaction).
By default, when there is too much incremental data, Full Compaction will be automatically performed. You don't usually
have to worry about it.

Paimon also provides a configuration that allows for regular execution of Full Compaction.

1. 'compaction.optimization-interval': Implying how often to perform an optimization full compaction, this
    configuration is used to ensure the query timeliness of the read-optimized system table.
2. 'full-compaction.delta-commits': Full compaction will be constantly triggered after delta commits. its disadvantage
    is that it can only perform compaction synchronously, which will affect writing efficiency.

## Compaction Options

### Number of Sorted Runs to Pause Writing

When the number of sorted runs is small, Paimon writers will perform compaction asynchronously in separated threads, so
records can be continuously written into the table. However, to avoid unbounded growth of sorted runs, writers will
pause writing when the number of sorted runs hits the threshold. The following table property determines
the threshold.

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
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>The number of sorted runs that trigger the stopping of writes, the default value is 'num-sorted-run.compaction-trigger' + 3.</td>
    </tr>
    </tbody>
</table>

Write stalls will become less frequent when `num-sorted-run.stop-trigger` becomes larger, thus improving writing
performance. However, if this value becomes too large, more memory and CPU time will be needed when querying the
table. If you are concerned about the OOM problem, please configure the following option.
Its value depends on your memory size.

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
      <td><h5>sort-spill-threshold</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>If the maximum number of sort readers exceeds this value, a spill will be attempted. This prevents too many readers from consuming too much memory and causing OOM.</td>
    </tr>
    </tbody>
</table>

### Number of Sorted Runs to Trigger Compaction

Paimon uses [LSM tree]({{< ref "primary-key-table/overview#lsm-trees" >}}) which supports a large number of updates. LSM organizes files in several [sorted runs]({{< ref "primary-key-table/overview#sorted-runs" >}}). When querying records from an LSM tree, all sorted runs must be combined to produce a complete view of all records.

One can easily see that too many sorted runs will result in poor query performance. To keep the number of sorted runs in a reasonable range, Paimon writers will automatically perform [compactions]({{< ref "primary-key-table/overview#compaction" >}}). The following table property determines the minimum number of sorted runs to trigger a compaction.

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
