---
title: "Write Performance"
weight: 2
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

Paimon's write performance is closely related to checkpoint, so if you need greater write throughput:

1. Flink Configuration (`'flink-conf.yaml'` or `SET` in SQL): Increase the checkpoint interval
   (`'execution.checkpointing.interval'`), increase max concurrent checkpoints to 3
   (`'execution.checkpointing.max-concurrent-checkpoints'`), or just use batch mode.
2. Increase `write-buffer-size`.
3. Enable `write-buffer-spillable`.
4. Rescale bucket number if you are using Fixed-Bucket mode.

Option `'changelog-producer' = 'lookup' or 'full-compaction'`, and option `'full-compaction.delta-commits'` have a
large impact on write performance, if it is a snapshot / full synchronization phase you can unset these options and
then enable them again in the incremental phase.

If you find that the input of the job shows a jagged pattern in the case of backpressure, it may be imbalanced work
nodes. You can consider turning on [Asynchronous Compaction]({{< ref "#asynchronous-compaction" >}}) to observe if the
throughput is increased.

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

### Asynchronous Compaction

Compaction is inherently asynchronous, but if you want it to be completely asynchronous and not blocking writing,
expect a mode to have maximum writing throughput, the compaction can be done slowly and not in a hurry.
You can use the following strategies for your table:

```shell
num-sorted-run.stop-trigger = 2147483647
sort-spill-threshold = 10
changelog-producer.lookup-wait = false
```

This configuration will generate more files during peak write periods and gradually merge into optimal read
performance during low write periods.

In the case of `'changelog-producer' = 'lookup'`, by default, the lookup will be completed at checkpointing, which
will block the checkpoint. So if you want an asynchronous lookup, you should also set `'changelog-producer.lookup-wait' = 'false'`.

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

## Local Merging

If your job suffers from primary key data skew
(for example, you want to count the number of views for each page in a website,
and some particular pages are very popular among the users),
you can set `'local-merge-buffer-size'` so that input records will be buffered and merged
before they're shuffled by bucket and written into sink.
This is particularly useful when the same primary key is updated frequently between snapshots.

The buffer will be flushed when it is full. We recommend starting with `64 mb`
when you are faced with data skew but don't know where to start adjusting buffer size.

(Currently, Local merging not works for CDC ingestion)

## File Format

If you want to achieve ultimate compaction performance, you can consider using row storage file format AVRO.
- The advantage is that you can achieve high write throughput and compaction performance.
- The disadvantage is that your analysis queries will be slow, and the biggest problem with row storage is that it
  does not have the query projection. For example, if the table have 100 columns but only query a few columns, the
  IO of row storage cannot be ignored. Additionally, compression efficiency will decrease and storage costs will
  increase.

This a tradeoff.

Enable row storage through the following options:
```shell
file.format = avro
metadata.stats-mode = none
```

The collection of statistical information for row storage is a bit expensive, so I suggest turning off statistical
information as well.

If you don't want to modify all files to Avro format, at least you can consider modifying the files in the previous
layers to Avro format. You can use `'file.format.per.level' = '0:avro,1:avro'` to specify the files in the first two
layers to be in Avro format.

## File Compression

By default, Paimon uses zstd with level 1, you can modify the compression algorithm:

`'file.compression.zstd-level'`: Default zstd level is 1. For higher compression rates, it can be configured to 9, but the read and write speed will significantly decrease.

## Stability

If there are too few buckets or resources, full-compaction may cause the checkpoint timeout, Flink's default
checkpoint timeout is 10 minutes.

If you expect stability even in this case, you can turn up the checkpoint timeout, for example:

```shell
execution.checkpointing.timeout = 60 min
```

## Write Initialize

In the initialization of write, the writer of the bucket needs to read all historical files. If there is a bottleneck
here (For example, writing a large number of partitions simultaneously), you can use `write-manifest-cache` to cache
the read manifest data to accelerate initialization.

## Write Memory

There are three main places in Paimon writer that takes up memory:

* Writer's memory buffer, shared and preempted by all writers of a single task. This memory value can be adjusted by the `write-buffer-size` table property.
* Memory consumed when merging several sorted runs for compaction. Can be adjusted by the `num-sorted-run.compaction-trigger` option to change the number of sorted runs to be merged.
* If the row is very large, reading too many lines of data at once will consume a lot of memory when making a compaction. Reducing the `read.batch-size` option can alleviate the impact of this case.
* The memory consumed by writing columnar ORC file. Decreasing the `orc.write.batch-size` option can reduce the consumption of memory for ORC format.
* If files are automatically compaction in the write task, dictionaries for certain large columns can significantly consume memory during compaction.
  * To disable dictionary encoding for all fields in Parquet format, set `'parquet.enable.dictionary'= 'false'`.
  * To disable dictionary encoding for all fields in ORC format, set `orc.dictionary.key.threshold='0'`. Additionally,set `orc.column.encoding.direct='field1,field2'` to disable dictionary encoding for specific columns.

If your Flink job does not rely on state, please avoid using managed memory, which you can control with the following Flink parameter:
```shell
taskmanager.memory.managed.size=1m
```
Or you can use Flink managed memory for your write buffer to avoid OOM, set table property:
```shell
sink.use-managed-memory-allocator=true
```

## Commit Memory

Committer node may use a large memory if the amount of data written to the table is particularly large, OOM may occur
if the memory is too small. In this case, you need to increase the Committer heap memory, but you may not want to
increase the memory of Flink's TaskManager uniformly, which may lead to a waste of memory.

You can use fine-grained-resource-management of Flink to increase committer heap memory only:
1. Configure Flink Configuration `cluster.fine-grained-resource-management.enabled: true`. (This is default after Flink 1.18)
2. Configure Paimon Table Options: `sink.committer-memory`, for example 300 MB, depends on your `TaskManager`.
   (`sink.committer-cpu` is also supported)
