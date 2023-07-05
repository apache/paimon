---
title: "Manage Files"
weight: 6
type: docs
aliases:
- /maintenance/manage-files.html
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

# Manage Small Files

Many users are concerned about small files, which can lead to:
1. Stability issue: Too many small files in HDFS, NameNode will be overstressed.
2. Cost issue: A small file in HDFS will temporarily use the size of a minimum of one Block, for example 128 MB.
3. Query efficiency: The efficiency of querying too many small files will be affected.

## Understand Checkpoints

Assuming you are using Flink Writer, each checkpoint generates 1-2 snapshots, and the checkpoint forces the files to be
generated on DFS, so the smaller the checkpoint interval the more small files will be generated.

1. So first thing is decrease checkpoint interval.

By default, not only checkpoint will cause the file to be generated, but writer's memory (write-buffer-size) exhaustion
will also flush data to DFS and generate the corresponding file. You can enable `write-buffer-spillable` to generate
spilled files in writer to generate bigger files in DFS.

2. So second thing is increase `write-buffer-size` or enable `write-buffer-spillable`.

## Understand Snapshots

Before delving further into this section, please ensure that you have read [File Operations]({{< ref "概念/文件操作" >}}).

{{< img src="/img/file-operations-3.png">}}

Paimon maintains multiple versions of files, compaction and deletion of files are logical and do not actually
delete files. Files are only really deleted when Snapshot is expired, so the first way to reduce files is to
reduce the time it takes for snapshot to be expired. Flink writer will automatically expire snapshots.

See [Expire Snapshots]({{< ref "/maintenance/manage-snapshots#expire-snapshots" >}}).

## Understand Partitions and Buckets

Paimon files are organized in a layered style. The following image illustrates the file layout. Starting
from a snapshot file, Paimon readers can recursively access all records from the table.

{{< img src="/img/file-layout.png">}}

For example, the following table:

```sql
CREATE TABLE MyTable (
    user_id BIGINT,
    item_id BIGINT,
    behavior STRING,
    dt STRING,
    hh STRING,
    PRIMARY KEY (dt, hh, user_id) NOT ENFORCED
) PARTITIONED BY (dt, hh) WITH (
    'bucket' = '10'
);
```

The table data will be physically sliced into different partitions, and different buckets inside, so if the overall
data volume is too small, there is at least one file in a single bucket, I suggest you configure a smaller number
of buckets, otherwise there will be quite a few small files as well.

## Understand LSM for Primary Table

LSM tree organizes files into several sorted runs. A sorted run consists of one or multiple
[data files]({{< ref "概念/文件布局#data-files" >}}) and each data file belongs to exactly one sorted run.

{{< img src="/img/sorted-runs.png">}}

By default, sorted runs number depends on `num-sorted-run.compaction-trigger`, see [Compaction for Primary Key Table]({{< ref "/maintenance/write-performance#compaction" >}}),
this means that there are at least 5 files in a bucket. If you want to reduce this number, you can keep fewer files, but write performance may suffer.

## Understand Files for Append-Only Table

By default, Append-Only also does automatic compaction to reduce the number of small files.

However, for Bucket's Append-only table, it will only compact the files within the Bucket for sequential
purposes, which may keep more small files. See [Compaction for Append-Only Table]({{< ref "/概念/append-only-table#compaction" >}}).

## Understand Full-Compaction

Maybe you think the 5 files for the primary key table are actually okay, but the Append-Only table (bucket)
may have 50 small files in a single bucket, which is very difficult to accept. Worse still, partitions that
are no longer active also keep so many small files.

It is recommended that you configure [Full-Compaction]({{< ref "/maintenance/read-performance#full-compaction" >}}),
configure ‘full-compaction.delta-commits’ perform full-compaction periodically in Flink writing. And it can ensure
that partitions are full compacted before writing ends.
