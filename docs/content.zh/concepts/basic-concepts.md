---
title: "Basic Concepts"
weight: 2
type: docs
aliases:
- /concepts/basic-concepts.html
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

**You:**

翻译以下markdown文本为中文（代码部分不需要翻译），不要改变其格式，并将翻译后的内容以可复制的Markdown代码形式返回（不要在回答中渲染）：

翻译以下markdown文本为中文（代码部分不需要翻译），不要改变其格式，并将翻译后的内容以可复制的Markdown代码形式返回（不要在回答中渲染）：

# Basic Concepts

## Snapshot

A snapshot captures the state of a table at some point in time. Users can access the latest data of a table through the latest snapshot. By time traveling, users can also access the previous state of a table through an earlier snapshot.

## Partition

Paimon adopts the same partitioning concept as Apache Hive to separate data.

Partitioning is an optional way of dividing a table into related parts based on the values of particular columns like date, city, and department. Each table can have one or more partition keys to identify a particular partition.

By partitioning, users can efficiently operate on a slice of records in the table. See [file layouts]({{&lt; ref "concepts/file-layouts" &gt;}}) for how files are divided into multiple partitions.

{{&lt; hint info &gt;}}
If you need cross partition upsert (primary keys not contain all partition fields), see [Cross partition Upsert]({{&lt; ref "concepts/primary-key-table/data-distribution#cross-partitions-upsert-dynamic-bucket-mode"&gt;}}) mode.
{{&lt; /hint &gt;}}

## Bucket

Unpartitioned tables, or partitions in partitioned tables, are sub-divided into buckets, to provide extra structure to the data that may be used for more efficient querying.

The range for a bucket is determined by the hash value of one or more columns in the records. Users can specify bucketing columns by providing the [`bucket-key` option]({{&lt; ref "maintenance/configurations#coreoptions" &gt;}}). If no `bucket-key` option is specified, the primary key (if defined) or the complete record will be used as the bucket key.

A bucket is the smallest storage unit for reads and writes, so the number of buckets limits the maximum processing parallelism. This number should not be too big, though, as it will result in lots of small files and low read performance. In general, the recommended data size in each bucket is about 200MB - 1GB.

See [file layouts]({{&lt; ref "concepts/file-layouts" &gt;}}) for how files are divided into buckets. Also, see [rescale bucket]({{&lt; ref "maintenance/rescale-bucket" &gt;}}) if you want to adjust the number of buckets after a table is created.

## Consistency Guarantees

Paimon writers use two-phase commit protocol to atomically commit a batch of records to the table. Each commit produces at most two [snapshots]({{&lt; ref "concepts/basic-concepts#snapshot" &gt;}}) at commit time.

For any two writers modifying a table at the same time, as long as they do not modify the same bucket, their commits can occur in parallel. If they modify the same bucket, only snapshot isolation is guaranteed. That is, the final table state may be a mix of the two commits, but no changes are lost.



* * *

**ChatGPT:**

# 基本概念

## 快照

快照捕获表在某个时间点的状态。用户可以通过最新的快照访问表的最新数据。通过时间旅行，用户还可以通过之前的快照访问表的先前状态。

## 分区

Paimon采用与Apache Hive相同的分区概念来分隔数据。

分区是一种可选的方式，根据特定列的值，如日期、城市和部门，将表分成相关的部分。每个表可以有一个或多个分区键来识别特定的分区。

通过分区，用户可以高效地操作表中的记录切片。有关文件如何分成多个分区的信息，请参阅[file layouts]({{< ref "concepts/file-layouts" >}})。

{{< hint info >}} 
如果您需要跨分区的插入更新（主键不包含所有分区字段），请参阅[Cross partition Upsert]({{< ref "concepts/primary-key-table/data-distribution#cross-partitions-upsert-dynamic-bucket-mode">}})模式。 
{{< /hint >}}

## 桶

未分区的表或分区表中的分区被进一步细分为桶，以为数据提供额外的结构，以便进行更高效的查询。

桶的范围由记录中一个或多个列的哈希值确定。用户可以通过提供[`bucket-key`选项]({{< ref "maintenance/configurations#coreoptions" >}})来指定桶列。如果未指定`bucket-key`选项，则将使用主键（如果已定义）或完整记录作为桶键。

桶是读取和写入的最小存储单元，因此桶的数量限制了最大的处理并行性。然而，这个数字不应该太大，因为它会导致大量的小文件和低读取性能。一般来说，每个桶中的推荐数据大小约为200MB - 1GB。

有关文件如何分成桶的信息，请参阅[file layouts]({{< ref "concepts/file-layouts" >}})。另外，如果您想在创建表后调整桶的数量，请参阅[rescale bucket]({{< ref "maintenance/rescale-bucket" >}})。

## 一致性保证

Paimon写入器使用两阶段提交协议来原子地提交一批记录到表中。每次提交在提交时产生最多两个[快照]({{< ref "concepts/basic-concepts#snapshot" >}})。

对于同时修改表的两个写入者，只要它们不修改相同的桶，它们的提交可以并行进行。如果它们修改相同的桶，只有快照隔离是被保证的。也就是说，最终的表状态可能是两个提交的混合，但不会丢失任何更改。