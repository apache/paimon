---
title: "Data Distribution"
weight: 2
type: docs
aliases:
- /concepts/primary-key-table/data-distribution.html
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

# 数据分布

{{< hint info >}} 默认情况下，派蒙表只有一个存储桶，这意味着它只提供单一并行读写。请根据您的表配置存储桶策略。 {{< /hint >}}

存储桶是读写的最小存储单元，每个存储桶目录包含一个[LSM树]({{< ref "concepts/file-layouts#lsm-trees" >}})。

## 固定存储桶

配置一个大于0的存储桶，使用固定存储桶模式，根据 `Math.abs(key_hashcode % numBuckets)` 计算记录的存储桶。

重新调整存储桶只能通过离线过程完成，参见[重新调整存储桶]({{< ref "/maintenance/rescale-bucket" >}})。太多的存储桶会导致太多的小文件，而太少的存储桶会导致写入性能差。

## 动态存储桶

配置 `'bucket' = '-1'`。首先到达的键将落入旧存储桶，而新键将落入新存储桶，存储桶和键的分布取决于数据到达的顺序。派蒙维护一个索引来确定哪个键对应哪个存储桶。

派蒙将自动扩展存储桶的数量。

* 选项1：`'dynamic-bucket.target-row-num'`：控制一个存储桶的目标行数。
* 选项2：`'dynamic-bucket.initial-buckets'`：控制初始化存储桶的数量。

{{< hint info >}} 动态存储桶仅支持单个写作业。请不要启动多个作业来写入同一分区（这可能导致重复数据）。即使启用了 `'write-only'` 并启动了专用的压缩作业，也不会起作用。 {{< /hint >}}

### 普通动态存储桶模式

当您的更新不跨分区时（没有分区，或主键包含所有分区字段），动态存储桶模式使用哈希索引来维护从键到存储桶的映射，它需要比固定存储桶模式更多的内存。

性能：

1. 一般来说，不会有性能损失，但会有一些额外的内存消耗，一个分区中的**1亿**条记录占用**1 GB**以上的内存，不再活动的分区不占用内存。
2. 对于更新速率较低的表格，建议使用此模式以显著提高性能。

`普通动态存储桶模式` 支持排序压缩以加速查询。请参阅[排序压缩]({{< ref "maintenance/dedicated-compaction#sort-compact" >}})。

### 跨分区Upsert动态存储桶模式

当您需要跨分区的Upsert（主键不包含所有分区字段）时，动态存储桶模式直接维护键到分区和存储桶的映射，使用本地磁盘，并在启动流写作业时通过读取表中的所有现有键来初始化索引。不同的合并引擎有不同的行为：

1. 去重：从旧分区删除数据并将新数据插入新分区。
2. 部分更新和聚合：将新数据插入旧分区。
3. 第一行：如果存在旧值，则忽略新数据。

性能：对于具有大量数据的表格，性能将显著下降。此外，初始化需要很长时间。

如果您的Upsert不依赖于过旧的数据，可以考虑配置索引TTL以减少索引和初始化时间：

* `'cross-partition-upsert.index-ttl'`：Rocksdb索引和初始化的TTL，这可以避免维护太多的索引，导致性能变得越来越差。

但请注意，这可能会导致数据重复。