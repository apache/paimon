---
title: "File Layouts"
weight: 3
type: docs
aliases:
- /concepts/file-layouts.html
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

# 文件布局

所有表的文件都存储在一个基础目录下。Paimon文件以分层样式组织。以下图片说明了文件布局。从快照文件开始，Paimon读者可以递归访问表中的所有记录。

{{< img src="/img/file-layout.png">}}

## 快照文件

所有快照文件都存储在`snapshot`目录中。

快照文件是一个包含有关此快照的信息的JSON文件，包括

* 正在使用的模式文件
* 包含此快照所有更改的清单列表

## 清单文件

所有清单列表和清单文件都存储在`manifest`目录中。

清单列表是清单文件名称的列表。

清单文件是包含有关LSM数据文件和更改日志文件的更改的文件。例如，在相应的快照中创建了哪个LSM数据文件以及哪个文件被删除。

## 数据文件

数据文件按分区和存储桶分组。每个存储桶目录包含一个[LSM树]({{< ref "concepts/file-layouts#lsm-trees" >}})及其[更改日志文件]({{< ref "concepts/primary-key-table/changelog-producer" >}})。

目前，Paimon支持将orc（默认）、parquet和avro用作数据文件的格式。

## LSM树

Paimon将LSM树（日志结构合并树）作为文件存储的数据结构。本文档简要介绍了有关LSM树的概念。

### 排序运行

LSM树将文件组织成几个排序运行。排序运行由一个或多个[数据文件]({{< ref "concepts/file-layouts#data-files" >}})组成，每个数据文件都属于一个排序运行。

数据文件内部按其主键排序。在排序运行内，数据文件的主键范围永远不会重叠。

{{< img src="/img/sorted-runs.png">}}

如您所见，不同的排序运行可能具有重叠的主键范围，甚至可能包含相同的主键。在查询LSM树时，必须组合所有排序运行，并根据用户指定的[合并引擎]({{< ref "concepts/primary-key-table/merge-engine" >}})和每个记录的时间戳合并所有具有相同主键的记录。

写入LSM树的新记录将首先缓存在内存中。当内存缓冲区已满时，内存中的所有记录将被排序并刷新到磁盘。现在创建了一个新的排序运行。

### 压实

当越来越多的记录写入LSM树时，排序运行的数量将增加。因为查询LSM树需要组合所有排序运行，太多的排序运行会导致查询性能不佳，甚至会导致内存不足。

为了限制排序运行的数量，我们必须不时地将几个排序运行合并为一个大的排序运行。这个过程称为压实。

然而，压实是一个资源密集型的过程，需要消耗一定数量的CPU时间和磁盘IO，因此过于频繁的压实可能会导致写入速度变慢。这是查询性能和写入性能之间的权衡。Paimon目前采用类似Rocksdb的[通用压实](https://github.com/facebook/rocksdb/wiki/Universal-Compaction)的压实策略。

默认情况下，当Paimon追加记录到LSM树时，它也会根据需要执行压实。用户还可以选择在专用的压实作业中执行所有压实。有关更多信息，请参阅[专用压实作业]({{< ref "maintenance/dedicated-compaction#dedicated-compaction-job" >}})