---
title: "Overview"
weight: 1
type: docs
aliases:
- /concepts/overview.html
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

# 概述

Apache Paimon(incubating) 是一个支持高速数据摄取、变更数据跟踪和高效实时分析的流式数据湖平台。

## 架构

{{< img src="/img/architecture.png">}}

如上图所示：

**读写：** Paimon 支持多种读写数据和执行OLAP查询的方式。

* 对于读取操作，它支持从历史快照（批处理模式）、从最新偏移量（流处理模式）或以混合方式读取增量快照。
* 对于写入操作，它支持从数据库的变更日志（CDC）进行流同步，或从离线数据进行批量插入/覆盖。

**生态系统：** 除了支持Apache Flink外，Paimon还支持其他计算引擎，如Apache Hive、Apache Spark和Trino。

**内部：** 在内部，Paimon将列文件存储在文件系统/对象存储中，并使用LSM树结构来支持大量数据更新和高性能查询。

## 统一存储

对于像Apache Flink这样的流处理引擎，通常有三种类型的连接器：

* 消息队列，例如Apache Kafka，在此流水线的源和中间阶段都使用它，以确保延迟保持在几秒钟内。
* OLAP系统，例如ClickHouse，它以流式方式接收处理后的数据，并提供用户的即席查询服务。
* 批处理存储，例如Apache Hive，它支持传统批处理的各种操作，包括`INSERT OVERWRITE`。

Paimon提供表的抽象。它的使用方式与传统数据库没有区别：

* 在`批处理`执行模式下，它就像一个Hive表，支持各种批处理SQL操作。查询它以查看最新的快照。
* 在`流处理`执行模式下，它就像一个消息队列。查询它就像查询来自消息队列的流式变更日志，其中历史数据永不过期。