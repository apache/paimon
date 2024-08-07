---
title: "Overview"
weight: 1
type: docs
aliases:
- /engines/overview.html
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

# Overview

## Compatibility Matrix

|                                     Engine                                      |    Version    |  Batch Read | Batch Write |  Create Table |  Alter Table  | Streaming Write  |  Streaming Read  | Batch Overwrite |  DELETE & UPDATE   | MERGE INTO  | Time Travel |
|:-------------------------------------------------------------------------------:|:-------------:|:-----------:|:-----------:|:-------------:|:-------------:|:----------------:|:----------------:|:---------------:|:------------------:|:-----------:|:-----------:|
|                                      Flink                                      |  1.15 - 1.20  |     ✅      |      ✅      |      ✅       |  ✅(1.17+)   |        ✅        |       ✅        |        ✅        |    ✅(1.17+)      |      ❌     |      ✅      |
|                                      Spark                                      |   3.1 - 3.5   |     ✅      |   ✅(3.2+)   |      ✅       |      ✅      |      ✅(3.3+)    |    ✅(3.3+)     |     ✅(3.2+)     |     ✅(3.2+)      |   ✅(3.2+)  |   ✅(3.3+)   |
|                                      Hive                                       |   2.1 - 3.1   |     ✅      |      ✅      |      ✅       |      ❌      |        ❌        |       ❌        |        ❌        |         ❌        |      ❌     |      ✅      |
|                                      Trino                                      |   420 - 439   |     ✅      |   ✅(427+)   |   ✅(427+)    |   ✅(427+)   |        ❌        |       ❌        |        ❌        |         ❌        |      ❌     |      ✅      |
|                                     Presto                                      | 0.236 - 0.280 |     ✅      |      ❌      |      ✅       |      ✅      |        ❌        |       ❌        |        ❌        |         ❌        |      ❌     |      ❌      |
| [StarRocks](https://docs.starrocks.io/docs/data_source/catalog/paimon_catalog/) |     3.1+      |     ✅      |      ❌      |      ❌       |      ❌      |        ❌        |       ❌        |        ❌        |         ❌        |      ❌     |      ✅      |
| [Doris](https://doris.apache.org/docs/lakehouse/datalake-analytics/paimon)      |    2.0.6+     |     ✅      |      ❌      |      ❌       |      ❌      |        ❌        |       ❌        |        ❌        |         ❌        |      ❌     |      ✅      |

## Streaming Engines

### Flink Streaming

Flink is the most comprehensive streaming computing engine that is widely used for data CDC ingestion and the
construction of streaming pipelines.

Recommended version is Flink 1.17.2.

### Spark Streaming

You can also use Spark Streaming to build a streaming pipeline. Spark's schema evolution capability will be better
implemented, but you must accept the mechanism of mini-batch.

## Batch Engines

### Spark Batch

Spark Batch is the most widely used batch computing engine.

Recommended version is Spark 3.4.3.

### Flink Batch

Flink Batch is also available, which can make your pipeline more integrated with streaming and batch unified.

## OLAP Engines

### StarRocks

StarRocks is the most recommended OLAP engine with the most advanced integration.

Recommended version is StarRocks 3.2.6.

### Other OLAP

You can also use Doris and Trino and Presto, or, you can just use Spark, Flink and Hive to query Paimon tables.

## Download

[Download Link]({{< ref "project/download#engine-jars" >}})
