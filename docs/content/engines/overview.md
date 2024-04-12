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

|                                     Engine                                      |    Version    | Batch Read | Batch Write | Create Table | Alter Table | Streaming Write | Streaming Read | Batch Overwrite |
|:-------------------------------------------------------------------------------:|:-------------:|:----------:|:-----------:|:------------:|:-----------:|:---------------:|:--------------:|:---------------:|
|                                      Flink                                      |  1.15 - 1.19  |     ✅      |      ✅      |      ✅       |  ✅(1.17+)   |        ✅        |       ✅        |        ✅        |
|                                      Spark                                      |   3.1 - 3.5   |     ✅      |      ✅      |      ✅       |      ✅      |        ✅        |    ✅(3.3+)     |        ✅        |
|                                      Hive                                       |   2.1 - 3.1   |     ✅      |      ✅      |      ✅       |      ❌      |        ❌        |       ❌        |        ❌        |
|                                      Spark                                      |      2.4      |     ❌      |      ❌      |      ❌       |      ❌      |        ❌        |       ❌        |        ❌        |
|                                      Trino                                      |   422 - 426   |     ✅      |      ❌      |      ❌       |      ❌      |        ❌        |       ❌        |        ❌        |
|                                      Trino                                      |   427 - 439   |     ✅      |      ❌      |      ✅       |      ✅      |        ❌        |       ❌        |        ❌        |
|                                     Presto                                      | 0.236 - 0.280 |     ✅      |      ❌      |      ✅       |      ✅      |        ❌        |       ❌        |        ❌        |
| [StarRocks](https://docs.starrocks.io/docs/data_source/catalog/paimon_catalog/) |     3.1+      |     ✅      |      ❌      |      ❌       |      ❌      |        ❌        |       ❌        |        ❌        |
|     [Doris](https://doris.apache.org/docs/lakehouse/multi-catalog/paimon/)      |     2.0+      |     ✅      |      ❌      |      ❌       |      ❌      |        ❌        |       ❌        |        ❌        |

Recommended versions are Flink 1.17.2, Spark 3.5.0, Hive 2.3.9

## Download

[Download Link]({{< ref "project/download#engine-jars" >}})
