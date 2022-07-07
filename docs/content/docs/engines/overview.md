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

Table Store not only supports Flink SQL writes and queries natively,
but also provides queries from other popular engines, such as
Apache Hive and Apache Spark.

## Engine Matrix

| Engine    | Version  | Feature                                              |
|:----------|:---------|:-----------------------------------------------------|
| Flink     | 1.14     | read, write, create/drop table, create/drop database |
| Flink     | 1.15     | read, write, create/drop table, create/drop database |
| Hive      | 2.3      | read                                                 |
| Spark     | 3.0      | read                                                 |
| Spark     | 3.1      | read                                                 |
| Spark     | 3.2      | read                                                 |
| Trino     | 358      | read                                                 |
| Trino     | 388      | read                                                 |