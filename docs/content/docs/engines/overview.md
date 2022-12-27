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
Apache Spark and Apache Hive.

## Compatibility Matrix

| Engine | Version | Feature | Read Pushdown |
|---|---|---|---|
| Flink | 1.16/1.15/1.14 | batch/streaming read, batch/streaming write, create/drop table, create/drop database | Projection, Filter |
| Hive      | 3.1/2.3/2.2/2.1/2.1 CDH 6.3 | batch read | Projection, Filter |
| Spark     | 3.3/3.2/3.1/3.0 | batch read, batch write, create/drop table, create/drop database | Projection, Filter |
| Spark     | 2.4 | batch read | Projection, Filter |
| Trino     | 388/358 | batch read | Projection, Filter |
