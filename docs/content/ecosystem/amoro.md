---
title: "Amoro"
weight: 6
type: docs
aliases:
- /ecosystem/amoro.html
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

# Apache Amoro With Paimon

**[Apache Amoro(incubating)](https://amoro.apache.org)** is a Lakehouse management system built on open data lake formats. Working with compute engines including Flink, Spark, and Trino, Amoro brings pluggable and
**[Table Maintenance](https://amoro.apache.org/docs/latest/self-optimizing/)** features for a Lakehouse to provide out-of-the-box data warehouse experience, and helps data platforms or products easily build infra-decoupled, stream-and-batch-fused and lake-native architecture.
**[AMS](https://amoro.apache.org/docs/latest/#architecture)(Amoro Management Service)** provides Lakehouse management features, like self-optimizing, data expiration, etc. It also provides a unified catalog service for all compute engines, which can also be combined with existing metadata services like HMS(Hive Metastore).


# Table Format

Apache Amoro supports all catalog types supported by paimon, including common catalog: Hadoop, Hive, Glue, JDBC, Nessie and other third-party catalog.
Amoro supports all storage types supported by Paimon, including common store: Hadoop, S3, GCS, ECS, OSS, and so on.

{{< img src="/img/amoro-paimon.png">}}

In the future, Paimon automatic optimization strategy will be supported, and users can achieve the best balance experience by cooperating with Amoro automatic optimization



