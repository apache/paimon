---
title: Apache Flink Table Store
type: docs
bookToc: false
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

# Apache Flink Table Store

Flink Table Store is a unified storage to build dynamic tables for both streaming and
batch processing in Flink, supporting high-speed data ingestion and timely data query.
Table Store offers the following core capabilities:
- Support storage of large datasets and allow read/write in both batch and streaming mode.
- Support streaming queries with minimum latency down to milliseconds.
- Support Batch/OLAP queries with minimum latency down to the second level.
- Support incremental snapshots for stream consumption by default. So users do not need to combine different pipelines by themself.

{{< columns >}}
## Try Table Store

If you’re interested in playing around with Flink Table Store, check out our
quick start guide with [Flink]({{< ref "docs/engines/flink" >}}), [Spark]({{< ref "docs/engines/spark3" >}}) or [Hive]({{< ref "docs/engines/hive" >}}). It provides a step by
step introduction to the APIs and guides you through real applications.

<--->

## Get Help with Table Store

If you get stuck, check out our [community support
resources](https://flink.apache.org/community.html). In particular, Apache
Flink’s user mailing list is consistently ranked as one of the most active of
any Apache project, and is a great way to get help quickly.

{{< /columns >}}

Flink Table Store is developed under the umbrella of
[Apache Flink](https://flink.apache.org/).
