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

Flink Table Store is a unified streaming and batch store for building dynamic
tables on Apache Flink. It is designed to be the best connector to Flink as
the storage for streaming warehouse. It uses a full Log-Structured Merge-Tree
(LSM) structure for high speed and large amount of data update & query capability.

The Table Store supports the following usage:
- **Streaming Insert**: Write changelog streams, including cdc from database and streams from Flink Sink.
- **Batch Insert**: Write batch data as offline warehouse, including OVERWRITE support.
- **Batch/OLAP Query**: Read snapshot of the storage, efficient querying of real-time data.
- **Streaming Query**: Read changes of the storage, ensure exactly-once consistency.

{{< columns >}}
## Try Table Store

If you’re interested in playing around with Flink Table Store, check out our
[Quickstart]({{< ref "docs/try-table-store/quick-start" >}}). It provides a step by
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
