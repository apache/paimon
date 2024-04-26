---
title: Apache Paimon
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

# Apache Paimon

Apache Paimon is a lake format that enables building a Realtime Lakehouse Architecture with Flink and Spark 
for both streaming and batch operations. Paimon innovatively combines lake format and LSM (Log-structured merge-tree) 
structure, bringing realtime streaming updates into the lake architecture.

Paimon offers the following core capabilities:

- Realtime updates:
  - Primary key table supports writing of large-scale updates, has very high update performance, typically through Flink Streaming.
  - Support defining Merge Engines, update records however you like. Deduplicate to keep last row, or partial-update, or aggregate records, or first-row, you decide.
  - Support defining changelog-producer, produce correct and complete changelog in updates for merge engines, simplifying your streaming analytics.
- Huge Append Data Processing:
  - Append table (no primary-key) provides large scale batch & streaming processing capability. Automatic Small File Merge.
  - Supports Data Compaction with z-order sorting to optimize file layout, provides fast queries based on data skipping using indexes such as minmax.
- Data Lake Capabilities: 
  - Scalable metadata: supports storing Petabyte large-scale datasets and storing a large number of partitions.
  - Supports ACID Transactions & Time Travel & Schema Evolution.

{{< columns >}}

## Try Paimon

If you’re interested in playing around with Paimon, check out our
quick start guide with [Flink]({{< ref "flink/quick-start" >}}) or [Spark]({{< ref "spark/quick-start" >}}). It provides a step by
step introduction to the APIs and guides you through real applications.

<--->

## Get Help with Paimon

If you get stuck, you can subscribe User Mailing List (user-subscribe@paimon.apache.org),
Paimon tracks issues in GitHub and prefers to receive contributions as pull requests. You can also create an issue.

{{< /columns >}}
