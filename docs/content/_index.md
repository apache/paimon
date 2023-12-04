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

Apache Paimon(incubating) is a streaming data lake platform that supports high-speed data ingestion, change data tracking and efficient real-time analytics.

Paimon offers the following core capabilities:

- Unified Batch & Streaming: Paimon supports batch write and batch read, as well as streaming write changes and streaming read table changelogs.
- Data Lake: As a data lake storage, Paimon has the following advantages: low cost, high reliability, and scalable metadata.
- Merge Engines: Paimon supports rich Merge Engines. By default, the last entry of the primary key is reserved. You can also use the "partial-update" or "aggregation" engine.
- Changelog producer: Paimon supports rich Changelog producers, such as "lookup" and "full-compaction". The correct changelog can simplify the construction of a streaming pipeline.
- Append Only Tables: Paimon supports Append Only tables, automatically compact small files, and provides orderly stream reading. You can use this to replace message queues.

{{< columns >}}
## Try Paimon

If you’re interested in playing around with Paimon, check out our
quick start guide with [Flink]({{< ref "engines/flink" >}}), [Spark]({{< ref "engines/spark" >}}) or [Hive]({{< ref "engines/hive" >}}). It provides a step by
step introduction to the APIs and guides you through real applications.

<--->

## Get Help with Paimon

If you get stuck, you can subscribe User Mailing List (user-subscribe@paimon.apache.org),
Paimon tracks issues in GitHub and prefers to receive contributions as pull requests. You can also create an issue.

{{< /columns >}}
