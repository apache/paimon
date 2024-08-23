---
title: "Streaming"
weight: 2
type: docs
aliases:
- /append-table/streaming.html
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

# Streaming

You can streaming write to the Append table in a very flexible way through Flink, or through read the Append table
Flink, using it like a queue. The only difference is that its latency is in minutes. Its advantages are very low cost
and the ability to push down filters and projection.

## Automatic small file merging

In streaming writing job, without bucket definition, there is no compaction in writer, instead, will use
`Compact Coordinator` to scan the small files and pass compaction task to `Compact Worker`. In streaming mode, if you
run insert sql in flink, the topology will be like this:

{{< img src="/img/unaware-bucket-topo.png">}}

Do not worry about backpressure, compaction never backpressure.

If you set `write-only` to true, the `Compact Coordinator` and `Compact Worker` will be removed in the topology.

The auto compaction is only supported in Flink engine streaming mode. You can also start a compaction job in flink by
flink action in paimon and disable all the other compaction by set `write-only`.

## Streaming Query

You can stream the Append table and use it like a Message Queue. As with primary key tables, there are two options
for streaming reads:
1. By default, Streaming read produces the latest snapshot on the table upon first startup, and continue to read the
   latest incremental records.
2. You can specify `scan.mode` or `scan.snapshot-id` or `scan.timestamp-millis` or `scan.file-creation-time-millis` to
   streaming read incremental only.

Similar to flink-kafka, order is not guaranteed by default, if your data has some sort of order requirement, you also
need to consider defining a `bucket-key`, see [Bucketed Append]({{< ref "append-table/bucketed" >}})
