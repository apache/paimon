---
title: "Read Performance"
weight: 2
type: docs
aliases:
- /maintenance/read-performance.html
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

# Read Performance

## Full Compaction

Configure 'full-compaction.delta-commits' perform full-compaction periodically in Flink writing.
And it can ensure that partitions are full compacted before writing ends.

{{< hint info >}}
Paimon defaults to handling small files and providing decent read performance. Please do not configure
this full-compaction option without any requirements, as it will have a significant impact on performance.
{{< /hint >}}

### Primary Key Table

For Primary Key Table, it's a 'MergeOnRead' technology. When reading data, multiple layers of LSM data are merged,
and the number of parallelism will be limited by the number of buckets. Although Paimon's merge will be efficient,
it still cannot catch up with the ordinary AppendOnly table.

If you want to query fast enough in certain scenarios, but can only find older data, you can:

1. Configure 'full-compaction.delta-commits', when writing data (currently only Flink), full compaction will be performed periodically.
2. Configure 'scan.mode' to 'compacted-full', when reading data, snapshot of full compaction is picked. Read performance is good.

You can flexibly balance query performance and data latency when reading.

### Append Only Table

Small files can slow reading and affect DFS stability. By default, when there are more than 'compaction.max.file-num'
(default 50) small files in a single bucket, a compaction is triggered. However, when there are multiple buckets, many
small files will be generated.

You can use full-compaction to reduce small files. Full-compaction will eliminate most small files.

## Format

Paimon has some query optimizations to parquet reading, so parquet will be slightly faster that orc.
