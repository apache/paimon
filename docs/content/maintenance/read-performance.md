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
and the number of parallelism will be limited by the number of buckets. Although Paimon's merge performance is efficient,
it still cannot catch up with the ordinary AppendOnly table.

If you want to query fast enough in certain scenarios, but can only find older data, you can:

1. Configure 'full-compaction.delta-commits' when writing data (currently only in Flink). For streaming jobs, full compaction will then be performed periodically; For batch jobs, full compaction will be carried out when the job ends.
2. Query from [read-optimized system table]({{< ref "how-to/system-tables#read-optimized-table" >}}). Reading from results of full compaction avoids merging records with the same key, thus improving reading performance.

You can flexibly balance query performance and data latency when reading.

### Append Only Table

Small files will slow down reading performance and affect the stability of DFS. By default, when there are more than 
'compaction.max.file-num' (default 50) small files in a single bucket, a compaction task will be triggered to compact 
them. Furthermore, if there are multiple buckets, many small files will be generated.

You can use full-compaction to reduce small files. Full-compaction will eliminate most small files.

## Format

Paimon has some query optimizations to parquet reading, so parquet will be slightly faster that orc.
