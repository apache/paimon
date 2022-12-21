---
title: "LSM Trees"
weight: 4
type: docs
aliases:
- /concepts/lsm-trees.html
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

# LSM Trees

Table Store adapts the LSM tree (log-structured merge-tree) as the data structure for file storage. This documentation breifly introduces the concepts about LSM trees.

## Sorted Runs

LSM tree organizes files into several sorted runs. A sorted run consists of one or multiple [data file]({{< ref "docs/concepts/file-layouts#data-files" >}})s and each data file belongs to exactly one sorted run.

Records within a data file are sorted by their primary keys. WIthin a sorted run, ranges of primary keys of data files never overlap.

{{< img src="/img/sorted-runs.png">}}

As you can see, different sorted runs may have overlapping primary key ranges, and may even contain the same primary key. When querying the LSM tree, all sorted runs must be combined and all records with the same primary key must be merged according to the user-specified [merge engine]({{< ref "docs/features/table-types#merge-engines" >}}) and the timestamp of each record.

New records written into the LSM tree will be first buffered in memory. When the memory buffer is full, all records in memory will be sorted and flushed to disk. A new sorted run is now created.

## Compaction

When more and more records are written into the LSM tree, the number of sorted runs will increase. Because querying an LSM tree requires all sorted runs to be combined, too many sorted runs will result in a poor query performance, or even out of memory.

To limit the number of sorted runs, we have to merge several sorted runs into one big sorted run once in a while. This procedure is called compaction.

However, compaction is a resource intensive procedure which consumes a certain amount of CPU time and disk IO, so too frequent compaction may in turn result in slower writes. It is a trade-off between query and write performance. Table Store currently adapts a compaction strategy similar to Rocksdb's [universal compaction](https://github.com/facebook/rocksdb/wiki/Universal-Compaction).

By default, when Table Store writers append records to the LSM tree, they'll also perform compactions as needed. Users can also choose to perform all compactions in a dedicated compaction job. See [dedicated compaction job]({{< ref "docs/maintenance/write-performance#dedicated-compaction-job" >}}) for more info.
