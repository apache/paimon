---
title: "Read Optimized"
weight: 7
type: docs
aliases:
- /primary-key-table/read-optimized.html
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

# Read Optimized

## Overview

For Primary Key Table, it's a 'MergeOnRead' technology. When reading data, multiple layers of LSM data are merged,
and the number of parallelism will be limited by the number of buckets. Although Paimon's merge performance is efficient,
it still cannot catch up with the ordinary AppendOnly table.

We recommend that you use [Deletion Vectors]({{< ref "primary-key-table/deletion-vectors" >}}) mode.

If you don't want to use Deletion Vectors mode, you want to query fast enough in certain scenarios, but can only find
older data, you can also:

1. Configure 'compaction.optimization-interval' when writing data. For streaming jobs, optimized compaction will then
   be performed periodically; For batch jobs, optimized compaction will be carried out when the job ends. (Or configure
   `'full-compaction.delta-commits'`, its disadvantage is that it can only perform compaction synchronously, which will
   affect writing efficiency)
2. Query from [read-optimized system table]({{< ref "maintenance/system-tables#read-optimized-table" >}}). Reading from
   results of optimized files avoids merging records with the same key, thus improving reading performance.

You can flexibly balance query performance and data latency when reading.
