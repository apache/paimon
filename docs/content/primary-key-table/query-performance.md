---
title: "Query Performance"
weight: 8
type: docs
aliases:
- /primary-key-table/query-performance.html
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

# Query Performance

## Table Mode

The table schema has the greatest impact on query performance. See [Table Mode]({{< ref "primary-key-table/table-mode" >}}).

For Merge On Read table, the most important thing you should pay attention to is the number of buckets, which will limit
the concurrency of reading data.

For MOW (Deletion Vectors) or COW table or [Read Optimized]({{< ref "concepts/system-tables#read-optimized-table" >}}) table,
there is no limit to the concurrency of reading data, and they can also utilize some filtering conditions for non-primary-key columns.

## Data Skipping By Primary Key Filter

For a regular bucketed table (For example, bucket = 5), the filtering conditions of the primary key will greatly
accelerate queries and reduce the reading of a large number of files.

## Data Skipping By File Index

You can use file index to table with Deletion Vectors enabled, it filters files by index on the read side.

```sql
CREATE TABLE <PAIMON_TABLE> WITH (
    'deletion-vectors.enabled' = 'true',
    'file-index.bloom-filter.columns' = 'c1,c2',
    'file-index.bloom-filter.c1.items' = '200'
);
```

Supported filter types:

`Bloom Filter`:
* `file-index.bloom-filter.columns`: specify the columns that need bloom filter index.
* `file-index.bloom-filter.<column_name>.fpp` to config false positive probability.
* `file-index.bloom-filter.<column_name>.items` to config the expected distinct items in one data file.

`Bitmap`:
* `file-index.bitmap.columns`: specify the columns that need bitmap index. See [Index Bitmap]({{< ref "concepts/spec/fileindex#index-bitmap" >}}).

`Bit-Slice Index Bitmap`
* `file-index.bsi.columns`: specify the columns that need bsi index.

More filter types will be supported...

If you want to add file index to existing table, without any rewrite, you can use `rewrite_file_index` procedure. Before
we use the procedure, you should config appropriate configurations in target table. You can use ALTER clause to config
`file-index.<filter-type>.columns` to the table.

How to invoke: see [flink procedures]({{< ref "flink/procedures#procedures" >}}) 

## Dedicated Split Generation
When Paimon table snapshots contain large amount of source splits, Flink jobs reading from this table might endure long initialization time or even OOM in JobManagers. In this case, you can configure `'scan.dedicated-split-generation' = 'true'` to avoid such problem. This option would enable executing the source split generation process in a dedicated subtask that runs on TaskManager, instead of in the source coordinator on the JobManager.

Note that this feature could have some side effects on your Flink jobs. For example:

1. It will change the DAG of the flink job, thus breaking checkpoint compatibility if enabled on an existing job.
2. It may lead to the Flink AdaptiveBatchScheduler inferring a small parallelism for the source reader operator. you can configure `scan.infer-parallelism` to avoid this possible drawback.
3. The failover strategy of the Flink job would be forced into global failover instead of regional failover, given that the dedicated source split generation task would be connected to all downstream subtasks.

So please make sure these side effects are acceptable to you before enabling it.

