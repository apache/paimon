---
title: "Query Performance"
weight: 3
type: docs
aliases:
- /append-table/query-performance.html
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

## Data Skipping By Order

Paimon by default records the maximum and minimum values of each field in the manifest file.

In the query, according to the `WHERE` condition of the query, together with the statistics in the manifest we can
perform file filtering. If the filtering effect is good, the query that would have cost minutes will be accelerated to
milliseconds to complete the execution.

Often the data distribution is not always ideal for filtering, so can we sort the data by the field in `WHERE` condition?
You can take a look at [Flink COMPACT Action]({{< ref "maintenance/dedicated-compaction#sort-compact" >}}),
[Flink COMPACT Procedure]({{< ref "flink/procedures" >}}) or [Spark COMPACT Procedure]({{< ref "spark/procedures" >}}).

## Data Skipping By File Index

You can use file index too, it filters files by indexing on the reading side.

```sql
CREATE TABLE <PAIMON_TABLE> (<COLUMN> <COLUMN_TYPE> , ...) WITH (
    'file-index.bloom-filter.columns' = 'c1,c2',
    'file-index.bloom-filter.c1.items' = '200'
);
```

Define `file-index.bloom-filter.columns`, Data file index is an external index file and Paimon will create its
corresponding index file for each file. If the index file is too small, it will be stored directly in the manifest,
otherwise in the directory of the data file. Each data file corresponds to an index file, which has a separate file
definition and can contain different types of indexes with multiple columns.

Different file indexes may be efficient in different scenarios. For example bloom filter may speed up query in point lookup
scenario. Using a bitmap may consume more space but can result in greater accuracy.

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
