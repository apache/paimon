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

For MOW (Deletion Vectors) or COW table or [Read Optimized]({{< ref "maintenance/system-tables#read-optimized-table" >}}) table,
There is no limit to the concurrency of reading data, and they can also utilize some filtering conditions for non-primary-key columns.

## Data Skipping By Primary Key Filter

For a regular bucketed table (For example, bucket = 5), the filtering conditions of the primary key will greatly
accelerate queries and reduce the reading of a large number of files.

## Data Skipping By File Index

You can use file index to table with Deletion Vectors enabled, it filters files by index on the read side.

```sql
CREATE TABLE <PAIMON_TABLE> WITH (
    'deletion-vectors' = 'true',
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
* `file-index.bitmap.columns`: specify the columns that need bitmap index.

`Bit-Slice Index Bitmap`
* `file-index.bsi.columns`: specify the columns that need bsi index.

More filter types will be supported...

If you want to add file index to existing table, without any rewrite, you can use `rewrite_file_index` procedure. Before
we use the procedure, you should config appropriate configurations in target table. You can use ALTER clause to config
`file-index.<filter-type>.columns` to the table.

How to invoke: see [flink procedures]({{< ref "flink/procedures#procedures" >}}) 
