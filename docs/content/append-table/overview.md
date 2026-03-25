---
title: "Overview"
weight: 1
type: docs
aliases:
- /append-table/overview.html
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

# Overview

If a table does not have a primary key defined, it is an append table. Compared to the primary key table, it does not
have the ability to directly receive changelogs. It cannot be directly updated with data through upsert. It can only
receive incoming data from append data.

{{< tabs "create-append-table" >}}
{{< tab "Flink" >}}
```sql
CREATE TABLE my_table (
    product_id BIGINT,
    price DOUBLE,
    sales BIGINT
) WITH (
    -- 'target-file-size' = '256 MB',
    -- 'file.format' = 'parquet',
    -- 'file.compression' = 'zstd',
    -- 'file.compression.zstd-level' = '3'
);
```
{{< /tab >}}
{{< /tabs >}}

Batch write and batch read in typical application scenarios, similar to a regular Hive partition table, but compared to
the Hive table, it can bring:

1. Time travel enables reproducible queries that use exactly the same table snapshot, or lets users easily examine
   changes. Version rollback allows users to quickly correct problems by resetting tables to a good state.
2. Scan planning is fast — data files are pruned with partition and column-level stats, using table metadata. File
   Index (BloomFilter, Bitmap, Range Bitmap) and aggregate push-down further accelerate queries.
3. Schema evolution supports add, drop, update, or rename columns, and has no side-effects.
4. Rich ecosystem — adds tables to compute engines including Flink, Spark, Hive, Trino, Presto, StarRocks, and Doris,
   working just like a SQL table.
5. Incremental Clustering with z-order/hilbert/order sorting to optimize data layout at low cost.
6. Streaming read & write like a queue, DELETE / UPDATE / MERGE INTO support low-cost row-level operations.

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

## Append Streaming

You can stream write to the Append table in a very flexible way through Flink, or read the Append table through
Flink, using it like a queue. The only difference is that its latency is in minutes. Its advantages are very low cost
and the ability to push down filters and projection.

**Pre small files merging**

"Pre" means that this compact occurs before committing files to the snapshot.

If Flink's checkpoint interval is short (for example, 30 seconds), each snapshot may produce lots of small changelog
files. Too many files may put a burden on the distributed storage cluster.

In order to compact small changelog files into large ones, you can set the table option `precommit-compact = true`.
Default value of this option is false, if true, it will add a compact coordinator and worker operator after the writer
operator, which copies changelog files into large ones.

**Post small files merging**

"Post" means that this compact occurs after committing files to the snapshot.

In streaming write job, without bucket definition, there is no compaction in writer, instead, will use
`Compact Coordinator` to scan the small files and pass compaction task to `Compact Worker`. In streaming mode, if you
run insert sql in flink, the topology will be like this:

{{< img src="/img/unaware-bucket-topo.png">}}

Do not worry about backpressure, compaction never backpressure.

If you set `write-only` to true, the `Compact Coordinator` and `Compact Worker` will be removed in the topology.

The auto compaction is only supported in Flink engine streaming mode. You can also start a compaction job in Flink by
Flink action in Paimon and disable all the other compactions by setting `write-only`.

**Streaming Query**

You can stream the Append table and use it like a Message Queue. As with primary key tables, there are two options
for streaming reads:
1. By default, Streaming read produces the latest snapshot on the table upon first startup, and continue to read the
   latest incremental records.
2. You can specify `scan.mode`, `scan.snapshot-id`, `scan.timestamp-millis` and/or `scan.file-creation-time-millis` to
   stream read incremental only.

Similar to flink-kafka, order is not guaranteed by default, if your data has some sort of order requirement, you also
need to consider defining a `bucket-key`, see [Bucketed Append]({{< ref "append-table/bucketed" >}})

## Aggregate push down

Append Table supports aggregate push down:

```sql
SELECT COUNT(*) FROM TABLE WHERE DT = '20230101';
```

This query can be accelerated during compilation and returns very quickly.

For Spark SQL, table with default `metadata.stats-mode` can be accelerated:

```sql
SELECT MIN(a), MAX(b) FROM TABLE WHERE DT = '20230101';

SELECT * FROM TABLE ORDER BY a LIMIT 1;
```

Min max topN query can be also accelerated during compilation and returns very quickly.

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

Define `file-index.bitmap.columns`, Data file index is an external index file and Paimon will create its
corresponding index file for each file. If the index file is too small, it will be stored directly in the manifest,
otherwise in the directory of the data file. Each data file corresponds to an index file, which has a separate file
definition and can contain different types of indexes with multiple columns.

Different file indexes may be efficient in different scenarios. For example bloom filter may speed up query in point lookup
scenario. Using a bitmap may consume more space but can result in greater accuracy.

* [BloomFilter]({{< ref "concepts/spec/fileindex#index-bloomfilter" >}}): `file-index.bloom-filter.columns`.
* [Bitmap]({{< ref "concepts/spec/fileindex#index-bitmap" >}}): `file-index.bitmap.columns`.
* [Range Bitmap]({{< ref "concepts/spec/fileindex#index-range-bitmap" >}}): `file-index.range-bitmap.columns`.

If you want to add file index to existing table, without any rewrite, you can use `rewrite_file_index` procedure. Before
we use the procedure, you should config appropriate configurations in target table. You can use ALTER clause to config
`file-index.<filter-type>.columns` to the table.

How to invoke: see [flink procedures]({{< ref "flink/procedures#procedures" >}}) 

## Row Level Operations

Now, only Spark SQL supports DELETE & UPDATE & MERGE INTO, you can take a look at [Spark Write]({{< ref "spark/sql-write" >}}).

Example:
```sql
DELETE FROM my_table WHERE currency = 'UNKNOWN';
```

Update append table has two modes:

1. COW (Copy on Write): search for the hit files and then rewrite each file to remove the data that needs to be deleted
   from the files. This operation is costly.
2. MOW (Merge on Write): By specifying `'deletion-vectors.enabled' = 'true'`, the Deletion Vectors mode can be enabled.
   Only marks certain records of the corresponding file for deletion and writes the deletion file, without rewriting the entire file.
