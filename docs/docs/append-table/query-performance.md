---
title: "Query Performance"
sidebar_position: 2
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

Append-table queries can avoid work at several layers. Start with the predicates your queries use, then choose a
layout or index that helps eliminate irrelevant data.

| Query pattern | Optimization | What it can skip |
| --- | --- | --- |
| Filters on partition columns | Partition pruning | Other partitions. |
| Equality or `IN` on all bucket-key columns | [Bucket pruning](./bucketed#data-skipping) | Other buckets in a bucketed table. |
| Selective filters on data columns | File statistics, improved by clustering | Files whose value ranges cannot match. |
| Predicates supported by a file index | Bloom filter, bitmap, or range bitmap indexes | Irrelevant data identified by the index. |
| Supported aggregates with sufficient metadata | Aggregate pushdown | Reading data rows to compute the aggregate. |

These optimizations can be combined. Their effectiveness depends on the engine, the predicate, and the distribution of
the stored values. Use the engine's `EXPLAIN` output and scan metrics to check which optimizations a query actually uses.

## File Statistics and Clustering

Paimon stores column statistics in file metadata, subject to the table's statistics configuration. Min/max values let
a reader reject files whose ranges do not overlap a query predicate. For example:

```sql
SELECT * FROM my_table
WHERE dt = '2026-09-10' AND product_id BETWEEN 100 AND 200;
```

The partition filter first limits the scan to one date. Within that partition, file statistics can exclude files
whose `product_id` range falls outside the requested interval. If every file contains a broad mix of product IDs,
min/max pruning will be less effective.

![The same nine product IDs are spread across three files before clustering. After clustering, only one file overlaps product IDs 100 through 200.](/img/append-file-pruning.svg)

In this example, every unsorted file overlaps the predicate, so all three must be read. After clustering, only the
middle file overlaps it. File pruning selects candidate files; the reader still evaluates the predicate on their rows.

[Incremental clustering](./incremental-clustering) sorts selected files by frequently filtered columns, which can make
their value ranges more selective without rewriting every file on each run. Use
[sort compaction](../maintenance/dedicated-compaction#sort-compact) for an explicit sort rewrite. Clustering improves the
physical layout; SQL result ordering still requires `ORDER BY`.

## File Indexes

File indexes provide filtering beyond min/max statistics. Configure the relevant columns before writing indexed data:

| Index | Table option | Typical use |
| --- | --- | --- |
| [Bloom filter](../concepts/spec/fileindex#index-bloomfilter) | `file-index.bloom-filter.columns` | Equality lookups; false positives may still require reading data. |
| [Bitmap](../concepts/spec/fileindex#index-bitmap) | `file-index.bitmap.columns` | Equality and set-membership filtering. |
| [Range bitmap](../concepts/spec/fileindex#index-range-bitmap) | `file-index.range-bitmap.columns` | Range filtering. |

Each indexed data file has associated index data. Small indexes can be embedded in the manifest; larger indexes are
stored alongside the data files. Indexes add storage and write work, so select columns used by relevant queries.

### Index Existing Files

Changing the table options affects subsequently written files; it does not add indexes to existing files. After setting
the options, run `rewrite_file_index` to build indexes for existing data without rewriting the data files. The procedure
still reads the data needed to construct the indexes.

For example, in Flink SQL, with the table in the `default` database:

```sql
ALTER TABLE my_table SET ('file-index.bloom-filter.columns' = 'product_id');
CALL sys.rewrite_file_index(`table` => 'default.my_table');
```

The equivalent Spark SQL is:

```sql
ALTER TABLE my_table SET TBLPROPERTIES ('file-index.bloom-filter.columns' = 'product_id');
CALL sys.rewrite_file_index(table => 'default.my_table');
```

Use the actual database name for your table. To limit the rewrite, Flink accepts a `partitions` argument and Spark
accepts a partition predicate in `where`. See [Flink procedures](../flink/procedures) and
[Spark procedures](../spark/procedures) for those engine-specific arguments.

### Check Index Coverage

Query the [file indexes system table](../concepts/system-tables#file-indexes-table) to see which data files have the
configured indexes. For example, in Spark SQL:

```sql
SELECT column_name, index_type, storage_type,
       COUNT(DISTINCT file_path) AS indexed_file_count
FROM `my_table$file_indexes`
GROUP BY column_name, index_type, storage_type
ORDER BY column_name, index_type, storage_type;
```

Each row in the system table describes one column and index type in one data file. `storage_type` is `EMBEDDED` for
index data stored in metadata and `FILE` for an external index file. Compare the indexed files with `my_table$files`
to check coverage; the presence of an index alone does not show that a query used it. Check the query's predicates,
plan, and scan metrics as well.

## Aggregate Pushdown

Supported aggregate queries can use table metadata. Using the partitioned table from the [overview](./):

```sql
SELECT COUNT(*) FROM my_table WHERE dt = '2026-09-10';
```

Spark can also use column statistics for supported `MIN` and `MAX` queries:

```sql
SELECT MIN(price), MAX(sales) FROM my_table WHERE dt = '2026-09-10';
```

Keep the required statistics available through `metadata.stats-mode` and any column-specific statistics settings.
Pushdown depends on the query and available metadata: a filter that needs row-by-row evaluation can prevent a
metadata-only aggregate. These examples use a partition predicate so that complete files can be selected.

Spark can also use statistics to reduce the files scanned for a top-N query:

```sql
SELECT * FROM my_table ORDER BY price LIMIT 1;
```

Top-N pruning does not imply that the full result can be produced from metadata alone. Check the query plan rather
than assuming every aggregate or `ORDER BY ... LIMIT` query avoids reading data files.
