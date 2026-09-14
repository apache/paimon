---
title: "Query Performance"
sidebar_position: 8
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

Start with the scan plan and the workload: which partitions and columns are filtered, whether
rows still need merging, and whether the query is a scan, aggregate, join, or ranked search.

| Symptom or query pattern | Check first | Next step |
| --- | --- | --- |
| MOR scan spends time merging | Overlapping sorted runs and bucket sizes | Tune [compaction](./compaction) or evaluate a different [table mode](./table-mode) |
| Selective primary-key lookup scans too much data | Partition, bucket-key, and file key-range pruning | [Primary-key filters](#data-skipping-by-primary-key-filter) |
| Filters on non-key columns scan many files | Whether versions must be merged before filtering | [File indexes](#data-skipping-by-file-index) or [clustering](./pk-clustering-override) |
| Vector, text, or scalar index search | Index family and coverage of compacted data | [Primary-Key Indexes](./global-index) |
| Large join shuffle | Fixed-bucket layout on both inputs | [Bucketed Join](#bucketed-join) |

## Table Mode

[Table Mode](./table-mode) determines whether files can be read independently. MOR merges
files with overlapping key ranges, which constrains split planning and parallelism. Bucket count
and skew therefore matter, alongside the number of sorted runs.

MOW, fully compacted COW results, and the
[read-optimized table](../concepts/system-tables#read-optimized-table) can avoid that merge work
and use file-based splits. Parallelism still depends on file sizes, split planning, and available
engine resources. Resolving row versions also makes filters on non-key columns safe to apply at
the file scan; MOR generally has to apply such filters after merging.

## Aggregate push down

Deletion-vector tables can support metadata-based `COUNT(*)` when the planned splits expose exact
merged row counts and the connector can push down the query. For example, with `dt` as a partition
column:

```sql
SELECT COUNT(*) FROM orders WHERE dt = '20230101';
```

Additional row filters can require reading data. Use the engine's `EXPLAIN` output to verify the
actual plan rather than assuming every count is answered from metadata.

Spark does **not** currently push `MIN` or `MAX` into metadata aggregation for primary-key tables.
File statistics can describe obsolete physical rows as well as visible rows, so default
`metadata.stats-mode` alone does not make this optimization available.

## Data Skipping By Primary Key Filter

For a fixed-bucket table, equality conditions that determine the bucket key can prune buckets.
Partition filters prune partitions, and primary-key range statistics can prune files. The default
bucket key excludes partition columns from the primary key.

A predicate on only part of a composite key does not necessarily identify a bucket. Primary-key
sorting and key-range pruning are also affected by [PK Clustering Override](./pk-clustering-override),
which sorts files by other columns.

## Data Skipping By File Index

File indexes can narrow reads of fully compacted files or tables using deletion vectors, where
row versions do not need to be merged across the filtered files. Paimon still applies the relevant
row filters and deletion vectors for correctness.

| Index | Table option | Useful predicate pattern |
| --- | --- | --- |
| [Bloom filter](../concepts/spec/fileindex#index-bloomfilter) | `file-index.bloom-filter.columns` | Equality and point lookups |
| [Bitmap](../concepts/spec/fileindex#index-bitmap) | `file-index.bitmap.columns` | Exact matches on indexed values |
| [Range bitmap](../concepts/spec/fileindex#index-range-bitmap) | `file-index.range-bitmap.columns` | Range predicates |

A data file can have indexes for several columns. Small index data can be embedded in metadata;
larger index data is stored alongside the data file. Size and selectivity determine whether an
index saves enough scan work to justify its storage and maintenance cost.

To add file indexes to an existing table without rewriting its data files, configure the
`file-index.<type>.columns` options and run `rewrite_file_index`; see
[Flink Procedures](../flink/procedures).

File indexes are separate from [Primary-Key Indexes](./global-index), whose index groups follow
compacted data levels and support scalar predicates and ranked searches. Check that page's
coverage rules, especially for Vector and Full Text search.

## Bucketed Join

Spark can use compatible fixed-bucket layouts to avoid a join shuffle. For example:

```sql
SET spark.sql.sources.v2.bucketing.enabled = true;

CREATE TABLE fact_table (order_id INT, f1 STRING) USING paimon
TBLPROPERTIES ('bucket' = '10', 'primary-key' = 'order_id');

CREATE TABLE dim_table (order_id INT, f2 STRING) USING paimon
TBLPROPERTIES ('bucket' = '10', 'primary-key' = 'order_id');

SELECT * FROM fact_table AS fact
JOIN dim_table AS dim ON fact.order_id = dim.order_id;
```

The setting enables Spark to use partitioning reported by V2 data sources. The join keys, bucket
keys, and bucket counts must be compatible; the same bucket count alone is not sufficient.
Check `EXPLAIN` to confirm that Spark avoids the shuffle for the actual query.
