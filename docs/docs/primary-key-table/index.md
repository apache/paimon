---
title: "Primary-Key Table"
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

<a id="overview"></a>

# Primary-Key Tables

A primary-key table maintains one logical row per key. Incoming records can insert, update, or
remove that row according to the table's **merge engine**. Multiple physical versions may remain
in data files until they are merged during reads or compaction.

Use a primary-key table for CDC replication, upsert pipelines, or combining updates to the same
entity. For data that only grows without merging by key, see [Append Table](../append-table/).

## Quick Start

The following Flink SQL table keeps the latest row for each `order_id`:

```sql
CREATE TABLE orders (
    order_id BIGINT,
    customer_id BIGINT,
    amount DECIMAL(12, 2),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'bucket' = '4'
);

INSERT INTO orders VALUES (1, 101, 12.00);
INSERT INTO orders VALUES (1, 101, 15.00);

SELECT * FROM orders;
-- 1, 101, 15.00
```

This example uses four fixed buckets, the default `deduplicate` merge engine, and the default
merge-on-read storage mode. Omitting `bucket` selects dynamic buckets. See
[Data Distribution](./data-distribution) before choosing a bucket mode for your workload, and
[Sequence Field](./sequence-rowkind#sequence-field) when updates can arrive out of order.

## Design a Primary-Key Table

These settings answer different questions. Choose them together, checking each feature's
compatibility requirements.

| Decision | What it controls | Read next |
| --- | --- | --- |
| Partition and bucket keys | Where a row is stored and how work is distributed | [Data Distribution](./data-distribution) |
| Merge engine | How records with the same key form a logical row | [Merge Engine](./merge-engine/) |
| Sequence and row kind | Which update takes precedence and whether it inserts or retracts | [Sequence and Row Kind](./sequence-rowkind) |
| Table mode | Whether readers merge versions, read compacted files, or apply deletion vectors | [Table Mode](./table-mode) |
| Changelog producer | Which changes a streaming consumer receives | [Changelog Producer](./changelog-producer) |

Then tune [Compaction](./compaction) and [Query Performance](./query-performance). For specialized
workloads, see [Primary-Key Indexes](./global-index), [PK Clustering Override](./pk-clustering-override),
[Chain Table](./chain-table), and [BLOB Storage](./blob-storage).

## Bucket

A table, or each partition of a partitioned table, contains buckets. Each bucket has its own LSM
tree of data files. Changelog files and indexes may accompany those data files, depending on the
configuration.

![A partition contains independent buckets, each with Level-0 files and higher-level sorted runs.](/img/primary-key-storage-layout.svg)

In fixed-bucket mode, Paimon hashes the `bucket-key` columns to select a bucket. If `bucket-key`
is not set, it uses the primary-key columns excluding partition columns. Dynamic and postpone
buckets use different assignment mechanisms; see [Data Distribution](./data-distribution).

Buckets distribute writes and affect read parallelism, but are not a universal one-reader limit:
[Table Mode](./table-mode) explains when files can be read independently. Use the
[bucket sizing guidance](./data-distribution#bucket-sizing) to balance parallelism against
small-file overhead.

## LSM Trees

Paimon stores primary-key data in an LSM tree (log-structured merge-tree). Writers buffer records,
sort them, and flush new files. Compaction merges files to reduce the work needed by future reads.

### Sorted Runs

A sorted run contains one or more data files. In the default primary-key layout, records in each
file are sorted by key, and file key ranges do not overlap within the same run. Different runs
can overlap and contain different versions of the same key.

![Files have disjoint key ranges within a sorted run; key 7 occurs in two different runs.](/img/primary-key-sorted-runs.svg)

A merge-on-read scan combines the relevant runs and applies the
[merge engine](./merge-engine/) and [record ordering](./sequence-rowkind) to records with the same
key. The result contains one logical row per key, even if several physical versions exist.

Each Level-0 file is a separate sorted run; each higher level forms a run of non-overlapping
files. [Compaction](./compaction) keeps the number of runs manageable.
[PK Clustering Override](./pk-clustering-override) is a specialized layout that changes the physical
sort order and resolves row versions using deletion vectors.

## Nullable Primary Keys

Primary key fields are `NOT NULL` by default. Set `primary-key.nullable` to `true` when a source
system can produce null key components:

```sql
CREATE TABLE nullable_orders (
    order_id BIGINT,
    payload STRING
) WITH (
    'primary-key' = 'order_id',
    'primary-key.nullable' = 'true'
);
```

Null key components use null-safe equality. For example, two records whose key is `(1, NULL)` are
treated as the same key and are merged by the configured merge engine. The option is disabled by
default and cannot be changed after the table has snapshots.

In Flink, define a nullable Paimon primary key with the `primary-key` table option as shown above.
The standard SQL `PRIMARY KEY` constraint implies `NOT NULL`, so Paimon does not expose a nullable
key as a Flink SQL primary-key constraint.

Flink streaming reads that emit updates or deletes require a full changelog producer, for example
`changelog-producer=input`. The default `changelog-producer=none` produces an upsert changelog,
which Flink can normalize only when the table exposes a SQL primary-key constraint. Because a
nullable key cannot be exposed as that constraint, Paimon rejects this streaming-read combination
instead of producing an invalid Flink plan. Insert-only streaming reads, such as tables using the
`first-row` merge engine, are not affected.
