---
title: "Table Mode"
sidebar_position: 3
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

# Table Mode

A table mode determines **where the work of resolving row versions happens**. It does not change
the logical result defined by the [merge engine](./merge-engine/). The modes below use table
options to control compaction and deletion vectors; there is no separate `table-mode` option.

## Choose a Mode

| Mode | Configuration | Read path | Main cost |
| --- | --- | --- | --- |
| Merge On Read (MOR) | Default | Merge overlapping sorted runs | Read CPU and memory grow with overlapping versions |
| Copy On Write (COW) | `full-compaction.delta-commits = 1` | Read the fully compacted result | Frequent full compaction increases write amplification |
| Merge On Write (MOW) | `deletion-vectors.enabled = true` | Read files and skip invalid row positions | Writers look up previous rows and maintain deletion vectors |

For a typical `deduplicate` table with frequent analytical reads, consider MOW. MOR can suit
workloads that favor write throughput. COW can suit workloads that can afford to fully compact
each commit. Evaluate update rate, read latency, and compaction resources before choosing.

![MOR merges versions on read; COW rewrites them during full compaction; MOW masks obsolete rows with deletion vectors.](/img/primary-key-read-write-modes.svg)

## Merge On Read

MOR is the default. Writes create sorted files, and compaction reduces the number of runs over
time. Compaction can include full compactions; MOR does not mean that only minor compactions run.

Readers merge overlapping key ranges before returning logical rows. Those ranges must be read
together, which constrains parallelism and makes bucket sizing important. See
[Data Distribution](./data-distribution) for bucket assignment and sizing.

Filters on mutable non-key columns generally cannot be applied before merging. For example,
if an old row has `status = 'open'` and its replacement has `status = 'closed'`, filtering files
for `status = 'open'` too early could discard the replacement and incorrectly return the old row.
The reader must resolve overlapping versions first. Where files can be read without merging,
Paimon can apply non-key filters earlier.

## Copy On Write

In Flink SQL:

```sql
ALTER TABLE orders SET ('full-compaction.delta-commits' = '1');
```

This requests synchronous full compaction for modified buckets after each commit; in a Flink
streaming write, the interval is counted in checkpoints. It does not compact after every individual input row.
The resulting fully compacted files can be read without merging overlapping versions.

Repeated full compaction can rewrite a large amount of unchanged data. Use it when the read
benefit justifies the write amplification. The `lookup` changelog producer is incompatible with
`full-compaction.delta-commits`; see [Changelog Producer](./changelog-producer#full-compaction).

## Merge On Write

Enable deletion vectors when creating the table. For example, in Flink SQL:

```sql
CREATE TABLE orders_mow (
    order_id BIGINT,
    amount DECIMAL(12, 2),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'bucket' = '4',
    'deletion-vectors.enabled' = 'true'
);
```

Changing this option on an existing table is blocked by default. A migration requires explicit
`deletion-vectors.modifiable` configuration and full compaction to avoid exposing duplicate rows;
setting the enable flag alone is not a complete migration procedure.

During lookup compaction, Paimon finds earlier versions and records their physical row positions
in deletion vectors. Readers skip those positions while scanning data files, so they can read
files independently without merging overlapping key versions. Applying the deletion vectors
still has a cost, but avoids the MOR merge work.

The example below uses `deduplicate`: an update replaces key `7`, while a delete removes key `9`.
The old data file is retained, with both obsolete positions marked in its deletion vector.

![An update to key 7 and a delete of key 9 mark two old file positions; readers see the new value of key 7 and the unchanged key 11.](/img/primary-key-deletion-vector-update.svg)

:::info Data visibility

By default, batch reads skip Level-0 files until lookup compaction publishes them. Writers wait
for this compaction by default. Asynchronous compaction or a dedicated compaction job can delay
visibility; see [Asynchronous Compaction](./compaction#asynchronous-compaction).

For batch scans, `deletion-vectors.merge-on-read = true` includes uncompacted data by merging it
at read time, with additional read cost. It does not change streaming changelog behavior.

:::

Check the requirements of specialized features before combining them. In particular,
[First Row](./merge-engine/first-row), [PK Clustering Override](./pk-clustering-override), and
[Primary-Key Indexes](./global-index#requirements) have their own deletion-vector constraints.

## MOR Read Optimized

A MOR table can also provide a faster view of older, fully compacted data:

1. Configure `compaction.optimization-interval` to schedule optimization full compactions.
2. Query the [read-optimized system table](../concepts/system-tables#read-optimized-table).

This view reads the optimized result without merging subsequent updates. Its freshness depends
on completed optimization compactions. Use the regular table when a query must include newer data.
