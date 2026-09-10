---
title: "Data Distribution"
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

# Data Distribution

Buckets divide the data inside a table or partition into independent LSM trees. Choose a bucket
mode based on how keys arrive, whether updates move between partitions, and how many writers
must ingest concurrently. For the file layout, see [Buckets and LSM Trees](./#bucket).

## Choose a Bucket Mode

| Mode | `bucket` | Assignment | Planning consideration |
| --- | --- | --- | --- |
| Fixed | Positive integer | Hash the bucket key into a fixed number of buckets | Predictable layout; resizing requires an explicit rescale operation |
| Dynamic (default) | `-1` | Maintain a key-to-bucket index and grow buckets | Index memory and single-writer constraints |
| Postpone | `-2` | Stage data, then assign real buckets | Batch/compaction workflow and per-partition sizing |

If the primary key excludes a partition column that can change, read
[Cross Partitions Upsert](#cross-partitions-upsert) before choosing a mode.

## Fixed Bucket

Set `bucket` to a positive integer. Paimon calculates the bucket using
`Math.abs(key_hashcode % numBuckets)`.

For primary-key tables, `bucket-key` defaults to the primary key excluding partition columns.
An explicit bucket key must be a subset of the primary key and must not contain partition columns.
For example, with primary key `(dt, order_id)` and partition key `dt`, `order_id` is the default
bucket key.

### Bucket Sizing

Too many buckets create small files; too few can concentrate writes and leave overlapping ranges
that are expensive to read. For MOR workloads, 200 MB–1 GB per bucket is an initial sizing guide,
not a limit. Measure skew, throughput, and read plans to select the actual count. A batch scan can
split one bucket into independent key ranges, so bucket count is not a hard read-parallelism cap.

To change an existing layout, use the offline [Rescale Bucket](../maintenance/rescale-bucket)
workflow.

## Dynamic Bucket

Dynamic buckets are the default for primary-key tables (`bucket = -1`). Paimon maintains an index
that maps keys to buckets. Existing keys return to their assigned buckets; new keys can be
assigned to new buckets as the table grows. The layout therefore depends on the arrival of keys.
Do not configure `bucket-key` in this mode.

| Option | Purpose |
| --- | --- |
| `dynamic-bucket.target-row-num` | Target row count per bucket |
| `dynamic-bucket.initial-buckets` | Initial bucket count |
| `dynamic-bucket.max-buckets` | Maximum bucket count |

:::warning Concurrent writers

Only one job may write to a given partition in dynamic-bucket mode. Concurrent writers can assign
the same key independently and create duplicates. `write-only` with a dedicated compaction job
does not remove this restriction.

:::

When updates stay within a partition, Paimon uses a hash index for bucket assignment. Budget
additional memory for active partitions: 100 million entries in a partition require roughly 1 GB
of index memory. Inactive partitions do not retain this in-memory index. Evaluate this cost
against the benefit of growing buckets automatically, particularly when most arriving keys are new.

## Postpone Bucket

Postpone bucket mode is configured by `'bucket' = '-2'`.
Use it to defer bucket sizing and allow different partitions to have different bucket counts.
The write path and visibility depend on the engine and `postpone.batch-write-fixed-bucket`.

### Spark Batch Write

By default, `postpone.batch-write-fixed-bucket` is `true`. The fixed-bucket flow uses Spark's
DataSource V1 write path, even when `spark.paimon.write.use-v2-write` is enabled. Unless direct
writing applies, Spark completes each batch in three steps:

1. Write the current batch to uncommitted bucket `-2` files. Spark derives each partition's row
   count and file size directly from the staged file metadata; there is no extra input scan, cache,
   or per-row statistics pass.
2. Calculate the required bucket number per touched partition. For a partition without real
   buckets, an explicitly configured `postpone.default-bucket-num` is used exactly. Otherwise,
   `postpone.target-row-num-per-bucket`, when configured, takes precedence over
   `postpone.target-size-per-bucket` (default `1 GB`). An inferred result is at least `1`, rounded
   up to a power of two, and capped by
   `postpone.batch-write-fixed-bucket.max-parallelism`.
3. Route the staged records to real buckets and commit them. The current batch becomes visible only
   in this commit.

An existing partition normally keeps its bucket number. Spark first rescales its real buckets when
the uncapped required bucket number is greater than the existing bucket number multiplied by
`postpone.batch-write-fixed-bucket.rescale-load-factor` (default `32`), and the capped result is
larger than the existing layout. Different partitions may have different target bucket numbers.
The rescale is a separate overwrite commit which changes real buckets only; the current batch is
appended in the following commit.

`postpone.default-bucket-num` has no default value. When it is explicitly configured, Spark can
skip the staged bucket `-2` files and write directly to real buckets for `INSERT OVERWRITE`, or
when the base snapshot contains no real buckets. An overwrite always uses the configured number
exactly and does not rescale the replaced layout. An append to an existing real-bucket partition
ignores this option and still uses the staged batch to decide whether rescaling is required. If a
batch mixes existing and new real-bucket partitions, the whole batch remains staged; only the new
partitions use the configured default.

Previously committed bucket `-2` files are not included in the calculation, read, rewritten, or
deleted by an append or rescale. They remain available to merge-on-read and regular postpone
compaction. `INSERT OVERWRITE` still follows its normal replacement semantics.

### Compaction-Based Assignment

When `postpone.batch-write-fixed-bucket` is `false`,
records are first stored in the `bucket-postpone` directory of each partition
and are not available to readers.
To move these records into the correct bucket and make them readable, run a compaction job.
See `compact` [procedure](../flink/procedures).
The bucket number for partitions compacted for the first time can be configured by the option
`postpone.default-bucket-num`. Its value is used exactly and takes precedence over automatic
estimation. Otherwise, `postpone.target-row-num-per-bucket`, when configured, calculates the
bucket number as `ceil(row_count / target_row_count)`. If it is not configured, Paimon calculates
the bucket number as `ceil(postpone_file_size / postpone.target-size-per-bucket)`; the target size
defaults to `1 GB`. Both estimates are at least `1`. Execution parallelism does not determine the
logical bucket number.
Partitions that already have real bucket files keep their existing bucket number.

Finally, when you feel that the bucket number of some partition is too small,
you can also run a rescale job.
See `rescale` [procedure](../flink/procedures).

## Cross Partitions Upsert

An update crosses partitions when a key's partition value changes and the primary key does not
include all partition fields. Dynamic buckets (`bucket = -1`) maintain a mapping from each key
to its partition and bucket, using local disk. Starting a streaming writer initializes this index
by reading existing keys.

| Merge engine | Existing key arrives with a different partition value |
| --- | --- |
| `deduplicate` | Delete the row in the old partition and insert it in the new partition |
| `partial-update` or `aggregation` | Apply the update in the old partition |
| `first-row` | Keep the existing row and ignore the incoming row |

The index can add substantial startup and storage costs for large tables. If updates only need
to match recent data, `cross-partition-upsert.index-ttl` limits the history retained in the index
and considered during initialization. Choose a TTL that covers the required update history;
expiring an entry means it can no longer locate an older row through that index.

Fixed and postpone buckets do not maintain this cross-partition mapping. Their inputs must
supply a complete changelog, including the retraction of the old partition's row, to preserve
uniqueness across partition moves.

## Pick Partition Fields

| Candidate | Guidance |
| --- | --- |
| Immutable creation time | Usually a good fit: it can be included in the primary key without moving an entity between partitions |
| Mutable event time | Moving an entity requires a before image for the old partition and an after image for the new one, or a cross-partition index |
| CDC operation timestamp (`op_ts`) | Usually a poor entity partition key: each change has a new timestamp, which may not identify the old row's partition |

For complete CDC input, a primary key containing the partition field can retract the old
partition's row and insert the new one. Use `changelog-producer = input` when streaming readers
must receive those input before/after records; this producer does not create a missing before
image. See [Changelog Producer](./changelog-producer#input).
