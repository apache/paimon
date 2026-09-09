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

A bucket is the smallest storage unit for reads and writes, each bucket directory contains an [LSM tree](./#lsm-trees).

## Fixed Bucket

Configure a bucket greater than 0, using Fixed Bucket mode, according to `Math.abs(key_hashcode % numBuckets)` to compute
the bucket of record.

Rescaling buckets can only be done through offline processes, see [Rescale Bucket](../maintenance/rescale-bucket).
A too large number of buckets leads to too many small files, and a too small number of buckets leads to poor write performance.

For partitioned tables, each partition can have its own bucket count when
`'bucket.per-partition-count-enabled' = 'true'` is set. In that case, after a rescale operation existing
partitions retain their original bucket count while newly created partitions use the updated table-level
default. When the option is disabled (the default), all partitions share the single table-level bucket count.

## Dynamic Bucket

Default mode for primary key table, or configure `'bucket' = '-1'`.

The keys that arrive first will fall into the old buckets, and the new keys will fall into the new buckets, the
distribution of buckets and keys depends on the order in which the data arrives. Paimon maintains an index to determine
which key corresponds to which bucket.

Paimon will automatically expand the number of buckets.

- Option1: `'dynamic-bucket.target-row-num'`: controls the target row number for one bucket.
- Option2: `'dynamic-bucket.initial-buckets'`: controls the number of initialized bucket.
- Option3: `'dynamic-bucket.max-buckets'`: controls the number of max buckets.

:::info

Dynamic Bucket only support single write job. Please do not start multiple jobs to write to the same partition
(this can lead to duplicate data). Even if you enable `'write-only'` and start a dedicated compaction job, it won't work.

:::

When your updates do not cross partitions (no partitions, or primary keys contain all partition fields), Dynamic
Bucket mode uses HASH index to maintain mapping from key to bucket, it requires more memory than fixed bucket mode.

Performance:

1. Generally speaking, there is no performance loss, but there will be some additional memory consumption, **100 million**
   entries in a partition takes up **1 GB** more memory, partitions that are no longer active do not take up memory.
2. For tables with low update rates, this mode is recommended to significantly improve performance.

## Postpone Bucket

Postpone bucket mode is configured by `'bucket' = '-2'`.
This mode aims to solve the difficulty to determine a fixed number of buckets
and support different buckets for different partitions.

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

When you need cross partition upsert (primary keys not contain all partition fields), recommend using the '-1' bucket.
Key Dynamic mode directly maintains the mapping of keys to partition and bucket, uses local disks, and initializes 
indexes by reading all existing keys in the table when starting stream write job. Different merge engines have different behaviors:

1. Deduplicate: Delete data from the old partition and insert new data into the new partition.
2. PartialUpdate & Aggregation: Insert new data into the old partition.
3. FirstRow: Ignore new data if there is old value.

Performance: For tables with a large amount of data, there will be a significant loss in performance. Moreover,
initialization takes a long time.

If your upsert does not rely on too old data, you can consider configuring index TTL to reduce Index and initialization time:
- `'cross-partition-upsert.index-ttl'`: The TTL in local index and initialization, this can avoid maintaining too many
  indexes and lead to worse and worse performance.

You can also use Cross Partitions Upsert with bucket (N > 0) or bucket (-2), in these modes, there is no global index to
ensure that your data undergoes reasonable deduplication, so relying on your input to have a complete changelog can
ensure the uniqueness of the data.

## Pick Partition Fields

The following three types of fields may be defined as partition fields in the warehouse:

- Creation Time (Recommended): The creation time is generally immutable, so you can confidently treat it as a partition field
  and add it to the primary key.
- Event Time: Event time is a field in the original table. For CDC data, such as tables synchronized from MySQL
  CDC or Changelogs generated by Paimon, they are all complete CDC data, including `UPDATE_BEFORE` records, even
  if you declare the primary key containing partition field, you can achieve the unique effect (require `'changelog-producer'='input'`).
- CDC op_ts: It cannot be defined as a partition field, unable to know previous record timestamp. So you need to use cross partition upsert, it will consume more resources.
