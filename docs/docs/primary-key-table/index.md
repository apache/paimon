---
title: "PrimaryKey Table"
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

# Overview

If you define a table with primary key, you can insert, update or delete records in the table.

Primary keys consist of a set of columns that contain unique values for each record. Paimon enforces data ordering by
sorting the primary key within each bucket, allowing users to achieve high performance by applying filtering conditions
on the primary key. See [CREATE TABLE](../flink/sql-ddl#create-table).

## Nullable Primary Keys

Primary key fields are `NOT NULL` by default. Set `primary-key.nullable` to `true` when a source
system can produce null key components:

```sql
CREATE TABLE orders (
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

## Bucket

Unpartitioned tables, or partitions in partitioned tables, are sub-divided into buckets, to provide extra structure to the data that may be used for more efficient querying.

Each bucket directory contains an LSM tree and its [changelog files](./changelog-producer).

The range for a bucket is determined by the hash value of one or more columns in the records. Users can specify bucketing columns by providing the [`bucket-key` option](../maintenance/configurations#coreoptions). If no `bucket-key` option is specified, the primary key (if defined) or the complete record will be used as the bucket key.

A bucket is the smallest storage unit for reads and writes, so the number of buckets limits the maximum processing parallelism. This number should not be too big, though, as it will result in lots of small files and low read performance. In general, the recommended data size in each bucket is about 200MB - 1GB.

Also, see [rescale bucket](../maintenance/rescale-bucket) if you want to adjust the number of buckets after a table is created.

## LSM Trees

Paimon adopts the LSM tree (log-structured merge-tree) as the data structure for file storage. This documentation briefly introduces the concepts about LSM trees.

### Sorted Runs

LSM tree organizes files into several sorted runs. A sorted run consists of one or multiple data files and each data file belongs to exactly one sorted run.

Records within a data file are sorted by their primary keys. Within a sorted run, ranges of primary keys of data files never overlap.

![](/img/sorted-runs.png)

As you can see, different sorted runs may have overlapped primary key ranges, and may even contain the same primary key. When querying the LSM tree, all sorted runs must be combined and all records with the same primary key must be merged according to the user-specified [merge engine](./merge-engine/) and the timestamp of each record.

New records written into the LSM tree will be first buffered in memory. When the memory buffer is full, all records in memory will be sorted and flushed to disk. A new sorted run is now created.
