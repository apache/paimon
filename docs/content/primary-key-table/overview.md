---
title: "Overview"
weight: 1
type: docs
aliases:
- /primary-key-table/overview.html
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
on the primary key. See [CREATE TABLE]({{< ref "flink/sql-ddl#create-table" >}}).

## Bucket

Unpartitioned tables, or partitions in partitioned tables, are sub-divided into buckets, to provide extra structure to the data that may be used for more efficient querying.

Each bucket directory contains an LSM tree and its [changelog files]({{< ref "primary-key-table/changelog-producer" >}}).

The range for a bucket is determined by the hash value of one or more columns in the records. Users can specify bucketing columns by providing the [`bucket-key` option]({{< ref "maintenance/configurations#coreoptions" >}}). If no `bucket-key` option is specified, the primary key (if defined) or the complete record will be used as the bucket key.

A bucket is the smallest storage unit for reads and writes, so the number of buckets limits the maximum processing parallelism. This number should not be too big, though, as it will result in lots of small files and low read performance. In general, the recommended data size in each bucket is about 200MB - 1GB.

Also, see [rescale bucket]({{< ref "maintenance/rescale-bucket" >}}) if you want to adjust the number of buckets after a table is created.

## LSM Trees

Paimon adapts the LSM tree (log-structured merge-tree) as the data structure for file storage. This documentation briefly introduces the concepts about LSM trees.

### Sorted Runs

LSM tree organizes files into several sorted runs. A sorted run consists of one or multiple data files and each data file belongs to exactly one sorted run.

Records within a data file are sorted by their primary keys. Within a sorted run, ranges of primary keys of data files never overlap.

{{< img src="/img/sorted-runs.png">}}

As you can see, different sorted runs may have overlapping primary key ranges, and may even contain the same primary key. When querying the LSM tree, all sorted runs must be combined and all records with the same primary key must be merged according to the user-specified [merge engine]({{< ref "primary-key-table/merge-engine" >}}) and the timestamp of each record.

New records written into the LSM tree will be first buffered in memory. When the memory buffer is full, all records in memory will be sorted and flushed to disk. A new sorted run is now created.
