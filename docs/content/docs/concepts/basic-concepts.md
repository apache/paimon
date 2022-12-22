---
title: "Basic Concepts"
weight: 2
type: docs
aliases:
- /concepts/basic-concepts.html
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

# Basic Concepts

## Snapshot

A snapshot captures the state of a table at some point in time. Users can access the latest data of a table through the latest snapshot. By time traveling, users can also access the previous state of a table through an earlier snapshot.

## Partition

Table Store adopts the same partitioning concept as Apache Hive to separate data.

Partitioning is an optional way of dividing a table into related parts based on the values of particular columns like date, city, and department. Each table can have one or more partition keys to identify a particular partition.

By partitioning, users can efficiently operate on a slice of records in the table. See [file layouts]({{< ref "docs/concepts/file-layouts" >}}) for how files are divided into multiple partitions.

{{< hint info >}}

Partition keys must be a subset of primary keys if primary keys are defined.

{{< /hint >}}

## Bucket

Unpartitioned tables, or partitions in partitioned tables, are sub-divided into buckets, to provide extra structure to the data that may be used for more efficient querying.

The range for a bucket is determined by the hash value of one or more columns in the records. Users can specify bucketing columns by providing the [`bucket-key` option]({{< ref "docs/maintenance/configurations#coreoptions" >}}). If no `bucket-key` option is specified, the primary key (if defined) or the complete record will be used as the bucket key.

A bucket is the smallest storage unit for reads and writes, so the number of buckets limits the maximum processing parallelism. This number should not be too big, though, as it will result in lots of small files and low read performance. In general, the recommended data size in each bucket is about 1GB.

See [file layouts]({{< ref "docs/concepts/file-layouts" >}}) for how files are divided into buckets. Also, see [rescale bucket]({{< ref "docs/maintenance/rescale-bucket" >}}) if you want to adjust the number of buckets after a table is created.
