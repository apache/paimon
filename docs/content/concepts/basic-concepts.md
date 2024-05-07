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

## File Layouts

All files of a table are stored under one base directory. Paimon files are organized in a layered style. The following image illustrates the file layout. Starting from a snapshot file, Paimon readers can recursively access all records from the table.

{{< img src="/img/file-layout.png">}}

## Snapshot

All snapshot files are stored in the `snapshot` directory.

A snapshot file is a JSON file containing information about this snapshot, including

* the schema file in use
* the manifest list containing all changes of this snapshot

A snapshot captures the state of a table at some point in time. Users can access the latest data of a table through the
latest snapshot. By time traveling, users can also access the previous state of a table through an earlier snapshot.

## Manifest Files

All manifest lists and manifest files are stored in the `manifest` directory.

A manifest list is a list of manifest file names.

A manifest file is a file containing changes about LSM data files and changelog files. For example, which LSM data file is created and which file is deleted in the corresponding snapshot.

## Data Files

Data files are grouped by partitions. Currently, Paimon supports using orc (default), parquet and avro as data file's format.

## Partition

Paimon adopts the same partitioning concept as Apache Hive to separate data.

Partitioning is an optional way of dividing a table into related parts based on the values of particular columns like date, city, and department. Each table can have one or more partition keys to identify a particular partition.

By partitioning, users can efficiently operate on a slice of records in the table.

## Consistency Guarantees

Paimon writers use two-phase commit protocol to atomically commit a batch of records to the table. Each commit produces
at most two [snapshots]({{< ref "concepts/basic-concepts#snapshot" >}}) at commit time. It depends on the incremental write and compaction strategy. If only incremental writes are performed without triggering a compaction operation, only an incremental snapshot will be created. If a compaction operation is triggered, an incremental snapshot and a compacted snapshot will be created.

For any two writers modifying a table at the same time, as long as they do not modify the same partition, their commits 
can occur in parallel. If they modify the same partition, only snapshot isolation is guaranteed. That is, the final table 
state may be a mix of the two commits, but no changes are lost.
See [dedicated compaction job]({{< ref "maintenance/dedicated-compaction#dedicated-compaction-job" >}}) for more info.
