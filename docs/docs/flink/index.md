---
title: "Flink"
sidebar_position: 5
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

# Flink

Use Flink to ingest streams into Paimon, query table snapshots, follow changes, and enrich events
with lookup joins. Start with [Quick Start](./quick-start) for a complete local example or
[Installation](./installation) to prepare an existing cluster.

![A Flink writer commits snapshots to Paimon; batch, streaming, and lookup readers consume the table in different ways.](/img/flink-read-write-overview.svg)

Streaming writers commit data as checkpoints complete. Batch readers select a table snapshot;
streaming readers can load a snapshot and then follow changes. Lookup joins use a Paimon table
to enrich records from another stream. The table's layout and changelog configuration determine
which read paths and change semantics are available.

## Find the Right Guide

| What you want to do | Start here | Continue with |
| --- | --- | --- |
| Run your first pipeline | [Quick Start](./quick-start) | [Installation](./installation) |
| Define a catalog, keys, and partitions | [SQL DDL](./sql-ddl) | [SQL Alter](./sql-alter), [Default Value](./default-value) |
| Ingest or modify records | [SQL Write](./sql-write) | [CDC Ingestion](../cdc-ingestion/) |
| Read current or historical data | [SQL Query](./sql-query) | [Snapshots](../maintenance/manage-snapshots), [Tags](../maintenance/manage-tags) |
| Enrich a stream | [Lookup Joins](./sql-lookup) | [Query Service](./sql-lookup#query-service) |
| Tune and operate a job | [Runtime Configuration](./configuration) | [Consumer ID](./consumer-id), [Savepoint](./savepoint), [Data Lineage](./lineage) |
| Run a maintenance operation | [Procedures](./procedures) | [Action Jars](./action-jars) |
| Diagnose unexpected results or recovery failures | [Troubleshooting](./troubleshooting) | [Runtime Configuration](./configuration) |
| Develop with Java | [Flink API](../program-api/flink-api) | [Table Concepts](../concepts/basic-concepts) |

## Choose the Table Semantics

- **[Append tables](../append-table/):** store rows without merging by primary key. Start here for
  append-only events and logs.
- **[Primary-key tables](../primary-key-table/):** merge changes to the same key. Choose the
  [merge engine](../primary-key-table/merge-engine/) and
  [changelog producer](../primary-key-table/changelog-producer) for CDC and upsert workloads.
- **[Data Evolution](../multimodal-table/data-evolution):** use row tracking and column-level
  updates for supported append-table workloads, including multimodal data.

## SQL or Action Jar?

Use SQL for table definitions and read/write queries. On Flink 1.18+, use `CALL` for the
operations listed in [Procedures](./procedures). Named procedure arguments require Flink 1.19+.
Use an [action jar](./action-jars) when submitting the corresponding operation from the command line.
Both interfaces can start Flink jobs, so select the runtime mode and options for the operation.
