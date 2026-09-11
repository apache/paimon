---
title: "Concepts"
sidebar_position: 1
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

Apache Paimon is a table format for data lakes that supports batch and streaming workloads.
Compute engines read and write Paimon tables, while table data and file metadata live in a
filesystem or object store. A catalog provides names and metadata operations for those tables.

[![Compute engines use a catalog to discover Paimon tables and read or write their snapshots and data in shared storage.](/img/concepts-architecture.svg)](/img/concepts-architecture.svg)

## Start Here

Read the core concepts in this order:

1. [Basic Concepts](./basic-concepts): choose a table type, understand partitions and buckets,
   and follow a read from snapshot to data files.
2. [Concurrency Control](./concurrency-control): understand how snapshots become visible and
   what happens when writers compete.
3. [Catalog](./catalog): choose how engines discover and manage tables.

To run your first table, use the [Flink quick start](../flink/quick-start),
[Spark quick start](../spark/quick-start), or [Python API](../pypaimon/python-api).

## Unified Storage

A Paimon table can serve several access patterns. The available operations depend on the table
type, engine, and connector.

| Access pattern | What the reader or writer does | Learn more |
| --- | --- | --- |
| Batch reads | Query the latest table state or a retained historical snapshot. | [Flink queries](../flink/sql-query), [Spark queries](../spark/sql-query) |
| Streaming reads | Discover new snapshots and consume appended records or changes, according to the table's changelog configuration. | [Append streaming](../append-table/#append-streaming), [Changelog producers](../primary-key-table/changelog-producer) |
| Streaming writes | Ingest new events or apply database changes to a primary-key table. | [CDC ingestion](../cdc-ingestion/) |
| Batch writes | Insert records, overwrite data, or use the row-level operations supported by the engine and table. | [Flink writes](../flink/sql-write), [Spark writes](../spark/sql-write) |

Historical snapshots and changelogs are subject to retention. Configure
[snapshot expiration](../maintenance/manage-snapshots#expire-snapshots) and, when needed,
[tags](../maintenance/manage-tags) to match your recovery and time-travel requirements.

## Explore by Topic

| Topic | Pages |
| --- | --- |
| Table design | [Append tables](../append-table/), [Primary-key tables](../primary-key-table/), [Multimodal tables](../multimodal-table/) |
| Catalog and metadata | [Catalog](./catalog), [Data types](./data-types), [Views](./views), [Functions](./functions) |
| Inspect a table | [System tables](./system-tables) for snapshots, files, partitions, indexes, and configuration |
| REST Catalog | [Architecture and setup](./rest/), authentication, table types, and API references |
| Storage specification | [On-disk layout](./spec/), schemas, snapshots, manifests, data files, and indexes |

The [storage specification](./spec/) is a reference for readers implementing or inspecting the
format. Start with [Basic Concepts](./basic-concepts) for the relationships between its parts.
