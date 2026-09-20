---
title: "Ecosystem"
sidebar_position: 90
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

# Ecosystem

Use Paimon tables across ingestion pipelines, SQL engines, and lakehouse management tools.
Start with an integration below, then follow [Connecting Engines](./connecting-engines) to
configure catalog discovery and storage access.

![Flink and Spark write Paimon tables; SQL engines query the shared tables, while Amoro provides table management.](/img/ecosystem-overview.svg)

## Choose an Integration

| What you want to do | Start here |
| --- | --- |
| Ingest CDC events or build a streaming pipeline | [Flink Quick Start](../flink/quick-start), [CDC Ingestion](../cdc-ingestion/) |
| Run batch transformations or Spark SQL | [Spark Quick Start](../spark/quick-start) |
| Process streams with Spark micro-batches | [Spark Structured Streaming](../spark/structured-streaming) |
| Query Paimon from an OLAP engine | [StarRocks](./starrocks), [Doris](./doris) |
| Query or write tables with distributed SQL | [Trino](./trino) |
| Access tables from Hive | [Hive](./hive) |
| Inspect tables in a lakehouse management service | [Amoro](./amoro) |

## Compatibility Matrix

For bundled connectors, match the engine version to the connector artifact for your Paimon
release. The Flink, Spark, and Hive versions below describe connector modules in this branch;
artifact availability depends on the release. Externally maintained integrations have their own
release cycle, embedded Paimon version, and feature limits.

| Integration | Version selection | Access to Paimon tables |
| --- | --- | --- |
| [Flink](../flink/installation) | 1.16–1.20 and 2.0–2.2 | Batch and streaming reads/writes; [DDL](../flink/sql-ddl) and [row changes](../flink/sql-write) have version-specific requirements. |
| [Spark](../spark/quick-start) | 3.2–3.5, 4.0, and 4.1; match the Scala binary version | Batch reads/writes, [DDL](../spark/sql-ddl), and [row changes](../spark/sql-write); [streaming](../spark/structured-streaming) requires Spark 3.3+. |
| [Hive](./hive#version) | 2.1, 2.2, 2.3, 3.1, and 2.1-cdh-6.3 | Batch reads, table creation, and `INSERT INTO`; writes require MapReduce. |
| [Trino](./trino#version) | Match the independently released Paimon connector to Trino | Batch reads; supported connectors also provide DDL, inserts, and time travel. See the guide's table-layout limits. |
| [Presto](https://github.com/apache/paimon-presto) | Follow the separate connector's version requirements | See the connector repository for installation and supported operations. |
| [StarRocks](./starrocks#version) | Paimon catalogs available from 3.1 | Query existing tables through an external catalog; check the engine release for individual features. |
| [Doris](./doris#version) | Select a release with the required catalog and reader features | Query existing tables through an external catalog; REST catalog access requires Doris 3.1+. |

A connector's ability to read a table also depends on its data types, file format, merge engine,
and enabled features, such as deletion vectors. Check the relevant engine guide before enabling
a new table feature in a warehouse shared by several engines.

## Streaming Engines

Use [Flink](../flink/) for continuous ingestion, change processing, and lookup joins.
Use [Spark Structured Streaming](../spark/structured-streaming) for micro-batch pipelines.
Configure the table's [changelog producer](../primary-key-table/changelog-producer) for the
changes that downstream readers need.

## Batch Engines

Use [Spark SQL](../spark/sql-query) or [Flink batch SQL](../flink/sql-query) to read a snapshot
and run transformations. Consult the write guides for [Spark](../spark/sql-write) and
[Flink](../flink/sql-write) before using overwrite, `DELETE`, `UPDATE`, or `MERGE INTO`:
SQL support and table requirements differ by engine and version.

## OLAP Engines

[StarRocks](./starrocks) and [Doris](./doris) query Paimon through their own external catalogs.
[Trino](./trino) and [Presto](https://github.com/apache/paimon-presto) use separately distributed
connectors. Configure access to both the catalog and the underlying files, and choose the
[table read mode](../primary-key-table/table-mode) for your freshness requirements.

## Download

Use the [engine downloads](../project/download#engine-jars) for Paimon artifacts and each
integration guide for installation. For Trino and Presto, use the connector project's release
instructions; its version does not necessarily match this documentation's Paimon version.
