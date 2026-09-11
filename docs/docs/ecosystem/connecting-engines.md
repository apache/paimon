---
title: "Connecting Engines"
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

# Connecting Engines

An engine needs to discover a Paimon table and read its files. Use the same catalog backend
and table location as the writer, then configure storage access in the engine that runs the query.
The SQL catalog name is local to that engine and can differ between engines.

![An engine resolves a table through a catalog, then reads Paimon metadata and data files from shared storage. Catalog and storage access are configured separately.](/img/ecosystem-catalog-access.svg)

## Identify the Catalog and Storage

Collect these settings from the job or engine that created the table:

| Setting | What to check |
| --- | --- |
| Catalog backend | Filesystem, Hive metastore, REST, or another backend supported by the reader. |
| Catalog endpoint | The metastore or service URI, with the authentication needed to connect. A filesystem catalog has no separate metastore service. |
| Warehouse or table location | The shared storage URI. A filesystem catalog uses the warehouse root; a Hive external table points to one table directory. REST catalogs can use a warehouse identifier. |
| Storage access | Filesystem libraries, endpoint configuration, and credentials available to the engine processes that access the files. |
| Table features | Primary keys, bucket mode, file format, types, and features that the reader must understand. |

See [Catalog](../concepts/catalog) for Paimon's catalog backends and
[Filesystems](../maintenance/filesystems) for Paimon storage dependencies. External engines
can use their own filesystem implementations and configuration names.

## Configure the Reader

1. Install the connector or enable the engine's built-in Paimon integration.
2. Configure a catalog using the engine's own property names.
3. Configure access to the catalog service and the warehouse storage on the relevant nodes.
4. Query an existing table by its fully qualified name before adding optional read settings.

Catalog properties are not interchangeable. For example, a Hive-backed Paimon catalog uses
`metastore = hive` in Flink, while StarRocks and Doris use `paimon.catalog.type = hive` and
`paimon.catalog.type = hms`, respectively. Follow the matching guide:
[Hive](./hive#installation), [Trino](./trino#configure-paimon-catalog),
[StarRocks](./starrocks#create-paimon-catalog), or [Doris](./doris#create-paimon-catalog).

For a local experiment, a `file:` warehouse is sufficient when all processes share that path.
For a distributed cluster, use shared storage that every participating process can access.

## Check the Read Semantics

Query a small, known table first. Compare values as well as row counts, especially for
primary-key tables with updates and deletes.

| Read path | What the result represents |
| --- | --- |
| Ordinary batch query | A table snapshot, subject to the engine's metadata caching and supported read mode. |
| Time travel | A selected historical snapshot or tag; syntax and support depend on the engine. The referenced state must still be retained. |
| Read-optimized query | Compacted data from a primary-key table; recent changes may be absent until full compaction completes. |
| Streaming query | A snapshot and/or subsequent changes, according to the engine and scan configuration. |

See [Table Mode](../primary-key-table/table-mode),
[System Tables](../concepts/system-tables), and
[Snapshot Retention](../maintenance/manage-snapshots) for the underlying behavior.

## Troubleshooting

| Symptom | Check first |
| --- | --- |
| Catalog exists but the table is missing | Confirm the backend, warehouse, database, and table registration match the writer. |
| Tables can be listed but a query cannot open files | Check storage endpoints, credentials, and filesystem dependencies on the nodes performing the read. |
| Class-loading or method-not-found errors | Check the engine/connector version pair and remove conflicting connector jars. |
| New data or schema is not visible | Check whether the writer committed a snapshot, then inspect engine metadata caches and any time-travel or read-optimized settings. |
| Updates or deletes produce unexpected rows | Confirm support for the table's merge engine and deletion vectors; use a Paimon-aware reader rather than scanning the underlying Parquet or ORC files directly. |

Continue with the engine guide for exact SQL and version-specific limitations.
