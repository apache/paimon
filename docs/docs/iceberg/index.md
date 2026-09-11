---
title: "Iceberg Compatibility"
sidebar_position: 98
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

# Iceberg Compatibility

Paimon can publish Iceberg metadata that points to its existing data files. This lets applications
query a Paimon table through an Iceberg connector while Paimon continues to manage writes,
compaction, and data retention.

![Paimon commits publish Iceberg metadata; both readers access the same data files.](/img/iceberg-publication.svg)

## Start Here

| Task | Guide |
| --- | --- |
| Read your first Paimon table through Flink or Spark's Iceberg connector | [Append tables](./append-table.mdx) |
| Read updates and deletes from a primary key table | [Primary key tables](./primary-key-table.mdx) |
| Choose Hadoop, Hive, REST, or path-based access | [Catalogs and metadata layout](./catalogs.md) |
| Query a named historical snapshot | [Tags](./iceberg-tags.md) |
| Connect Trino, Athena, or DuckDB | [Query engines](./ecosystem.mdx) |
| Check column types and format requirements | [Data types](./data-types.md) |
| Look up table options | [Configuration reference](./configurations.mdx) |

## How Publication Works

1. A writer commits a Paimon snapshot.
2. Paimon generates Iceberg manifests and snapshot metadata for the files eligible for Iceberg reads.
3. With Hive or REST storage, Paimon also publishes the metadata to the external catalog.
4. An Iceberg reader loads the published metadata and reads the referenced data files directly.

Enable publication with the Paimon table option `metadata.iceberg.storage`; its default is `disabled`.
For a first example, use `hadoop-catalog`. The Iceberg warehouse is then
`<paimon-warehouse>/iceberg`, using the default metadata layout.

```sql
'metadata.iceberg.storage' = 'hadoop-catalog'
```

Metadata publication does not copy the table's data. Iceberg readers therefore need access to both
the metadata location and the original Paimon data files, including the required filesystem
configuration and credentials.

## What Iceberg Readers See

| Paimon table | Files eligible for incremental publication | When changes become visible |
| --- | --- | --- |
| Append table | Data files in the committed snapshot | After metadata publication and reader refresh |
| Primary key table without Iceberg deletion vectors | Files at the highest LSM level | After full compaction, metadata publication, and reader refresh |
| Primary key table with Iceberg v3 deletion vectors | Files above L0, together with deletion vectors | After changes reach those files and metadata is published and refreshed |

Initial publication and metadata rebuilds use snapshot splits that can be read directly without
Paimon's merge logic. The table above describes subsequent incremental publication. Use compaction
to establish a predictable visibility boundary for primary key tables.

The [primary key guide](./primary-key-table.mdx) explains both modes and their configuration.
Disabling an Iceberg catalog's cache can help with interactive verification, but cannot make
uncompacted or unpublished changes visible.

:::caution Manage the table through Paimon

Use the Iceberg representation for reads. Perform writes, schema changes, compaction, snapshot
expiration, and file cleanup through Paimon. Both representations refer to shared data files;
independent Iceberg mutations or cleanup can invalidate Paimon's view of the table.

:::

## Supported Types

Compatibility depends on the column types, data file format, Iceberg format version, and reader.
See [supported data types and precision limits](./data-types.md) before enabling publication on an
existing table. Primary key deletion vectors and geospatial columns require Iceberg format v3.
