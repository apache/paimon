---
title: "Catalogs and Metadata Layout"
sidebar_position: 4
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

# Catalogs and Metadata Layout

Choose how readers discover the Iceberg table, then choose where Paimon writes its Iceberg metadata.
These are separate decisions: `metadata.iceberg.storage` selects the publication mode, while
`metadata.iceberg.storage-location` controls the filesystem layout.

## Choose a Publication Mode

| `metadata.iceberg.storage` | Reader access | Default metadata location | Additional setup |
| --- | --- | --- | --- |
| `disabled` | No Iceberg representation | None | Default setting |
| `table-location` | Load an individual table by path | Under the Paimon table | Iceberg reader with path-based access |
| `hadoop-catalog` | Iceberg Hadoop catalog | Separate Iceberg warehouse | [Flink or Spark catalog](./append-table.mdx#prepare-catalogs) |
| `hive-catalog` | Iceberg Hive catalog | Separate Iceberg warehouse | [Hive metastore and client](./hive-catalog.md) |
| `rest-catalog` | Iceberg REST catalog | Separate Iceberg warehouse, plus REST publication | [REST endpoint and Paimon Iceberg JAR](./rest-catalog.mdx) |

Choose `hadoop-catalog` for the walkthroughs. Use Hive or REST when your readers discover tables
through those services. Use `table-location` for an individual table that readers load by path.

## Metadata Layout

For a Paimon table at `<warehouse>/default.db/cities`, the two layouts are:

| `metadata.iceberg.storage-location` | Iceberg metadata directory |
| --- | --- |
| `catalog-location` | `<warehouse>/iceberg/default/cities/metadata` |
| `table-location` | `<warehouse>/default.db/cities/metadata` |

![Separate catalog metadata and metadata stored alongside a Paimon table both reference the original data files.](/img/iceberg-metadata-layout.svg)

The override is optional. By default, `table-location` storage uses the table layout; Hadoop, Hive,
and REST storage use the separate catalog layout. Changing the layout does not move Paimon's data
files.

The default Hadoop catalog walkthrough points the Iceberg reader at `<warehouse>/iceberg`.
A reader that loads a table directly uses the directory containing `metadata`, for example
`<warehouse>/default.db/cities` with the table layout.

### Databases with Custom Locations

The separate catalog layout derives its path from a Paimon database directory ending in `.db`.
For a database with a nonstandard location, store metadata alongside the table instead. For example,
combine Hive registration with the table layout:

```sql
'metadata.iceberg.storage' = 'hive-catalog',
'metadata.iceberg.storage-location' = 'table-location',
'metadata.iceberg.uri' = 'thrift://<metastore-host>:9083'
```

Use the registered Hive table to discover this location; the default
`<warehouse>/iceberg` Hadoop catalog path no longer describes this table's metadata.

## Keep Writer and Reader Settings Aligned

Paimon options configure publication. Iceberg connector options configure reading; they use
different names even when they describe the same service.

| Setting | Paimon table option | Iceberg reader setting |
| --- | --- | --- |
| Hive metastore | `metadata.iceberg.uri` | Hive catalog `uri` (Flink/Spark) or `hive.metastore.uri` (Trino) |
| REST endpoint | `metadata.iceberg.rest.uri` | REST catalog `uri` |
| REST warehouse | `metadata.iceberg.rest.warehouse` | REST catalog `warehouse` |
| Hadoop warehouse, with default layout | Derived from the Paimon table location | `<paimon-warehouse>/iceberg` |

See [Hive Catalog](./hive-catalog.md) and [REST Catalog](./rest-catalog.mdx) for complete examples,
and [configuration reference](./configurations.mdx) for the available Paimon options.
