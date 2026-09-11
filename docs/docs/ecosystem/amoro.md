---
title: "Amoro"
sidebar_position: 6
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

# Amoro

Use Apache Amoro to inspect Paimon tables through Amoro Management Service (AMS). Register an
existing Paimon catalog so operators can browse table metadata in one place.

## Supported Workflows

The [Amoro 0.8.1 Paimon guide](https://amoro.apache.org/docs/0.8.1/paimon-format/) describes table
metadata inspection and Spark SQL access through the Terminal. Check the Paimon guide for your
deployed Amoro version before configuring the integration.

![Register a Paimon catalog with AMS to browse table metadata. Terminal SQL runs in Spark, locally inside AMS or through Kyuubi.](/img/amoro-paimon.svg)

| Task | Where it runs |
| --- | --- |
| Browse schemas, options, files, snapshots, DDL, and compaction information | AMS table views |
| Query or operate on tables with SQL | Amoro Terminal, using a configured Spark runtime |
| Ingest data | A writer such as Flink or Spark |

## Connect a Paimon Catalog

1. Deploy AMS using the [Amoro deployment guide](https://amoro.apache.org/docs/0.8.1/deployment/).
2. Prepare the Paimon catalog settings and make its metastore and storage accessible to AMS.
   See [Connecting Engines](./connecting-engines) for the information to collect.
3. Install the filesystem dependencies required by the Amoro distribution. For S3 or OSS,
   the Paimon guide describes adding the corresponding jars to Amoro's `lib` directory;
   match their versions to the Paimon dependency used by that distribution.
4. Register the catalog using [Managing Catalogs](https://amoro.apache.org/docs/0.8.1/managing-catalogs/).
5. Open an existing table and check its schema, options, files, and snapshot history.

Use the catalog and filesystem implementations supported by the Paimon integration in that
Amoro release. Catalog support for another table format does not establish Paimon support.

## Query with the Terminal

The Terminal can run Spark locally inside AMS or use a configured Kyuubi service. Verify the
execution backend's Paimon connector, catalog settings, and storage access, especially when
using an external backend. See the [Amoro Terminal guide](https://amoro.apache.org/docs/0.8.1/using-kyuubi/)
and [Paimon Spark Quick Start](../spark/quick-start).

## Table Maintenance

The ability to display compaction information does not establish support for running Paimon
self-optimization. Verify maintenance support in your deployed Amoro version before assigning
it table maintenance work.

For Paimon-managed maintenance, see [Compaction](../primary-key-table/compaction),
[Snapshot Management](../maintenance/manage-snapshots), and [Flink Actions](../flink/action-jars).
