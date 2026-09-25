---
title: "Migration"
sidebar_position: 97
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

# Migration

Choose a migration path based on whether you need to replace a Hive table, keep a separate
copy, or expose historical Paimon data to existing Hive queries.

![Migration moves Hive files into Paimon, clone copies data while keeping the source, and tag-to-partition exposes Paimon snapshots as Hive partitions.](/img/migration-strategies.svg)

## Choose a Guide

| Goal | Guide | Effect on the source | Result |
| --- | --- | --- | --- |
| Convert a Hive table or database to Paimon | [Migrate from Hive](./migration-from-hive) | Moves data files; removes the original Hive table by default | Paimon append tables in a Hive catalog |
| Copy tables while keeping the source available | [Clone to Paimon](./clone-to-paimon) | Keeps source tables and data | Separate Paimon tables; Hive sources become append tables |
| Query daily views of an updating Paimon table using Hive partition filters | [Expose Tags as Hive Partitions](./upsert-to-partitioned) | Keeps the Paimon table and its write path | Hive partition values that select tags or preview snapshots |

Migration and clone import existing data. Tag-to-partition changes how Hive reads a Paimon
table; it does not migrate Hive files or physically repartition the Paimon table.

## Before Moving Data

1. **Choose the target table model.** Hive migration and Hive clone create
   [append tables](../append-table/). If the target needs primary-key updates or a different
   schema or partition layout, plan a data rewrite into a table with that model.
2. **Prepare the runtime and catalogs.** Configure access to the Hive metastore and storage.
   Use the engine setup and connector requirements linked from each guide.
3. **Define the validation scope.** Record source schemas, partitions, row counts, and key
   aggregates before the operation. For a consistent comparison, keep source data stable
   while it is being moved or copied.
4. **Plan the switch.** In-place migration is not atomic and requires a backup. Clone lets
   you validate a separate target before switching readers and writers.

## Related Guides

- [Flink Procedures](../flink/procedures/table-operations#migrate_table) and
  [Spark Migration Procedures](../spark/procedures/migration): engine-specific arguments.
- [COPY INTO](../spark/copy-into): import files with Spark SQL.
- [Manage Tags](../maintenance/manage-tags): create and retain historical views.
- [Hive](../ecosystem/hive): install the connector used to query Paimon from Hive.
