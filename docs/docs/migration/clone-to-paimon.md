---
title: "Clone to Paimon"
sidebar_position: 2
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

# Clone to Paimon

Clone copies data into separate Paimon tables while keeping source tables and files available.
Use it to validate a Paimon copy before switching workloads, or to refresh selected target
partitions. Unlike [migration](./migration-from-hive), clone does not move source files.

The Flink `clone` action supports Hive and Paimon sources. This guide starts with Hive tables
in ORC, Parquet, or Avro format, which become Paimon [append tables](../append-table/).

## Before You Start

- Install the matching [Flink action jar](../flink/action-jars) and set `FLINK_HOME`.
- Configure source and target catalogs separately. Hive sources require a Hive catalog; the
  examples use a filesystem catalog with a separate warehouse for the target.
- Ensure the job can read the source and write the target. Keep source data stable during the
  copy when you need a consistent validation baseline.
- Use distinct source and target locations. For an existing Hive-clone target, use an append
  table with `bucket = -1`, compatible file format, all source fields with compatible types,
  and the same partition fields.

## Understand Overwrite Behavior

With the default target option `dynamic-partition-overwrite = true`, clone replaces target
partitions represented in the copied data. Other target partitions remain unchanged. For a
non-partitioned target, the copied data replaces the table contents.

![Cloning source partition dt=2026-09-10 replaces that target partition, keeps dt=2026-09-09, and leaves the Hive source unchanged.](/img/migration-clone-overwrite.svg)

Re-running clone refreshes the copied partitions; it does not append a second copy. It also
does not synchronize source deletions for partitions absent from the copied data. If an
existing target sets `dynamic-partition-overwrite = false`, overwrite can replace the entire
target table even when the source is filtered. Check this option before cloning a subset.

## Clone Hive Table

The following example copies `default.hivetable` to `analytics.hivetable_copy`:

```bash
"$FLINK_HOME/bin/flink" run /path/to/paimon-flink-action-@@VERSION@@.jar \
    clone \
    --clone_from hive \
    --database default \
    --table hivetable \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://localhost:9083 \
    --target_database analytics \
    --target_table hivetable_copy \
    --target_catalog_conf warehouse=hdfs:///paimon-warehouse \
    --parallelism 10
```

To copy only selected partitions, add a quoted partition predicate. For a source partitioned
by the string column `dt`, append this argument to the command:

```bash
--where "dt = '2026-09-10'"
```

`--where` selects partitions, not arbitrary rows within a partition. Omit it to copy all
partitions. Table include/exclude lists are not accepted when cloning a single table.

## Clone Hive Database

Omit `--table` and supply `--target_database`. Source table names are preserved in the target
database, which is created if needed.

```bash
"$FLINK_HOME/bin/flink" run /path/to/paimon-flink-action-@@VERSION@@.jar \
    clone \
    --clone_from hive \
    --database default \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://localhost:9083 \
    --target_database analytics \
    --target_catalog_conf warehouse=hdfs:///paimon-warehouse \
    --parallelism 10 \
    --included_tables default.orders,default.customers
```

Omit `--included_tables` to consider all tables in the source database. Use
`--excluded_tables` to remove tables from that selection.

## Clone Hive Catalog

Omit source and target database/table arguments to preserve database and table names across
the catalog. The following example limits the selection to two source tables:

```bash
"$FLINK_HOME/bin/flink" run /path/to/paimon-flink-action-@@VERSION@@.jar \
    clone \
    --clone_from hive \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://localhost:9083 \
    --target_catalog_conf warehouse=hdfs:///paimon-warehouse \
    --parallelism 10 \
    --included_tables sales.orders,crm.customers
```

## Selection and Retry Options

| Option | Behavior |
| --- | --- |
| `--included_tables db.table,db.other` | Selects fully qualified source table names for database or catalog cloning. Omit to consider all tables in scope. |
| `--excluded_tables db.table,db.other` | Removes tables from the selection. Exclusion wins when a table is also included. |
| `--where "dt = '2026-09-10'"` | Restricts copied data using a partition predicate. |
| `--clone_if_exists false` | Skips targets that already exist. The default is `true`, which clones into existing compatible targets. |
| `--meta_only true` | Clones the schema without copying data. The default is `false`. |
| `--parallelism 10` | Sets the clone job parallelism. |

A multi-table clone does not commit all tables atomically. After a failure, inspect target
tables and rerun the required scope. `--clone_if_exists false` is useful for skipping existing
tables, but is not a resume mechanism: an existing table may have been created before its
data was copied.

## Clone a Paimon Source

Use `--clone_from paimon` and configure the source Paimon catalog. For example, to copy between
two filesystem catalogs:

```bash
"$FLINK_HOME/bin/flink" run /path/to/paimon-flink-action-@@VERSION@@.jar \
    clone \
    --clone_from paimon \
    --catalog_conf warehouse=hdfs:///source-paimon-warehouse \
    --database sales \
    --table orders \
    --target_catalog_conf warehouse=hdfs:///target-paimon-warehouse \
    --target_database analytics \
    --target_table orders_copy \
    --parallelism 10
```

Paimon sources can retain primary keys in the newly created target. The Hive append-table
restriction above applies to Hive sources. For Paimon sources, repeated
`--target_table_conf key=value` arguments can override options when creating the target;
these overrides are not supported for Hive sources.

## Verify the Clone

1. Confirm that the Flink job completed and that every selected target exists.
2. Connect an engine to the target catalog and inspect the schema and partition keys.
3. Compare row counts and aggregates with the stable source data, using the same partition
   predicate when cloning a subset. Confirm that unrelated target partitions remain as expected.
4. Verify that source readers still work, then switch workloads when the target is ready.

For SQL invocation instead of the action jar, see the [Flink `clone` procedure](../flink/procedures/table-operations#clone).
