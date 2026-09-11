---
title: "Runtime Configuration"
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

# Runtime Configuration

Use SQL hints for options that apply to one statement, session settings for a job or session,
and `ALTER TABLE ... SET` for persisted table options. See
[FlinkConnectorOptions](../maintenance/configurations#flinkconnectoroptions) for the full option reference.

## Checkpointing

Streaming writes need checkpointing to commit data. Configure it before submitting the write job:

```sql
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10 s';
```

A query reads committed snapshots. If newly written records are not visible, check that the
writer has completed a checkpoint. Changing a SQL client setting affects subsequently submitted
jobs; it does not reconfigure an already running job.

## Use Flink Managed Memory

By default, each sink task manages its own heap memory for writer buffers. Enable managed
memory to let Flink allocate a share of the slot's managed memory to the Paimon writer.

| Option | Default | Meaning |
| --- | --- | --- |
| `sink.use-managed-memory-allocator` | `false` | Use Flink managed memory for the writer's merge tree buffers. |
| `sink.managed.writer-buffer-memory` | `256 MiB` | Weight used by Flink to allocate managed memory to the writer. The actual allocation depends on the slot's managed memory and the operators sharing it. |

The writer-buffer setting is a **weight**, not a fixed allocation of that many bytes.

```sql
INSERT INTO paimon_table /*+ OPTIONS(
    'sink.use-managed-memory-allocator' = 'true',
    'sink.managed.writer-buffer-memory' = '256 mb'
) */
SELECT * FROM source_table;
```

## Setting dynamic options

Dynamic options tune a query or a subsequently submitted job without changing the options
stored in the catalog.

| Scope | Syntax | Effect |
| --- | --- | --- |
| One statement | `/*+ OPTIONS('key' = 'value') */` on the table | Applies to that table reference. |
| Session, all tables | `SET 'key' = 'value'` | Supplies a global dynamic option. |
| Session, selected tables | `SET 'paimon.catalog.database.table.key' = 'value'` | Overrides the corresponding global dynamic option for matching tables. |

In a table-scoped key, the catalog, database, or table component can be `*`. Avoid setting
conflicting values in overlapping table patterns. Reset session options after using them so
they do not affect later queries.

```sql
-- Global default for queries in this session.
SET 'scan.timestamp-millis' = '1697018249001';

-- Override it for one table.
SET 'paimon.mycatalog.default.T.scan.timestamp-millis' = '1697018249000';
SELECT * FROM mycatalog.default.T;

RESET 'paimon.mycatalog.default.T.scan.timestamp-millis';
RESET 'scan.timestamp-millis';

-- Match default.T in any catalog.
SET 'paimon.*.default.T.scan.timestamp-millis' = '1697018249000';
SELECT * FROM mycatalog.default.T;
RESET 'paimon.*.default.T.scan.timestamp-millis';
```

For symptom-based checks, start with [Troubleshooting](./troubleshooting).

## Related Tuning

| Concern | Guide |
| --- | --- |
| Source parallelism and split planning | [SQL Query](./sql-query#read-parallelism) |
| Writer buffers, parallelism, and compaction | [Write Performance](../maintenance/write-performance) |
| Missing lookup matches or a large dimension table | [Lookup Joins](./sql-lookup) |
| Progress retention and restarting readers | [Consumer ID](./consumer-id) |
| Upgrading or restoring a writer | [Savepoint](./savepoint) |
