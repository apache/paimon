---
title: "Troubleshooting"
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

# Troubleshooting

Start with the symptom, then inspect job state and table metadata before changing configuration.
The examples use a Paimon table named `t`; replace it with the affected table.

| Symptom | First check |
| --- | --- |
| A writer is running but rows are missing | [Completed checkpoints and committed snapshots](#new-writes-are-not-visible). |
| A query keeps running | [Batch versus streaming mode](#a-query-does-not-finish). |
| A historical read fails or starts unexpectedly | [Retained history and startup state](#historical-data-is-unavailable). |
| An overwrite did not clear a partition | [Dynamic versus static scope](#an-overwrite-did-not-clear-data). |
| Lookup matches are missing | [Dimension rows, keys, partitions, and refresh](#lookup-matches-are-missing). |
| Changing parallelism has no effect | [Explicit source settings and inference](#source-parallelism-is-unexpected). |
| A writer cannot restore from a savepoint | [Matching job and table states](#restoring-a-writer-fails). |

## New Writes Are Not Visible

1. Check the Flink writer for failed or incomplete checkpoints. Streaming writes need
   checkpointing to commit data; configuring the SQL client after submission does not change the running job.
2. Query the table's snapshots in a separate SQL session:

```sql
SET 'execution.runtime-mode' = 'batch';
SELECT snapshot_id, commit_kind, commit_time FROM `t$snapshots`;
```

If new snapshots are present, check the query's catalog, database, partition filters, and
time-travel hints. A session-level dynamic option can also keep a query on an older version.
See [Runtime Configuration](./configuration) and [Snapshots Table](../concepts/system-tables#snapshots-table).

## A Query Does Not Finish

A streaming Paimon source is unbounded by default. To read the current table once, use batch mode:

```sql
SET 'execution.runtime-mode' = 'batch';
SELECT * FROM t;
```

For a streaming query, continued execution is expected. If records stop arriving, inspect the
writer's commits and the reader's filters or startup mode; see [SQL Query](./sql-query).

## Historical Data Is Unavailable

Inspect the [snapshots](../concepts/system-tables#snapshots-table),
[tags](../concepts/system-tables#tags-table), and, when using a consumer ID,
[consumer progress](../concepts/system-tables#consumers-table):

```sql
SET 'execution.runtime-mode' = 'batch';
SELECT * FROM `t$tags`;
SELECT * FROM `t$consumers`;
```

Confirm that the requested snapshot or changelog history is still retained. An expired history
range cannot be recovered by changing scan options. A fresh scan can use stored consumer
progress instead of a new scan hint, and a restored Flink job uses its saved state. See
[Consumer ID](./consumer-id) before resetting a position, and
[snapshot retention](../maintenance/manage-snapshots) to preserve future recovery windows.

## An Overwrite Did Not Clear Data

Dynamic partition overwrite only replaces partitions present in the incoming data. Empty input
therefore leaves the table unchanged. To clear a selected partition, use static overwrite with
an explicit partition specification; to drop it, use `ALTER TABLE DROP PARTITION`. Check the
[overwrite scope examples](./sql-write#dynamic-overwrite) before running either operation.

If the table changed but a streaming reader did not emit the replacement, check
[overwrite consumption](./sql-query#read-overwrite), which is disabled by default.

## Lookup Matches Are Missing

1. Read the dimension table in batch mode and check that the expected row has been committed.
2. Check join-key values and types. For a partitioned dimension, check `scan.partitions` and
   whether the selected `max_pt()` partition contains the row.
3. Check lookup refresh and retry settings. A retry can bridge delayed dimension visibility;
   it does not correct a wrong key or partition filter.

An inner lookup join drops unmatched input rows; a left lookup join returns them with null
dimension fields. See [Lookup Joins](./sql-lookup) for a complete example and
[retry strategies](./sql-lookup#retry-lookup).

## Source Parallelism Is Unexpected

Check `scan.parallelism` first, then the global Flink parallelism and
`scan.infer-parallelism`. Inference is used only when neither explicit source nor global
parallelism is set. Batch estimates and streaming bucket inference have different rules;
see [Read Parallelism](./sql-query#read-parallelism).

For slow planning or JobManager memory pressure on a table with many splits, consider
[Dedicated Split Generation](./sql-query#dedicated-split-generation), including its
checkpoint-compatibility and failover implications.

## Restoring a Writer Fails

Compare the Flink savepoint with the Paimon table state. A savepoint retains job state but does
not automatically undo later table commits. For a tagged recovery point, match the
`savepoint-<checkpoint-id>` tag to the saved job state and follow the
[Savepoint recovery sequence](./savepoint#tag-with-savepoint).

Also check whether source topology or consumer mode changed. Dedicated split generation and
switching consumer modes can make the existing Flink state incompatible.
