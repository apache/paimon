---
title: "Lake-Stream Mode"
sidebar_position: 11
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

# Lake-Stream Mode

Lake-stream mode keeps a table's history in Paimon and serves its most recent writes from an
external streaming store, so a query can return data that is seconds old instead of waiting for the
next snapshot. Enable it on an existing table when snapshot-level freshness is too slow and you
already run a supported store. The table keeps its name, its schema, and its historical data.

A table is a lake-stream table when its `lakestream.enabled` option is `true`. Paimon then routes
each read and write by table name, and the store's tiering service moves data into Paimon in the
background.

Enabling the mode needs engine-specific catalog configuration, so each engine has its own page.
[Flink](../flink/lake-stream) is supported today, and Spark is not supported yet. Fluss is the first
supported store, and [Use Fluss as the Store](../flink/lake-stream#use-fluss-as-the-store) walks
through it end to end.

## Read a Lake-Stream Table

The name you query decides which data you get, and both names resolve to the same table.

| Name | What the read returns |
| --- | --- |
| `my_table` | Paimon snapshots only. Existing queries keep their current behavior and visibility. |
| `my_table$rt` | Paimon snapshots plus the records the store has not tiered yet. |

Query `my_table$rt` when you need the freshest data, and keep `my_table` for queries that must
reproduce a committed snapshot.

`$rt` is the same table read through the store rather than a second table. Writing to `my_table$rt`
is rejected; write to `my_table` and let the routing above decide where it lands. Querying
`my_table$rt` on a table without `lakestream.enabled` fails as an unknown table, and time travel
does not apply to it. Read a historical snapshot through `my_table` instead.

## Write to a Lake-Stream Table

Regular writes go to the store, and the store's tiering service moves them into Paimon afterwards.
This covers streaming and batch inserts, and `DELETE` and `UPDATE` where the store supports them.

`INSERT OVERWRITE` is not supported on a lake-stream table: every write to such a table is served by
the store, so an overwrite never reaches Paimon.

## Change a Lake-Stream Table

Paimon rejects a table rename while `lakestream.enabled` is `true`. The table name is how Paimon
finds the matching table in the store, so a rename would break that lookup.

Paimon also rejects replacing the table, which is what `CREATE OR REPLACE TABLE` does. Replacing a
table can truncate data the store has not tiered yet, and it can drop the `lakestream.enabled`
option without the restart that [disabling the mode](#disable-lake-stream-mode) requires.

Disable lake-stream mode first if you need to rename or replace the table.

Column changes are allowed, and they update the Paimon table only. The store's tiering service
reconciles its own schema afterwards, so the two schemas can differ briefly. Where you can, make
column changes through the store's catalog to keep the two schemas aligned.

Dropping a lake-stream table removes it from Paimon and leaves the store's table behind. The store
owns that table's lifecycle, so remove it through the store if you no longer need it.

## Disable Lake-Stream Mode

Set the table option back to `false`.

```sql
ALTER TABLE my_table SET ('lakestream.enabled' = 'false');
```

Reads and writes then use Paimon only, and `my_table$rt` stops resolving. Restart running write
pipelines without state so that they plan against the Paimon sink again.

Paimon does not wait for the store to finish tiering. Records that are still only in the store when
you disable the mode do not appear in later Paimon reads, so confirm that tiering has caught up if
you need complete results.
