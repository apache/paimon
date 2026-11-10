---
title: "Lake-Stream Mode"
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

Lake-stream mode keeps table history in Paimon and serves recent data through an external
Lakestream Store. This lets Flink read fresh data between Paimon snapshot commits. The store's
tiering service moves its records into Paimon in the background. Fluss is the first supported store.

## Enable Lake-Stream Mode

A lake-stream table has `lakestream.enabled` set to `true` in Paimon, a corresponding store table
with the same database and table name, and a Paimon catalog configured to use that store.

Create the Paimon catalog with `lakestream.store` and the store's connection options.

```sql
CREATE CATALOG `paimon_catalog` WITH (
    'type' = 'paimon',
    'warehouse' = 'hdfs:///path/to/warehouse',
    'lakestream.store' = 'fluss',
    'fluss.bootstrap.servers' = 'fluss-server:9123'
);
```

`lakestream.store` names the store implementation. Prefix every store connection option with that
identifier, and Paimon removes the prefix before it passes the options on, so
`fluss.bootstrap.servers` reaches Fluss as `bootstrap.servers`. See
[CatalogOptions](../maintenance/configurations#catalogoptions) for the catalog reference.

Set up the store table through the provider's catalog. For Fluss, follow
[Use Fluss as the Store](#use-fluss-as-the-store) to link both layers to the same table.

Stop the pipelines that write to the table before you change either side, and restart them without
state once both sides exist, so that Flink plans them against the store sink. A write that lands
while the store side is missing, or a writer that resumes from state saved against the Paimon sink,
can leave the store and Paimon holding different data for the same keys.

A catalog without `lakestream.store` can still read committed Paimon snapshots by the original
table name. Real-time reads and writes to an enabled table require the store configuration.
To resume direct writes to Paimon, [disable the mode](#disable-lake-stream-mode) first.

Use the Paimon catalog for lake-stream reads and writes. A table declared with `'connector' = 'paimon'`
and a `path` can read snapshots, but Flink rejects `$rt` reads and writes through a path mapping to an
enabled table.

## Write to a Lake-Stream Table

Flink uses the persisted `lakestream.enabled` setting to route batch and streaming inserts to the
store.

```sql
INSERT INTO my_table SELECT * FROM source_table;
```

Flink rejects `INSERT OVERWRITE`, `UPDATE`, and `DELETE` on lake-stream tables. Row-level changes
would select rows from committed Paimon snapshots while writing changes to the store, which may
already hold newer values.

Lake-stream writes use the main branch. A new Paimon branch can inherit `lakestream.enabled`.
To write directly to that branch, disable the option on the branch first.

A Flink CDC pipeline writes directly to Paimon through its own sink. Before using it to update a
lake-stream table, confirm that records for the affected keys have expired from the store. Later
store updates can otherwise use stale values.

## Read a Lake-Stream Table

The original name reads committed Paimon snapshots. The read-only `$rt` alias delegates to the
store, which can combine those snapshots with records it has yet to tier. Use the original name for
time travel.

### Read the Real-Time Name

Quote the alias with backticks in SQL.

```sql
-- Reads only data that Paimon has committed.
SELECT * FROM my_table;

-- Combines committed Paimon data with the store's un-tiered records.
SELECT * FROM `my_table$rt`;
```

Paimon connects to the store on the first lake-stream read or write. An unknown store identifier or
a connection option the store requires fails at that point with a configuration error.

## Change a Lake-Stream Table

Disable lake-stream mode before renaming or replacing the table. The store uses the original table
name to find its corresponding table. Dropping the Paimon table leaves the store table in place.

Keep physical columns, keys, and shared immutable options aligned between Paimon and the store.
Flink rejects a write when they differ or when the statement was planned against an older layout.
Apply supported schema changes through the store's catalog. With Fluss, a schema change made only
in Paimon can cause the tiering job to fail and restart until the two schemas match again.

## Disable Lake-Stream Mode

Stop the write pipelines and let the store finish tiering any records that subsequent Paimon reads
need. Then disable the option and restart the pipelines without state so Flink plans a Paimon sink.

```sql
ALTER TABLE my_table SET ('lakestream.enabled' = 'false');
```

Subsequent reads and writes use Paimon snapshots and sinks. The `$rt` alias is available only while
the table has lake-stream mode enabled.

## Use Fluss as the Store

Use Fluss 1.0 or later. Configure the Fluss cluster and tiering service to use the same Paimon catalog
and warehouse as `paimon_catalog`. See the Fluss guide to
[configuring Paimon as lakehouse storage](https://fluss.apache.org/docs/streaming-lakehouse/datalake-formats/paimon/).

For a new table, create it in Fluss with `table.datalake.enabled` set to `true`. Fluss creates the
corresponding Paimon table and enables lake-stream mode on it. If the Paimon table already exists,
use a Fluss version that supports in-place enablement and follow its promotion procedure.

```sql
CREATE CATALOG `fluss_store` WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'fluss-server:9123'
);

CREATE DATABASE IF NOT EXISTS fluss_store.my_db;

CREATE TABLE fluss_store.my_db.my_table (id INT) WITH (
    'table.datalake.enabled' = 'true'
);
```

Write and query through the Paimon catalog.

```sql
USE CATALOG paimon_catalog;
USE my_db;

INSERT INTO my_table SELECT * FROM source_table;

SELECT * FROM `my_table$rt`;
```

Run the query in batch mode to get a bounded union read. Fluss uses its `full` startup mode for
the lake-enabled table.

```sql
SET 'execution.runtime-mode' = 'batch';
SELECT * FROM `my_table$rt`;
```

The [Fluss tiering service](https://fluss.apache.org/docs/streaming-lakehouse/tiering-service/) runs
separately from the cluster. New records become visible through the original Paimon table name once
tiering commits them. For real-time reads, see
[Fluss union reads](https://fluss.apache.org/docs/streaming-lakehouse/union-read/).
