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

The Flink connector routes the reads and writes of a lake-stream table through the streaming store
you name on the catalog. [Lake-Stream Mode](../concepts/lake-stream) explains what the mode does to
a table.

## Enable Lake-Stream Mode

A lake-stream table needs three things: a table of the same database and name in the store, the
`lakestream.enabled` option on the Paimon table, and a Paimon catalog that names the store.

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

Paimon does not create the store-side table and does not move a table's existing data into it.
Create the store table yourself, in a catalog of the store;
[Use Fluss as the Store](#use-fluss-as-the-store) shows how for Fluss.

Stop the pipelines that write to the table before you change either side, and restart them without
state once both sides exist, so that Flink plans them against the store sink. A write that lands
while the store side is missing, or a writer that resumes from state saved against the Paimon sink,
can leave the store and Paimon holding different data for the same keys.

A catalog created without `lakestream.store` cannot serve a lake-stream table. `my_table$rt` fails
as an unknown table, and a write to `my_table` is rejected instead of quietly going to Paimon, which
would leave the store and Paimon holding different data for the same keys. Tables that are not
lake-stream tables are unaffected. Add `lakestream.store` and the store's connection options to the
catalog, or [disable the mode](../concepts/lake-stream#disable-lake-stream-mode) on the table first.

## Write to a Lake-Stream Table

Your write statements do not change. Flink reads `lakestream.enabled` from the table when it plans
the statement and routes the write accordingly.

```sql
INSERT INTO my_table SELECT * FROM source_table;
```

A Flink CDC pipeline does not plan its writes through the SQL sink, so it writes straight to Paimon
and bypasses the store. Confirm that the data it writes has already expired from the store: while
the store still holds records for the same primary keys, the two can disagree about them, and later
updates from the store may then use stale state. See
[Write to a Lake-Stream Table](../concepts/lake-stream#write-to-a-lake-stream-table).

## Read the Real-Time Name

`my_table$rt` is the same table read through the store, not a second table. Write the name in
backticks:

```sql
-- Reads only data that Paimon has committed.
SELECT * FROM my_table;

-- Combines committed Paimon data with the store's un-tiered records.
SELECT * FROM `my_table$rt`;
```

Paimon connects to the store on the first lake-stream read or write. An unknown store identifier or
a connection option the store requires fails at that point with a configuration error.

## Use Fluss as the Store

Fluss is a streaming storage system for the lakehouse, and can serve as a Paimon Lakestream Store.
Lake-stream mode requires Fluss 1.0 or later.

Set up a Fluss cluster with Paimon as its lakehouse storage first; the Fluss documentation covers
[deploying the streaming lakehouse](https://fluss.apache.org/docs/install-deploy/deploying-streaming-lakehouse/)
and [Paimon as a datalake format](https://fluss.apache.org/docs/streaming-lakehouse/datalake-formats/paimon/).

Then create the store-side table and the Paimon table that points at it. The two have to agree on
the database name and the table name, because that name is how Paimon finds the table in the store.

```sql
CREATE CATALOG `fluss_store` WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'fluss-server:9123'
);

CREATE TABLE fluss_store.my_db.my_table (id INT);

CREATE TABLE paimon_catalog.my_db.my_table (id INT) WITH (
    'lakestream.enabled' = 'true'
);
```

Writes and reads are then the ordinary ones, in the Paimon catalog:

```sql
INSERT INTO my_table SELECT * FROM source_table;

SELECT * FROM `my_table$rt`;
```

:::note
A Fluss log table is unbounded, so Flink rejects a batch query on `my_table$rt` as a query on an
unbounded table. Bound the read with scan hints. The example below reads the log from the beginning
up to the offset it had when the query started.
:::

```sql
SELECT * FROM `my_table$rt`
/*+ OPTIONS('scan.startup.mode' = 'earliest', 'scan.bounded.mode' = 'latest-offset') */;
```

Records reach Paimon only through the Fluss tiering service, which is a separate component from the
Fluss cluster that serves `my_table$rt`. Until the tiering service runs, the records stay in Fluss:
`my_table$rt` returns them and `my_table` returns nothing. The Fluss documentation covers the
[tiering service](https://fluss.apache.org/docs/streaming-lakehouse/tiering-service/) and
[union reads](https://fluss.apache.org/docs/streaming-lakehouse/union-read/).
