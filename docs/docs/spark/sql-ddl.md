---
title: "Create Tables, Views, and Tags"
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

# Create Tables, Views, and Tags

Create tables, views, and tags after selecting a [Paimon catalog](./catalogs). Use
[Alter Tables](./sql-alter) to evolve an existing schema and [Format Table Partitions](./format-table)
for catalog-managed partition DDL.

| Command | Creates data? | Use it for |
| --- | --- | --- |
| `CREATE TABLE` | No | Define an empty table with explicit columns and properties. |
| `CREATE TABLE ... AS SELECT` | Yes | Create and populate a table from query results. |
| `CREATE TABLE ... LIKE` | No | Copy a source table definition; requires Spark 3.4+. |
| `REPLACE TABLE` | Only with `AS SELECT` | Replace an existing table definition; requires Spark 3.4+ for the behavior below. |
| `CREATE OR REPLACE TABLE` | Only with `AS SELECT` | Create a missing table or replace one that exists. |

The examples assume `USE paimon.default` with a configured [catalog](./catalogs). In
`SparkGenericCatalog`, include `USING paimon` when creating or replacing a Paimon table.

## Catalog

<span id="create-catalog"></span>
<span id="create-filesystem-catalog"></span>
<span id="creating-hive-catalog"></span>
<span id="creating-jdbc-catalog"></span>
<span id="creating-rest-catalog"></span>
<span id="bear-token"></span>
<span id="dlf-ak"></span>
<span id="dlf-sts-token"></span>

See [Catalogs](./catalogs) for filesystem, Hive, JDBC, REST, and `SparkGenericCatalog` setup.

## Table

### Create Table

Tables created without an external location are managed by the catalog. Dropping a managed
table also deletes its files. See [Create External Table](#create-external-table) for Hive
catalog ownership rules.

Create an unpartitioned primary key table:

```sql
CREATE TABLE events (
    user_id BIGINT,
    item_id BIGINT,
    behavior STRING,
    dt STRING,
    hh STRING
) TBLPROPERTIES (
    'primary-key' = 'dt,hh,user_id'
);
```

To partition the data by date and hour, declare `PARTITIONED BY`. For a primary key table,
the primary key must include every partition column:

```sql
CREATE TABLE partitioned_events (
    user_id BIGINT,
    item_id BIGINT,
    behavior STRING,
    dt STRING,
    hh STRING
) PARTITIONED BY (dt, hh)
TBLPROPERTIES ('primary-key' = 'dt,hh,user_id');
```

Omit `primary-key` to create an append table. For storage layout and table options, see
[Primary Key Tables](../primary-key-table/) and [Append Tables](../append-table/).

### Manage Format Table Partitions

See [Format Table Partitions](./format-table) for partition registration, custom locations,
statistics, and mixed-version writer requirements.

### Create External Table

In a Hive catalog, specifying `LOCATION` creates an external table. Dropping that table removes
its Hive metadata and keeps the data files. Without `LOCATION`, the table is managed and dropping
it also deletes its files.

```sql
CREATE TABLE external_events (
    user_id BIGINT,
    item_id BIGINT,
    behavior STRING,
    dt STRING,
    hh STRING
) PARTITIONED BY (dt, hh)
TBLPROPERTIES ('primary-key' = 'dt,hh,user_id')
LOCATION '/path/to/external_events';
```

To register an existing Paimon table at a location, let Paimon load its schema, partitioning,
and properties from that location:

```sql
CREATE TABLE registered_events LOCATION '/path/to/existing_paimon_table';
```

If you specify columns or partitioning, they must match the existing table. Supplied table
properties may be a subset of the existing properties. Omitting those declarations avoids
repeating metadata already stored with the table.

### Create Table As Select

`CREATE TABLE ... AS SELECT` (CTAS) derives the schema from a query and writes its result to
a new table. Declare the target partitioning, primary key, and storage properties on the new
table as needed.

Create a small source table for the following examples:

```sql
CREATE TABLE source_events (
    user_id BIGINT,
    item_id BIGINT,
    behavior STRING,
    dt STRING,
    hh STRING
);
INSERT INTO source_events VALUES (1, 10, 'pv', '2025-01-01', '00');
```

Each statement below creates a separate target:

```sql
-- Copy query results into an append table.
CREATE TABLE events_copy AS SELECT * FROM source_events;

-- Partition the target by date.
CREATE TABLE events_by_date PARTITIONED BY (dt)
AS SELECT * FROM source_events;

-- Choose a file format for the target.
CREATE TABLE events_parquet TBLPROPERTIES ('file.format' = 'parquet')
AS SELECT * FROM source_events;

-- Create a primary key table.
CREATE TABLE events_by_user TBLPROPERTIES ('primary-key' = 'user_id')
AS SELECT * FROM source_events;

-- Combine partitioning and a primary key.
CREATE TABLE events_by_date_user PARTITIONED BY (dt)
TBLPROPERTIES ('primary-key' = 'dt,user_id')
AS SELECT * FROM source_events;
```

### Replace Table

Paimon supports preserving snapshot history for Spark `REPLACE TABLE` from **Spark 3.4**.

```sql
CREATE TABLE my_table (
    user_id BIGINT,
    item_id BIGINT,
    behavior STRING
) TBLPROPERTIES (
    'primary-key' = 'user_id',
    'bucket' = '2'
);

INSERT INTO my_table VALUES (1, 10, 'pv');

REPLACE TABLE my_table (
    user_id BIGINT,
    item_id BIGINT,
    category STRING
) TBLPROPERTIES (
    'primary-key' = 'user_id',
    'bucket' = '4'
);
```

In Paimon, this is not an atomic replacement. Paimon changes Spark's drop+create replace path to
truncate the current table and commit a new schema, while preserving the table location and snapshot
history. The current table becomes empty and uses the new schema, but old snapshots can still be
queried by time travel.

```sql
SELECT * FROM my_table;

SELECT * FROM my_table VERSION AS OF 1;
```

`REPLACE TABLE` requires the table to exist. If the table does not exist, use
`CREATE OR REPLACE TABLE` instead.

Both `REPLACE TABLE ... AS SELECT` and `CREATE OR REPLACE TABLE ... AS SELECT` are supported.
The former requires an existing target; the latter also creates the target when it is missing.
For example, given a source table with columns `(user_id BIGINT, item_id BIGINT, behavior STRING)`:

```sql
CREATE OR REPLACE TABLE my_table
TBLPROPERTIES (
    'primary-key' = 'user_id',
    'bucket' = '4'
)
AS SELECT user_id, item_id, behavior FROM source_table;
```

Snapshot preservation depends on the replacement path and catalog support. Keep the provider,
table type, and partitioning unchanged to use the in-place path. A replacement that changes
these can fall back to dropping and recreating the table, losing its snapshot history. A catalog
that does not support in-place replacement can also fall back to drop-and-create.

When a replacement query reads its own target, Paimon pins the source read to the existing
snapshot. On a partitioned table, restate the partitioning:

```sql
CREATE TABLE replace_example (id INT, dt STRING) PARTITIONED BY (dt);
INSERT INTO replace_example VALUES (1, 'a'), (2, 'b');

REPLACE TABLE replace_example PARTITIONED BY (dt)
AS SELECT * FROM replace_example WHERE dt = 'a';

SELECT * FROM replace_example;
-- 1  a
```

A self-referencing replacement that would change the provider, table type, or partitioning
is rejected before the table is dropped. Write the query result to a separate table first if
that change is needed.

### Create Table Like

A new table can be created from an existing source table. Available from **Spark 3.4**.

```sql
CREATE TABLE target_table LIKE source_table;
```

`CREATE TABLE LIKE` copies the source schema and partitioning.

In `SparkCatalog`, if `USING xxx` is not specified, the target inherits the source provider.

In `SparkGenericCatalog`, use `USING paimon` to enable Paimon `CREATE TABLE LIKE` semantics.

When Paimon handles the command, comments and table properties are copied only when the source and target providers are the same. If the providers are different, only the comment is copied.

`path`, `provider`, `location`, `owner`, `external` and `is-managed-location` are never copied. Users can still override the target table with `TBLPROPERTIES`.

`STORED AS` is not supported in `SparkCatalog`. In `SparkGenericCatalog`, commands without `USING paimon` use Spark native behavior.

```sql
CREATE TABLE source_tbl (
    id INT,
    name STRING,
    pt STRING
) COMMENT 'source comment'
PARTITIONED BY (pt)
TBLPROPERTIES ('primary-key' = 'id,pt', 'bucket' = '5');

-- target inherits the source provider
CREATE TABLE target_tbl LIKE source_tbl;
```

## View

A persistent view stores a query definition in the Paimon catalog. With `SparkCatalog`,
persistent views are supported by Hive, REST, and JDBC metastores. Temporary views belong to
the Spark session and use an unqualified name.

### Create Or Replace View

These examples read the `events` table from [Create Table](#create-table):

```sql
-- Persistent view in the selected catalog and database.
CREATE VIEW event_view AS SELECT user_id, behavior FROM events;
CREATE OR REPLACE VIEW event_view
AS SELECT user_id, behavior FROM events WHERE behavior = 'pv';

-- Session-scoped view; do not qualify the temporary view name with a database.
CREATE TEMPORARY VIEW temporary_events AS SELECT * FROM events;
CREATE OR REPLACE TEMPORARY VIEW temporary_events
AS SELECT * FROM events WHERE behavior = 'pv';
```

See [Views](../concepts/views) for catalog-specific behavior and
[`alter_view_dialect`](./procedures/metadata#alter_view_dialect) for dialect management.

### Drop View

```sql
DROP VIEW event_view;
DROP VIEW temporary_events;
```

## Tag

Tags retain named snapshots for later reads. The examples below assume `T` has committed data
and snapshots `1` and `2` exist. See [Manage Tags](../maintenance/manage-tags) for retention and
[Time Travel](./sql-query#batch-time-travel) for reading a tag.

### Create Or Replace Tag

Specify a snapshot and retention period, or omit them to tag the latest snapshot without an
explicit retention period. `IF NOT EXISTS` leaves an existing tag unchanged; `REPLACE TAG`
updates an existing tag.

```sql
-- create a tag based on the latest snapshot and no retention.
ALTER TABLE T CREATE TAG `TAG-1`;

-- create a tag based on the latest snapshot and no retention if it doesn't exist.
ALTER TABLE T CREATE TAG IF NOT EXISTS `TAG-1`;

-- create a tag based on the latest snapshot and retain it for 7 day.
ALTER TABLE T CREATE TAG `TAG-2` RETAIN 7 DAYS;

-- create a tag based on snapshot-1 and no retention.
ALTER TABLE T CREATE TAG `TAG-3` AS OF VERSION 1;

-- create a tag based on snapshot-2 and retain it for 12 hour.
ALTER TABLE T CREATE TAG `TAG-4` AS OF VERSION 2 RETAIN 12 HOURS;

-- replace an existing tag with new snapshot id and new retention
ALTER TABLE T REPLACE TAG `TAG-4` AS OF VERSION 2 RETAIN 24 HOURS;

-- Create the tag if missing, or replace it if it exists.
ALTER TABLE T CREATE OR REPLACE TAG `TAG-5` AS OF VERSION 2 RETAIN 24 HOURS;
```
When `tag.automatic-creation` is enabled, only one automatic tag can be created per snapshot.

### Delete Tag
Delete a tag or multiple tags of a table.
```sql
-- delete a tag.
ALTER TABLE T DELETE TAG `TAG-1`;

-- delete a tag if it exists.
ALTER TABLE T DELETE TAG IF EXISTS `TAG-1`;

-- delete multiple tags, delimiter is ','.
ALTER TABLE T DELETE TAG `TAG-1,TAG-2`;
```

### Rename Tag
Rename an existing tag with a new tag name.
```sql
ALTER TABLE T RENAME TAG `TAG-1` TO `TAG-2`;
```

### Show Tags
List all tags of a table.
```sql
SHOW TAGS T;
```
