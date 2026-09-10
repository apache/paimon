---
title: "Views"
sidebar_position: 10
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

# Views

A view is a named SQL query stored in the catalog. It lets users reuse query logic without
materializing another copy of the data. Paimon view metadata can hold multiple SQL dialect
representations so that engines can use a definition written for their dialect.

Start by checking catalog support, then use the operations below to create a view or manage its
SQL representations. Storing multiple dialects does not translate SQL automatically.

## Catalog support

| Catalog | View storage | Dialect changes |
| --- | --- | --- |
| REST | Managed by the REST service. | Supported through the view API, subject to server support. |
| JDBC | Stored in the `paimon_views` metadata table. | Supported. |
| Hive | Stored as a Hive metastore `VIRTUAL_VIEW`. | Only the default query is persisted; altering dialects is not supported. |
| Filesystem | View operations are not implemented. | Not supported. |

See [JDBC catalog notes](#jdbc-catalog-notes) for initialization and concurrency behavior.

### Representation structure

The Paimon view schema contains the following fields. How this metadata is persisted depends on
the catalog implementation.

| Field | Type | Description |
| --- | --- | --- |
| `fields` | List of data fields | The output columns of the view. |
| `query` | String | The default SQL query. |
| `dialects` | Map of strings to strings | Queries keyed by dialect identifier, such as `spark` or `flink`. |
| `comment` | Optional string | A description of the view. |
| `options` | Map of strings to strings | View properties. |

When an engine requests a dialect that is absent from `dialects`, Paimon returns the default
`query`. It does not translate that query into another SQL dialect. Dropping a dialect entry
therefore restores the default query for that dialect; it does not remove the view.

## Operations

### Create or replace view

Use `CREATE VIEW` in a catalog that supports views. The compute engine parses the SQL and resolves
the view's output schema; the catalog persists the resulting definition.

For an existing `my_db.sales` table with `region` and `amount` columns:

```sql
CREATE VIEW my_db.sales_view AS
SELECT region, SUM(amount) AS total_amount
FROM my_db.sales
GROUP BY region;
```

Replacement behavior depends on the engine. Paimon's Spark integration implements
`CREATE OR REPLACE VIEW` by dropping the existing view and creating a new one. This is not an
atomic replacement, and previously added dialect entries are not retained. To change one stored
dialect, use the procedure below.

### Alter view dialect via procedure

Use `sys.alter_view_dialect` with a REST or JDBC catalog to add, update, or drop a dialect query.
Use `add` when the dialect is absent and `update` when it already exists. SQL created in Flink or
Spark includes that engine's dialect, so the examples update it first.

#### Flink example

Run these statements in the Paimon catalog containing the view:

```sql
-- Update the Flink query while keeping the output columns unchanged.
CALL sys.alter_view_dialect(
    'my_db.sales_view', 'update', 'flink',
    'SELECT region, SUM(amount) AS total_amount FROM my_db.sales WHERE amount > 0 GROUP BY region'
);

-- Fall back to the default query.
CALL sys.alter_view_dialect('my_db.sales_view', 'drop', 'flink');

-- Add a Flink query again.
CALL sys.alter_view_dialect(
    'my_db.sales_view', 'add', 'flink',
    'SELECT region, SUM(amount) AS total_amount FROM my_db.sales GROUP BY region'
);
```

#### Spark example

For a view created in Spark, use the `spark` dialect:

```sql
CALL sys.alter_view_dialect(
    'my_db.sales_view', 'update', 'spark',
    'SELECT region, SUM(amount) AS total_amount FROM my_db.sales WHERE amount > 0 GROUP BY region'
);

CALL sys.alter_view_dialect('my_db.sales_view', 'drop', 'spark');

CALL sys.alter_view_dialect(
    'my_db.sales_view', 'add', 'spark',
    'SELECT region, SUM(amount) AS total_amount FROM my_db.sales GROUP BY region'
);
```

### Drop view

```sql
DROP VIEW my_db.sales_view;
```

### JDBC catalog notes

The JDBC catalog stores views in a dedicated `paimon_views` table that is created on first
initialization. A few things are worth knowing when running on top of an existing JDBC catalog:

- **Required permissions on upgrade.** Upgrading to a Paimon release with view support requires
  `CREATE TABLE` permission on the catalog database the first time the catalog is opened, so that
  the `paimon_views` table can be created. Operators who tightened privileges to CRUD-only after
  the initial deployment should either restore `CREATE TABLE` permission temporarily or create the
  `paimon_views` table manually beforehand.
- **Table and view share the same identifier namespace.** A name cannot be used by both a table
  and a view in the same database. `createTable`, `renameTable`, `createView` and `renameView` all
  validate this invariant under the catalog lock; concurrent operations targeting the same
  identifier will see exactly one winner.
- **Single-process atomicity does not depend on `lock.enabled`.** The JDBC catalog also keeps a
  per-JVM stripe lock keyed by `(catalog key, database, object name)`, so the table-vs-view name
  uniqueness invariant holds within one JVM even when `lock.enabled = false`. Setting
  `lock.enabled = true` (with `lock.type = jdbc`) is still recommended for multi-process
  deployments because the stripe lock only serializes operations within the same JVM.
- **Database visibility.** A database that contains only views (and no tables or properties) is
  reported by `listDatabases` and `SHOW DATABASES`. `DROP DATABASE ... CASCADE` removes both the
  tables and the views in that database; `DROP DATABASE` without `CASCADE` will reject databases
  that still hold any view.
- **Cross-database rename.** `renameView(from, to)` and `renameTable(from, to)` raise an
  `IllegalArgumentException` (`Database X does not exist.`) when the target database is missing,
  matching the BadRequest semantics of the REST catalog.

## See also

- [Spark SQL DDL – Views](../spark/sql-ddl#view)
- [REST Catalog Overview](./rest/)
- [REST Catalog View API](./rest/rest-api)
