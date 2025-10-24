---
title: "Views"
weight: 10
type: docs
aliases:
- /concepts/views.html
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

Paimon views are logical table definitions managed in the catalog. A view stores only the SQL query that
produces its result set, which allows one engine to define a view and other engines to consume it
without duplicating data. View metadata can be coordinated across engines.

## Catalog support

View metadata is persisted only when the catalog implementation supports it:

- **Hive metastore catalog** – view metadata is stored together with table metadata inside the
  metastore warehouse.
- **REST catalog** – view metadata is kept in the REST backend and exposed through the catalog API.

File-system catalogs do not currently support views because they lack persistent metadata storage.


### Representation structure

| Field     | Type | Description |
|-----------|------|-------------|
| `query`   | `string` | Canonical SQL `SELECT` statement that defines the view. |
| `dialect` | `string` | SQL dialect identifier (for example, `spark` or `flink`). |

Multiple representations can be stored for the same version so that different engines can access the
view using their native dialect.

## Operations

### Create or replace view

Use `CREATE VIEW` or `CREATE OR REPLACE VIEW` to register a view. Paimon assigns a UUID, writes the
first metadata file, and records version `1`.

```sql
CREATE VIEW sales_view AS
SELECT region, SUM(amount) AS total_amount
FROM sales
GROUP BY region;
```

### Inspect view definitions

- `SHOW VIEWS` lists all registered views in the current catalog.
- `SHOW CREATE VIEW view_name` returns the canonical SQL representation of a view.

### Alter view dialects via procedures

Paimon provides the `sys.alter_view_dialect` procedure so that engines can manage multiple SQL
representations for the same view version.

#### Flink example

```sql
-- Add a Flink dialect
CALL [catalog.]sys.alter_view_dialect('view_identifier', 'add', 'flink', 'SELECT ...');

-- Update the stored Flink dialect
CALL [catalog.]sys.alter_view_dialect('view_identifier', 'update', 'flink', 'SELECT ...');

-- Drop the Flink dialect representation
CALL [catalog.]sys.alter_view_dialect('view_identifier', 'drop', 'flink');
```

#### Spark example

```sql
-- Add a Spark dialect
CALL sys.alter_view_dialect('view_identifier', 'add', 'spark', 'SELECT ...');

-- Update the Spark dialect
CALL sys.alter_view_dialect('view_identifier', 'update', 'spark', 'SELECT ...');

-- Drop the Spark dialect
CALL sys.alter_view_dialect('view_identifier', 'drop', 'spark');
```

### Drop view

`DROP VIEW view_name;`

## See also

- [Spark SQL DDL – Views]({{< ref "spark/sql-ddl#view" >}})
- [REST Catalog Overview]({{< ref "concepts/rest/overview" >}})
