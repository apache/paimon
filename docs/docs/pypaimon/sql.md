---
title: "SQL Query"
sidebar_position: 8
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

# SQL Query

PyPaimon supports executing SQL queries on Paimon tables, powered by [pypaimon-rust](https://github.com/apache/paimon-rust/tree/main/bindings/python) and [DataFusion](https://datafusion.apache.org/).

## Installation

SQL queries require Python 3.10 or newer and the SQL extra:

```shell
pip install 'pypaimon[sql]'
```

This installs the Rust bindings and DataFusion dependencies used by SQL queries.

## Usage

Create a `SQLContext`, register one or more catalogs with their options, and run SQL queries.

### Basic Query

```python
from pypaimon_rust.datafusion import SQLContext
import pyarrow as pa

ctx = SQLContext()
ctx.register_catalog("paimon", {"warehouse": "/path/to/warehouse"})
ctx.set_current_catalog("paimon")
ctx.set_current_database("default")

# Execute SQL and get PyArrow RecordBatches
batches = ctx.sql("SELECT * FROM my_table")
table = pa.Table.from_batches(batches)
print(table)

# Convert to Pandas DataFrame
df = table.to_pandas()
print(df)
```

`SQLContext` can also be imported from `pypaimon`:

```python
from pypaimon import SQLContext
```

### Table Reference Format

The default catalog and default database can be configured via `set_current_catalog()` and `set_current_database()`, so you can reference tables in multiple ways:

```python
# Direct table name (uses default database)
ctx.sql("SELECT * FROM my_table")

# Two-part: database.table
ctx.sql("SELECT * FROM mydb.my_table")

# Three-part: catalog.database.table
ctx.sql("SELECT * FROM paimon.mydb.my_table")
```

### Multi-Catalog Query

`SQLContext` supports registering multiple catalogs for cross-catalog queries:

```python
ctx = SQLContext()
ctx.register_catalog("a", {"warehouse": "/path/to/warehouse_a"})
ctx.register_catalog("b", {
    "metastore": "rest",
    "uri": "http://localhost:8080",
    "warehouse": "warehouse_b",
})
ctx.set_current_catalog("a")
ctx.set_current_database("default")

# Cross-catalog join
batches = ctx.sql("""
    SELECT a_users.name, b_orders.amount
    FROM a.default.users AS a_users
    JOIN b.default.orders AS b_orders ON a_users.id = b_orders.user_id
""")
```

### Register Arrow Batches

You can register PyArrow RecordBatches as temporary tables:

```python
batch = pa.record_batch([[1, 2], ["alice", "bob"]], names=["id", "name"])
ctx.register_batch("paimon.default.my_temp", batch)
batches = ctx.sql("SELECT * FROM paimon.default.my_temp")
```

## Supported SQL Syntax

The SQL engine is powered by Apache DataFusion, which supports a rich set of SQL syntax. For the full SQL reference, see the [paimon-rust SQL documentation](https://paimon-rust.apache.org/sql.html) which covers:

- **DDL**: `CREATE SCHEMA`, `CREATE TABLE` (with `PARTITIONED BY`, `PRIMARY KEY`, `WITH` options), `DROP TABLE`, `ALTER TABLE`, `CREATE TEMPORARY TABLE/VIEW`
- **DML**: `INSERT INTO`, `INSERT OVERWRITE` (dynamic/static partitions), `UPDATE`, `DELETE`, `MERGE INTO`, `TRUNCATE TABLE`
- **Procedures**: `CALL sys.create_tag`, `CALL sys.rollback_to`, etc.
- **Queries**: `SELECT`, column projection, filter pushdown, `COUNT(*)` pushdown
- **Time Travel**: `VERSION AS OF`, `TIMESTAMP AS OF`
- **Vector Search**: `vector_search()` table function
- **Full-Text Search**: `full_text_search()` table function
- **Dynamic Options**: `SET` / `RESET`
- **System Tables**: `$options`, `$schemas`, `$snapshots`, `$tags`, `$manifests`

For the DataFusion query syntax (JOINs, aggregations, subqueries, CTEs, window functions, etc.), see the [DataFusion SQL documentation](https://datafusion.apache.org/user-guide/sql/index.html).

## SQL Command

Execute SQL queries on Paimon tables directly from the command line. This feature is powered by pypaimon-rust and DataFusion.

Install the [SQL extra](#installation) and configure `paimon.yaml` as shown in
[CLI basic usage](./cli#basic-usage). Use database-qualified table names unless
you have explicitly selected a default database.

### One-Shot Query

Execute a single SQL query and display the result:

```shell
paimon sql "SELECT * FROM users LIMIT 10"
```

Output:
```
 id    name  age      city
  1   Alice   25   Beijing
  2     Bob   30  Shanghai
  3 Charlie   35 Guangzhou
```

**Options:**

- `--format, -f`: Output format: `table` (default) or `json`

**Examples:**

```shell
# Direct table name (uses default catalog and database)
paimon sql "SELECT * FROM users"

# Two-part: database.table
paimon sql "SELECT * FROM mydb.users"

# Query with filter and aggregation
paimon sql "SELECT city, COUNT(*) AS cnt FROM users GROUP BY city ORDER BY cnt DESC"

# Output as JSON
paimon sql "SELECT * FROM users LIMIT 5" --format json
```

### Interactive REPL

Start an interactive SQL session by running `paimon sql` without a query argument. The REPL supports arrow keys for line editing, and command history is persisted across sessions in `~/.paimon_history`.

```shell
paimon sql
```

Output:
```
    ____        _
   / __ \____ _(_)___ ___  ____  ____
  / /_/ / __ `/ / __ `__ \/ __ \/ __ \
 / ____/ /_/ / / / / / / / /_/ / / / /
/_/    \__,_/_/_/ /_/ /_/\____/_/ /_/

  Powered by pypaimon-rust + DataFusion
  Type 'help' for usage, 'exit' to quit.

paimon> SHOW DATABASES;
default
mydb

paimon> USE mydb;
Using database 'mydb'.

paimon> SHOW TABLES;
orders
users

paimon> SELECT count(*) AS cnt
     > FROM users
     > WHERE age > 18;
 cnt
  42
(1 row in 0.05s)

paimon> exit
Bye!
```

SQL statements end with `;` and can span multiple lines. The continuation prompt `     >` indicates that more input is expected.

**REPL Commands:**

| Command | Description |
|---|---|
| `USE <database>;` | Switch the default database |
| `SHOW DATABASES;` | List all databases |
| `SHOW TABLES;` | List tables in the current database |
| `SELECT ...;` | Execute a SQL query |
| `help` | Show usage information |
| `exit` / `quit` | Exit the REPL |

See [Supported SQL Syntax](#supported-sql-syntax) for the SQL feature reference.
