---
title: "SQL Query"
weight: 8
type: docs
aliases:
  - /pypaimon/sql.html
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

PyPaimon supports executing SQL queries on Paimon tables, powered by [pypaimon-rust](https://github.com/apache/paimon-rust/tree/main/bindings/python) and [DataFusion](https://datafusion.apache.org/python/).

## Installation

SQL query support requires additional dependencies. Install them with:

```shell
pip install pypaimon[sql]
```

This will install `pypaimon-rust` and `datafusion`.

## Usage

Create a `SQLContext`, register one or more catalogs with their options, and run SQL queries.

### Basic Query

```python
from pypaimon.sql import SQLContext

ctx = SQLContext()
ctx.register_catalog("paimon", {"warehouse": "/path/to/warehouse"})
ctx.set_current_catalog("paimon")
ctx.set_current_database("default")

# Execute SQL and get PyArrow Table
table = ctx.sql("SELECT * FROM my_table")
print(table)

# Convert to Pandas DataFrame
df = table.to_pandas()
print(df)
```

### Table Reference Format

The default catalog and default database can be configured via `set_current_catalog()` and `set_current_database()`, so you can reference tables in two ways:

```python
# Direct table name (uses default database)
ctx.sql("SELECT * FROM my_table")

# Two-part: database.table
ctx.sql("SELECT * FROM mydb.my_table")
```

### Filtering

```python
table = ctx.sql("""
    SELECT id, name, age 
    FROM users 
    WHERE age > 18 AND city = 'Beijing'
""")
```

### Aggregation

```python
table = ctx.sql("""
    SELECT city, COUNT(*) AS cnt, AVG(age) AS avg_age
    FROM users
    GROUP BY city
    ORDER BY cnt DESC
""")
```

### Join

```python
table = ctx.sql("""
    SELECT u.name, o.order_id, o.amount
    FROM users u
    JOIN orders o ON u.id = o.user_id
    WHERE o.amount > 100
""")
```

### Subquery

```python
table = ctx.sql("""
    SELECT * FROM users
    WHERE id IN (
        SELECT user_id FROM orders
        WHERE amount > 1000
    )
""")
```

### Cross-Database Query

```python
# Query a table in another database using two-part syntax
table = ctx.sql("""
    SELECT u.name, o.amount
    FROM default.users u
    JOIN analytics.orders o ON u.id = o.user_id
""")
```

### Multi-Catalog Query

`SQLContext` supports registering multiple catalogs for cross-catalog queries:

```python
from pypaimon.sql import SQLContext

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
table = ctx.sql("""
    SELECT a_users.name, b_orders.amount
    FROM a.default.users AS a_users
    JOIN b.default.orders AS b_orders ON a_users.id = b_orders.user_id
""")
```

## Supported SQL Syntax

The SQL engine is powered by Apache DataFusion, which supports a rich set of SQL syntax including:

- `SELECT`, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`
- `JOIN` (INNER, LEFT, RIGHT, FULL, CROSS)
- Subqueries and CTEs (`WITH`)
- Aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, etc.)
- Window functions (`ROW_NUMBER`, `RANK`, `LAG`, `LEAD`, etc.)
- `UNION`, `INTERSECT`, `EXCEPT`

For the full SQL reference, see the [DataFusion SQL documentation](https://datafusion.apache.org/user-guide/sql/index.html).
