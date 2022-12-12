---
title: "Query Table"
weight: 4
type: docs
aliases:
- /development/query-table.html
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

# Query Table

You can directly SELECT the table in batch runtime mode of Flink SQL.

```sql
-- Batch mode, read latest snapshot
SET 'execution.runtime-mode' = 'batch';
SELECT * FROM MyTable;
```

## Query Engines

Table Store not only supports Flink SQL queries natively but also provides
queries from other popular engines. See [Engines]({{< ref "docs/engines/overview" >}})

## Query Optimization

It is highly recommended to specify partition and primary key filters
along with the query, which will speed up the data skipping of the query.

The filter functions that can accelerate data skipping are:
- `=`
- `<`
- `<=`
- `>`
- `>=`
- `IN (...)`
- `LIKE 'abc%'`
- `IS NULL`

Table Store will sort the data by primary key, which speeds up the point queries
and range queries. When using a composite primary key, it is best for the query
filters to form a [leftmost prefix](https://dev.mysql.com/doc/refman/5.7/en/multiple-column-indexes.html)
of the primary key for good acceleration.

Suppose that a table has the following specification:

```sql
CREATE TABLE orders (
    catalog_id BIGINT,
    order_id BIGINT,
    .....,
    PRIMARY KEY (catalog_id, order_id) NOT ENFORCED -- composite primary key
)
```

The query obtains a good acceleration by specifying a range filter for
the leftmost prefix of the primary key.

```sql
SELECT * FROM orders WHERE catalog_id=1025;

SELECT * FROM orders WHERE catalog_id=1025 AND order_id=29495;

SELECT * FROM orders
  WHERE catalog_id=1025
  AND order_id>2035 AND order_id<6000;
```

However, the following filter cannot accelerate the query well.

```sql
SELECT * FROM orders WHERE order_id=29495;

SELECT * FROM orders WHERE catalog_id=1025 OR order_id=29495;
```

## Snapshots Table

You can query the snapshot history information of the table through Flink SQL.

```sql
SELECT * FROM MyTable$snapshots;

+--------------+------------+-----------------+-------------------+--------------+-------------------------+
|  snapshot_id |  schema_id |     commit_user | commit_identifier |  commit_kind |             commit_time |
+--------------+------------+-----------------+-------------------+--------------+-------------------------+
|            2 |          0 | 7ca4cd28-98e... |                 2 |       APPEND | 2022-10-26 11:44:15.600 |
|            1 |          0 | 870062aa-3e9... |                 1 |       APPEND | 2022-10-26 11:44:15.148 |
+--------------+------------+-----------------+-------------------+--------------+-------------------------+
2 rows in set
```

By querying one table's snapshots table, you can know the commit and expiration
information about that table and time travel through the data.

## Schemas Table

You can query the historical schemas of the table through Flink SQL.

```sql
SELECT * FROM MyTable$schemas;

+-----------+--------------------------------+----------------+--------------+---------+---------+
| schema_id |                         fields | partition_keys | primary_keys | options | comment |
+-----------+--------------------------------+----------------+--------------+---------+---------+
|         0 | [{"id":0,"name":"word","typ... |             [] |     ["word"] |      {} |         |
|         1 | [{"id":0,"name":"word","typ... |             [] |     ["word"] |      {} |         |
|         2 | [{"id":0,"name":"word","typ... |             [] |     ["word"] |      {} |         |
+-----------+--------------------------------+----------------+--------------+---------+---------+
3 rows in set
```

You can join the snapshots table and schemas table to get the fields of given snapshots.

```sql

SELECT s.snapshot_id, t.schema_id, t.fields 
    FROM MyTable$snapshots s JOIN MyTable$schemas t 
    ON s.schema_id=t.schema_id where s.snapshot_id=100;

```

## Options Table

You can query the table's option information which is specified from the DDL
through Flink SQL. The options not shown will be the default value. You can take
reference to  [Configuration]({{< ref "docs/development/configuration" >}}).

```sql
SELECT * FROM MyTable$options;

+------------------------+--------------------+
|         key            |        value       |
+------------------------+--------------------+
| snapshot.time-retained |         5 h        |
+------------------------+--------------------+
1 rows in set
```

## Audit log Table

If you need to audit the changelog of the table, you can use the `audit_log` to
stream read table. Through `audit_log` table, you can get the `rowkind` column
when you get the incremental data of the table. You can use this column for
filtering and other operations to complete the audit.

There are four values for `rowkind`:
- `+I` Insertion operation.
- `-U` Update operation with the previous content of the updated row.
- `+U` Update operation with new content of the updated row.
- `-D` Deletion operation.

```sql
SELECT * FROM MyTable$audit_log;

+------------------+-----------------+-----------------+
|     rowkind      |     column_0    |     column_1    |
+------------------+-----------------+-----------------+
|        +I        |      ...        |      ...        |
+------------------+-----------------+-----------------+
|        -U        |      ...        |      ...        |
+------------------+-----------------+-----------------+
|        +U        |      ...        |      ...        |
+------------------+-----------------+-----------------+
3 rows in set
```
