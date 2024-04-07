---
title: "SQL Lookup"
weight: 5
type: docs
aliases:
- /flink/sql-lookup.html
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

# Lookup Joins

[Lookup Joins](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/joins/) are a type of join in streaming queries. It is used to enrich a table with data that is queried from Paimon. The join requires one table to have a processing time attribute and the other table to be backed by a lookup source connector.

Paimon supports lookup joins on tables with primary keys and append tables in Flink. The following example illustrates this feature.

## Prepare

First, let's create a Paimon table and update it in real-time.

```sql
-- Create a paimon catalog
CREATE CATALOG my_catalog WITH (
  'type'='paimon',
  'warehouse'='hdfs://nn:8020/warehouse/path' -- or 'file://tmp/foo/bar'
);

USE CATALOG my_catalog;

-- Create a table in paimon catalog
CREATE TABLE customers (
    id INT PRIMARY KEY NOT ENFORCED,
    name STRING,
    country STRING,
    zip STRING
);

-- Launch a streaming job to update customers table
INSERT INTO customers ...

-- Create a temporary left table, like from kafka
CREATE TEMPORARY TABLE orders (
    order_id INT,
    total INT,
    customer_id INT,
    proc_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = '...',
    'properties.bootstrap.servers' = '...',
    'format' = 'csv'
    ...
);
```

## Normal Lookup

You can now use `customers` in a lookup join query.

```sql
-- enrich each order with customer information
SELECT o.order_id, o.total, c.country, c.zip
FROM orders AS o
JOIN customers
FOR SYSTEM_TIME AS OF o.proc_time AS c
ON o.customer_id = c.id;
```

## Retry Lookup

If the records of `orders` (main table) join missing because the corresponding data of `customers` (lookup table) is not ready.
You can consider using Flink's [Delayed Retry Strategy For Lookup](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/hints/#3-enable-delayed-retry-strategy-for-lookup).
Only for Flink 1.16+.

```sql
-- enrich each order with customer information
SELECT /*+ LOOKUP('table'='c', 'retry-predicate'='lookup_miss', 'retry-strategy'='fixed_delay', 'fixed-delay'='1s', 'max-attempts'='600') */
o.order_id, o.total, c.country, c.zip
FROM orders AS o
JOIN customers
FOR SYSTEM_TIME AS OF o.proc_time AS c
ON o.customer_id = c.id;
```

## Async Retry Lookup

The problem with synchronous retry is that one record will block subsequent records, causing the entire job to be blocked.
You can consider using async + allow_unordered to avoid blocking, the records that join missing will no longer block
other records.

```sql
-- enrich each order with customer information
SELECT /*+ LOOKUP('table'='c', 'retry-predicate'='lookup_miss', 'output-mode'='allow_unordered', 'retry-strategy'='fixed_delay', 'fixed-delay'='1s', 'max-attempts'='600') */
o.order_id, o.total, c.country, c.zip
FROM orders AS o
JOIN customers /*+ OPTIONS('lookup.async'='true', 'lookup.async-thread-number'='16') */
FOR SYSTEM_TIME AS OF o.proc_time AS c
ON o.customer_id = c.id;
```

{{< hint info >}}
If the main table (`orders`) is CDC stream, `allow_unordered` will be ignored by Flink SQL (only supports append stream),
your streaming job may be blocked. You can try to use `audit_log` system table feature of Paimon to walk around
(convert CDC stream to append stream).
{{< /hint >}}

## Dynamic Partition

In traditional data warehouses, each partition often maintains the latest full data, so this partition table only 
needs to join the latest partition. Paimon has specifically developed the `max_pt` feature for this scenario.

**Create Paimon Partitioned Table**

```sql
CREATE TABLE customers (
  id INT,
  name STRING,
  country STRING,
  zip STRING,
  dt STRING,
  PRIMARY KEY (id, dt) NOT ENFORCED
) PARTITIONED BY (dt);
```

**Lookup Join**

```sql
SELECT o.order_id, o.total, c.country, c.zip
FROM orders AS o
JOIN customers /*+ OPTIONS('lookup.dynamic-partition'='max_pt()', 'lookup.dynamic-partition.refresh-interval'='1 h') */
FOR SYSTEM_TIME AS OF o.proc_time AS c
ON o.customer_id = c.id;
```

The Lookup node will automatically refresh the latest partition and query the data of the latest partition.

## Query Service

You can run a Flink Streaming Job to start query service for the table. When QueryService exists, Flink Lookup Join
will prioritize obtaining data from it, which will effectively improve query performance.

{{< tabs "query-service" >}}

{{< tab "Flink SQL" >}}

```sql
CALL sys.query_service('database_name.table_name', parallelism);
```

{{< /tab >}}

{{< tab "Flink Action" >}}

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    query_service \
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name> \
    [--parallelism <parallelism>] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]]
```

{{< /tab >}}

{{< /tabs >}}
