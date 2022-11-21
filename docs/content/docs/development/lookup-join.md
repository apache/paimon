---
title: "Lookup Join"
weight: 6
type: docs
aliases:
- /development/lookup-join.html
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

# Lookup Join

A [Lookup Join](https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/dev/table/sql/queries/joins/)
is used to enrich a table with data that is queried from Flink Table Store. The join requires one table to have
a processing time attribute and the other table to be backed by a lookup source connector.

First, create a table, and update it in real-time.

```sql
-- Create a table store catalog
CREATE CATALOG my_catalog WITH (
  'type'='table-store',
  'warehouse'='hdfs://nn:8020/warehouse/path' -- or 'file://tmp/foo/bar'
);

USE CATALOG my_catalog;

-- Create a table in table-store catalog
CREATE TABLE customers (
  id INT PRIMARY KEY NOT ENFORCED,
  name STRING,
  country STRING,
  zip STRING
);

-- Launch a streaming job to update customers table
INSERT INTO customers ...

-- Create a temporary left table, like from kafka
CREATE TEMPORARY TABLE Orders (
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

Then, you can use this table in lookup join.

```sql
-- enrich each order with customer information
SELECT o.order_id, o.total, c.country, c.zip
FROM Orders AS o
         JOIN customers FOR SYSTEM_TIME AS OF o.proc_time AS c
              ON o.customer_id = c.id;
```

The lookup join node will maintain the rocksdb cache locally and pull the latest updates
of the table in real time, and only pull the necessary data. Therefore, your filter conditions
are also very important.

In order to avoid excessive use of local disks, the lookup join feature is only suitable
for table sizes below tens of millions.

{{< hint info >}}
__Note:__ Partitioned or non-pk tables are not supported now.
{{< /hint >}}

Project pushdown can effectively reduce the overhead,
[FLINK-29138](https://issues.apache.org/jira/browse/FLINK-29138) fixed the bug that
the project cannot be pushed down to the source. So it is preferable to use a version
greater than or equal to `flink 1.14.6`, `flink 1.15.3` and `flink 1.16.0`. Or you can
cherry-pick the commit to your own Flink branch.

## RocksDBOptions

Options for rocksdb cache, you can configure options in `WITH` or dynamic table hints.

```sql
SELECT o.order_id, o.total, c.country, c.zip
  FROM Orders AS o JOIN customers /*+ OPTIONS('lookup.cache-rows'='20000') */
  FOR SYSTEM_TIME AS OF o.proc_time AS c ON o.customer_id = c.id;
```

{{< generated/rocksdb_configuration >}}
